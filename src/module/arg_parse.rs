use std::cmp::Ordering;
use std::fmt::Display;
use std::time::Duration;
use chrono::DateTime;
use metricsql_runtime::types::{TimestampTrait};
use metricsql_parser::prelude::Matchers;
use metricsql_parser::parser::{parse_duration_value, parse_number};
use metricsql_runtime::parse_metric_selector;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyString};
use crate::common::current_time_millis;
use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::storage::{MAX_CHUNK_SIZE, MAX_TIMESTAMP, MIN_CHUNK_SIZE};
use crate::storage::time_series::TimeSeries;

#[derive(Clone, Default, Debug, PartialEq, Copy)]
pub enum TimestampRangeValue {
    Earliest,
    Latest,
    #[default]
    Now,
    Value(Timestamp),
}

impl TimestampRangeValue {
    pub fn to_timestamp(&self) -> Timestamp {
        match self {
            TimestampRangeValue::Earliest => 0,
            TimestampRangeValue::Latest => MAX_TIMESTAMP,
            TimestampRangeValue::Now => Timestamp::now(),
            TimestampRangeValue::Value(ts) => *ts,
        }
    }

    pub fn to_series_timestamp(&self, series: &TimeSeries) -> Timestamp {
        match self {
            TimestampRangeValue::Earliest => series.first_timestamp,
            TimestampRangeValue::Latest => series.last_timestamp,
            TimestampRangeValue::Now => Timestamp::now(),
            TimestampRangeValue::Value(ts) => *ts,
        }
    }
}

impl TryFrom<&str> for TimestampRangeValue {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "-" => Ok(TimestampRangeValue::Earliest),
            "+" => Ok(TimestampRangeValue::Latest),
            "*" => Ok(TimestampRangeValue::Now),
            _ => {
                let ts =
                    parse_timestamp(value).map_err(|_| ValkeyError::Str("invalid timestamp"))?;
                if ts < 0 {
                    return Err(ValkeyError::Str(
                        "TSDB: invalid timestamp, must be a non-negative integer",
                    ));
                }
                Ok(TimestampRangeValue::Value(ts))
            }
        }
    }
}

impl TryFrom<&ValkeyString> for TimestampRangeValue {
    type Error = ValkeyError;

    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        if value.len() == 1 {
            let bytes = value.as_slice();
            match bytes[0] {
                b'-' => return Ok(TimestampRangeValue::Earliest),
                b'+' => return Ok(TimestampRangeValue::Latest),
                b'*' => return Ok(TimestampRangeValue::Now),
                _ => {}
            }
        }
        if let Ok(int_val) = value.parse_integer() {
            if int_val < 0 {
                return Err(ValkeyError::Str(
                    "TSDB: invalid timestamp, must be a non-negative integer",
                ));
            }
            Ok(TimestampRangeValue::Value(int_val))
        } else {
            let date_str = value.to_string_lossy();
            let ts =
                parse_timestamp(&date_str).map_err(|_| ValkeyError::Str("invalid timestamp"))?;
            Ok(TimestampRangeValue::Value(ts))
        }
    }
}

impl From<Timestamp> for TimestampRangeValue {
    fn from(ts: Timestamp) -> Self {
        TimestampRangeValue::Value(ts)
    }
}

impl Display for TimestampRangeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimestampRangeValue::Earliest => write!(f, "-"),
            TimestampRangeValue::Latest => write!(f, "+"),
            TimestampRangeValue::Value(ts) => write!(f, "{}", ts),
            TimestampRangeValue::Now => write!(f, "*"),
        }
    }
}

impl PartialOrd for TimestampRangeValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use TimestampRangeValue::*;

        match (self, other) {
            (Now, Now) => Some(Ordering::Equal),
            (Earliest, Earliest) => Some(Ordering::Equal),
            (Latest, Latest) => Some(Ordering::Equal),
            (Value(a), Value(b)) => a.partial_cmp(b),
            (Now, Value(v)) => {
                let now = Timestamp::now();
                now.partial_cmp(v)
            }
            (Value(v), Now) => {
                let now = Timestamp::now();
                v.partial_cmp(&now)
            }
            (Earliest, _) => Some(Ordering::Less),
            (_, Earliest) => Some(Ordering::Greater),
            (Latest, _) => Some(Ordering::Greater),
            (_, Latest) => Some(Ordering::Less),
        }
    }
}

// todo: better naming
pub struct TimestampRange {
    start: TimestampRangeValue,
    end: TimestampRangeValue,
}

impl TimestampRange {
    pub fn new(start: TimestampRangeValue, end: TimestampRangeValue) -> ValkeyResult<Self> {
        if start > end {
            return Err(ValkeyError::Str("invalid timestamp range: start > end"));
        }
        Ok(TimestampRange { start, end })
    }

    pub fn start(&self) -> &TimestampRangeValue {
        &self.start
    }

    pub fn end(&self) -> &TimestampRangeValue {
        &self.end
    }
}

pub struct MetadataFunctionArgs {
    pub label_name: Option<String>,
    pub start: Timestamp,
    pub end: Timestamp,
    pub matchers: Vec<Matchers>,
    pub limit: Option<usize>,
}

pub(crate) fn normalize_range_timestamps(
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> (TimestampRangeValue, TimestampRangeValue) {
    match (start, end) {
        (Some(start), Some(end)) if start > end => (end.into(), start.into()),
        (Some(start), Some(end)) => (start.into(), end.into()),
        (Some(start), None) => (
            TimestampRangeValue::Value(start),
            TimestampRangeValue::Latest,
        ),
        (None, Some(end)) => (TimestampRangeValue::Earliest, end.into()),
        (None, None) => (TimestampRangeValue::Earliest, TimestampRangeValue::Latest),
    }
}


pub fn parse_number_arg(arg: &ValkeyString, name: &str) -> ValkeyResult<f64> {
    if let Ok(value) = arg.parse_float() {
        return Ok(value);
    }
    let arg_str = arg.to_string_lossy();
    parse_number_with_unit(&arg_str)
        .map_err(|_| {
            if name.is_empty() {
              ValkeyError::Str("TSDB: invalid number")
            } else {
                ValkeyError::String(format!("TSDB: cannot parse {name} as number"))
            }
        })
}

pub fn parse_integer_arg(arg: &ValkeyString, name: &str, allow_negative: bool) -> ValkeyResult<i64> {
    let value = if let Ok(val) = arg.parse_integer() {
        val
    } else {
        let num = parse_number_arg(arg, name)?;
        if num != num.floor() {
            return Err(ValkeyError::Str("TSDB: value must be an integer"));
        }
        if num > i64::MAX as f64 {
            return Err(ValkeyError::Str("TSDB: value is too large"));
        }
        num as i64
    };
    if !allow_negative && value < 0 {
        let msg = format!("TSDB: {} must be a non-negative integer", name);
        return Err(ValkeyError::String(msg));
    }
    Ok(value)
}

pub fn parse_timestamp_arg(arg: &ValkeyString) -> ValkeyResult<Timestamp> {
    if arg.len() == 1 {
        let arg = arg.as_slice();
        if arg[0] == b'*' {
            return Ok(current_time_millis());
        }
    }
    let value = if let Ok(value) = arg.parse_integer() {
        value
    } else {
        let arg_str = arg.to_string_lossy();
        let value = DateTime::parse_from_rfc3339(&arg_str)
            .map_err(|_| ValkeyError::Str("TSDB: invalid timestamp"))?;
        value.timestamp_millis()
    };
    if value < 0 {
        return Err(ValkeyError::Str("TSDB: invalid timestamp, must be a non-negative value"));
    }
    Ok(value as Timestamp)
}

pub fn parse_timestamp(arg: &str) -> ValkeyResult<Timestamp> {
    // todo: handle +,
    if arg == "*" {
        return Ok(current_time_millis());
    }
    let value = if let Ok(dt) = arg.parse::<i64>() {
        dt
    } else {
        let value = DateTime::parse_from_rfc3339(arg)
            .map_err(|_| ValkeyError::Str("invalid timestamp"))?;
        value.timestamp_millis()
    };
    if value < 0 {
        return Err(ValkeyError::Str("TSDB: invalid timestamp, must be a non-negative integer"));
    }
    Ok(value)
}

pub fn parse_timestamp_range_value(arg: &str) -> ValkeyResult<TimestampRangeValue> {
    TimestampRangeValue::try_from(arg)
}

pub fn parse_timestamp_range(start: &ValkeyString, end: &ValkeyString) -> ValkeyResult<TimestampRange> {
    let first: TimestampRangeValue = start.try_into()?;
    let second: TimestampRangeValue = end.try_into()?;
    TimestampRange::new(first, second)
}

pub fn parse_duration_arg(arg: &ValkeyString) -> ValkeyResult<Duration> {
    if let Ok(value) = arg.parse_integer() {
        if value < 0 {
            return Err(ValkeyError::Str("TSDB: invalid duration, must be a non-negative integer"));
        }
        return Ok(Duration::from_millis(value as u64));
    }
    let value_str = arg.to_string_lossy();
    parse_duration(&value_str)
}

pub fn parse_duration(arg: &str) -> ValkeyResult<Duration> {
    match parse_duration_value(arg, 1) {
        Ok(d) => Ok(Duration::from_millis(d as u64)),
        Err(_e) => {
            match arg.parse::<i64>() {
                Ok(v) => Ok(Duration::from_millis(v as u64)),
                Err(_e) => {
                    let str = format!("ERR: failed to parse duration: {}", arg);
                    Err(ValkeyError::String(str))
                },
            }
        },
    }
}

pub fn parse_double(arg: &str) -> ValkeyResult<f64> {
    arg.parse::<f64>().map_err(|_e| {
        ValkeyError::Str("TSDB: invalid value")
    })
}

pub fn parse_number_with_unit(arg: &str) -> TsdbResult<f64> {
    parse_number(arg).map_err(|_e| {
        TsdbError::InvalidNumber(arg.to_string())
    })
}

pub fn parse_series_selector(arg: &str) -> TsdbResult<Matchers> {
    parse_metric_selector(arg).map_err(|_e| {
        TsdbError::InvalidSeriesSelector(arg.to_string())
    })
}

pub fn parse_chunk_size(arg: &str) -> ValkeyResult<usize> {
    fn get_error_result() -> ValkeyResult<usize> {
        let msg = format!("TSDB: CHUNK_SIZE value must be an integer multiple of 2 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]");
        Err(ValkeyError::String(msg))
    }

    let chunk_size = parse_number_with_unit(arg).map_err(|_e| {
        ValkeyError::Str("TSDB: invalid chunk size")
    })?;

    if chunk_size != chunk_size.floor() {
        return get_error_result()
    }
    if chunk_size < MIN_CHUNK_SIZE as f64 || chunk_size > MAX_CHUNK_SIZE as f64 {
        return get_error_result()
    }
    let chunk_size = chunk_size as usize;
    if chunk_size % 2 != 0 {
        return get_error_result()
    }
    Ok(chunk_size)
}