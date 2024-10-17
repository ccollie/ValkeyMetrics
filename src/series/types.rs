use crate::aggregators::Aggregator;
use crate::common::current_time_millis;
use crate::common::rounding::{round_to_decimal_digits, round_to_sig_figs};
use crate::common::types::{Matchers, Timestamp};
use crate::module::arg_parse::{parse_duration_ms, parse_timestamp};
use crate::series::time_series::TimeSeries;
use crate::series::MAX_TIMESTAMP;
use get_size::GetSize;
use metricsql_common::humanize::humanize_duration_ms;
use std::cmp::Ordering;
use std::fmt::Display;
use std::time::Duration;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyString};

#[derive(Clone, Debug, PartialEq, Copy)]
#[derive(GetSize)]
pub enum RoundingStrategy {
    SignificantDigits(i32),
    DecimalDigits(i32),
}

impl RoundingStrategy {
    pub fn round(&self, value: f64) -> f64 {
        match self {
            RoundingStrategy::SignificantDigits(digits) => round_to_sig_figs(value, *digits),
            RoundingStrategy::DecimalDigits(digits) => round_to_decimal_digits(value, *digits),
        }
    }
}

impl Display for RoundingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoundingStrategy::SignificantDigits(digits) => write!(f, "significant_digits({})", digits),
            RoundingStrategy::DecimalDigits(digits) => write!(f, "decimal_digits({})", digits),
        }
    }
}

#[derive(Clone, Default, Debug, PartialEq, Copy)]
pub enum TimestampRangeValue {
    /// The timestamp of the earliest sample in the series
    Earliest,
    /// The timestamp of the latest sample in the series
    Latest,
    #[default]
    /// The current time
    Now,
    /// A specific timestamp
    Value(Timestamp),
    /// A timestamp with a given delta from the current timestamp
    Relative(i64)
}

impl TimestampRangeValue {
    pub fn as_timestamp(&self) -> Timestamp {
        use TimestampRangeValue::*;
        match self {
            Earliest => 0,
            Latest => MAX_TIMESTAMP,
            Now => current_time_millis(),
            Value(ts) => *ts,
            Relative(delta) => current_time_millis() + *delta,
        }
    }

    pub fn as_series_timestamp(&self, series: &TimeSeries) -> Timestamp {
        use TimestampRangeValue::*;
        match self {
            Earliest => series.first_timestamp,
            Latest => series.last_timestamp,
            Now => current_time_millis(), // todo: use valkey server value
            Value(ts) => *ts,
            Relative(delta) => current_time_millis() + *delta,
        }
    }
}

impl TryFrom<&str> for TimestampRangeValue {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        use TimestampRangeValue::*;
        match value {
            "-" => Ok(Earliest),
            "+" => Ok(Latest),
            "*" => Ok(Now),
            _ => {
                // ergonomics. Support something like TS.RANGE key -6hrs -3hrs
                if let Some(ch) = value.chars().next() {
                    if ch == '-' || ch == '+' {
                        let delta = if ch == '+' {
                            parse_duration_ms(&value[1..])?
                        } else {
                            parse_duration_ms(value)?
                        };
                        return Ok(Relative(delta))
                    }
                }
                let ts = parse_timestamp(value)
                    .map_err(|_| ValkeyError::Str("invalid timestamp"))?;

                if ts < 0 {
                    return Err(ValkeyError::Str(
                        "TSDB: invalid timestamp, must be a non-negative integer",
                    ));
                }

                Ok(Value(ts))
            }
        }
    }
}

impl TryFrom<&ValkeyString> for TimestampRangeValue {
    type Error = ValkeyError;

    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        use TimestampRangeValue::*;
        if value.len() == 1 {
            let bytes = value.as_slice();
            match bytes[0] {
                b'-' => return Ok(Earliest),
                b'+' => return Ok(Latest),
                b'*' => return Ok(Now),
                _ => {}
            }
        }
        if let Ok(int_val) = value.parse_integer() {
            if int_val < 0 {
                return Err(ValkeyError::Str(
                    "TSDB: invalid timestamp, must be a non-negative integer",
                ));
            }
            Ok(Value(int_val))
        } else {
            let date_str = value.to_string_lossy();
            date_str.as_str().try_into()
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
        use TimestampRangeValue::*;
        match self {
            Earliest => write!(f, "-"),
            Latest => write!(f, "+"),
            Value(ts) => write!(f, "{}", ts),
            Now => write!(f, "*"),
            Relative(delta) => write!(f, "{}", humanize_duration_ms(*delta))
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
            (Relative(x), Relative(y)) => x.partial_cmp(y),
            (Value(a), Value(b)) => a.partial_cmp(b),
            (Now, Value(v)) => {
                let now = current_time_millis();
                now.partial_cmp(v)
            }
            (Value(v), Now) => {
                let now = current_time_millis();
                v.partial_cmp(&now)
            }
            (Relative(y), Now) => {
                y.partial_cmp(&0i64)
            },
            (Now, Relative(y)) => {
                0i64.partial_cmp(y)
            },
            (Value(_), Relative(y)) => {
                0i64.partial_cmp(y)
            },
            (Relative(delta), Value(y)) => {
                let relative = current_time_millis() + *delta;
                relative.partial_cmp(y)
            },
            (Earliest, _) => Some(Ordering::Less),
            (_, Earliest) => Some(Ordering::Greater),
            (Latest, _) => Some(Ordering::Greater),
            (_, Latest) => Some(Ordering::Less),
        }
    }
}

// todo: better naming
#[derive(Clone, Debug, PartialEq, Copy)]
pub struct TimestampRange {
    pub start: TimestampRangeValue,
    pub end: TimestampRangeValue,
}

impl TimestampRange {
    pub fn new(start: TimestampRangeValue, end: TimestampRangeValue) -> ValkeyResult<Self> {
        if start > end {
            return Err(ValkeyError::Str("ERR invalid timestamp range: start > end"));
        }
        Ok(TimestampRange { start, end })
    }

    pub fn get_series_range(&self, series: &TimeSeries, check_retention: bool) -> (Timestamp, Timestamp) {
        use TimestampRangeValue::*;

        // In case a retention is set shouldn't return chunks older than the retention
        let mut start_timestamp = self.start.as_series_timestamp(series);
        let end_timestamp = if let Relative(delta) = self.end {
            start_timestamp + delta
        } else {
            self.end.as_series_timestamp(series)
        };

        if check_retention && !series.retention.is_zero() {
            // todo: check for i64 overflow
            let retention_ms = series.retention.as_millis() as i64;
            let earliest = series.last_timestamp - retention_ms;
            start_timestamp = start_timestamp.max(earliest);
        }

        (start_timestamp, end_timestamp)
    }

    pub fn get_timestamps(&self) -> (Timestamp, Timestamp) {
        use TimestampRangeValue::*;

        let start_timestamp = self.start.as_timestamp();
        let end_timestamp = if let Relative(delta) = self.end {
            start_timestamp + delta
        } else {
            self.end.as_timestamp()
        };

        (start_timestamp, end_timestamp)
    }
}

impl Display for TimestampRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} to {}", self.start, self.end)
    }
}

impl Default for TimestampRange {
    fn default() -> Self {
        Self {
            start: TimestampRangeValue::Earliest,
            end: TimestampRangeValue::Latest,
        }
    }
}

pub struct MetadataFunctionArgs {
    pub start: Timestamp,
    pub end: Timestamp,
    pub matchers: Vec<Matchers>,
    pub limit: Option<usize>,
}

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct ValueFilter {
    pub min: f64,
    pub max: f64,
}

impl ValueFilter {
    pub(crate) fn new(min: f64, max: f64) -> ValkeyResult<Self> {
        if min > max {
            return Err(ValkeyError::Str("ERR invalid range"));
        }
        Ok(Self { min, max })
    }
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct RangeFilter {
    pub value: Option<ValueFilter>,
    pub timestamps: Option<Vec<Timestamp>>,
}

impl RangeFilter {
    pub fn new(value: Option<ValueFilter>, timestamps: Option<Vec<Timestamp>>) -> Self {
        Self { value, timestamps }
    }

    pub fn filter(&self, timestamp: Timestamp, value: f64) -> bool {
        if let Some(value_filter) = &self.value {
            if value < value_filter.min || value > value_filter.max {
                return false;
            }
        }
        if let Some(timestamps) = &self.timestamps {
            if !timestamps.contains(&timestamp) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum RangeAlignment {
    #[default]
    Default,
    Start,
    End,
    Timestamp(Timestamp),
}

impl RangeAlignment {
    pub fn get_aligned_timestamp(&self, start: Timestamp, end: Timestamp) -> Timestamp {
        match self {
            RangeAlignment::Default => 0,
            RangeAlignment::Start => start,
            RangeAlignment::End => end,
            RangeAlignment::Timestamp(ts) => *ts,
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketTimestamp {
    #[default]
    Start,
    End,
    Mid
}

impl BucketTimestamp {
    pub fn calculate(&self, ts: Timestamp, time_delta: i64) -> Timestamp {
        match self {
            Self::Start => ts,
            Self::Mid => ts + time_delta / 2,
            Self::End => ts + time_delta,
        }
    }

}
impl TryFrom<&str> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() == 1 {
            let c = value.chars().next().unwrap();
            match c {
                '-' => return Ok(BucketTimestamp::Start),
                '+' => return Ok(BucketTimestamp::End),
                _ => {}
            }
        }
        match value {
            value if value.eq_ignore_ascii_case("start") => return Ok(BucketTimestamp::Start),
            value if value.eq_ignore_ascii_case("end") => return Ok(BucketTimestamp::End),
            value if value.eq_ignore_ascii_case("mid") => return Ok(BucketTimestamp::Mid),
            _ => {}
        }
        Err(ValkeyError::Str("TSDB: invalid BUCKETTIMESTAMP parameter"))
    }
}

impl TryFrom<&ValkeyString> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        value.to_string_lossy().as_str().try_into()
    }
}

#[derive(Debug, Clone)]
pub struct AggregationOptions {
    pub aggregator: Aggregator,
    pub bucket_duration: Duration,
    pub timestamp_output: BucketTimestamp,
    pub alignment: RangeAlignment,
    pub time_delta: i64,
    pub empty: bool
}

#[derive(Debug, Clone)]
pub struct RangeGroupingOptions {
    pub(crate) aggregator: Aggregator,
    pub(crate) group_label: String,
}

#[derive(Debug, Default, Clone)]
pub struct RangeOptions {
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub aggregation: Option<AggregationOptions>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
    pub series_selector: Matchers,
    pub with_labels: bool,
    pub selected_labels: Vec<String>,
    pub grouping: Option<RangeGroupingOptions>,
}

impl RangeOptions {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self {
            date_range: TimestampRange {
                start: TimestampRangeValue::Value(start),
                end: TimestampRangeValue::Value(end),
            },
            ..Default::default()
        }
    }

    pub fn is_aggregation(&self) -> bool {
        self.aggregation.is_some()
    }
}
