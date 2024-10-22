use std::cmp::Ordering;
use std::fmt::Display;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyString};
use crate::common::types::Timestamp;
use crate::series::TimeSeries;
use crate::common::current_time_millis;
use crate::config::get_global_settings;
use crate::series::MAX_TIMESTAMP;

#[derive(Clone, Default, Debug, PartialEq, Eq, Copy)]
pub enum TimestampValue {
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

impl TimestampValue {
    pub fn as_timestamp(&self) -> Timestamp {
        use TimestampValue::*;
        match self {
            Earliest => 0,
            Latest => MAX_TIMESTAMP,
            Now => current_time_millis(),
            Value(ts) => *ts,
            Relative(delta) => current_time_millis().wrapping_add(*delta),
        }
    }

    pub fn as_series_timestamp(&self, series: &TimeSeries) -> Timestamp {
        use TimestampValue::*;
        match self {
            Earliest => series.first_timestamp,
            Latest => series.last_timestamp,
            Now => current_time_millis(), // todo: use valkey server value
            Value(ts) => *ts,
            Relative(delta) => current_time_millis().wrapping_add(*delta),
        }
    }
}

impl TryFrom<&str> for TimestampValue {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        use TimestampValue::*;
        use crate::error_consts;
        use crate::module::arg_parse::{parse_duration_ms, parse_timestamp};
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
                    .map_err(|_| ValkeyError::Str(error_consts::INVALID_TIMESTAMP))?;

                if ts < 0 {
                    return Err(ValkeyError::Str(
                        "ERR: invalid timestamp, must be a non-negative integer",
                    ));
                }

                Ok(Value(ts))
            }
        }
    }
}

impl TryFrom<&ValkeyString> for TimestampValue {
    type Error = ValkeyError;

    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        use TimestampValue::*;
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
                    "ERR: invalid timestamp, must be a non-negative integer",
                ));
            }
            Ok(Value(int_val))
        } else {
            let date_str = value.to_string_lossy();
            date_str.as_str().try_into()
        }
    }
}

impl From<Timestamp> for TimestampValue {
    fn from(ts: Timestamp) -> Self {
        TimestampValue::Value(ts)
    }
}

impl Display for TimestampValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use metricsql_common::humanize::humanize_duration_ms;
        use TimestampValue::*;
        match self {
            Earliest => write!(f, "-"),
            Latest => write!(f, "+"),
            Value(ts) => write!(f, "{}", ts),
            Now => write!(f, "*"),
            Relative(delta) => write!(f, "{}", humanize_duration_ms(*delta))
        }
    }
}

impl PartialOrd for TimestampValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use TimestampValue::*;
        use crate::common::current_time_millis;

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
    pub start: TimestampValue,
    pub end: TimestampValue,
}

impl TimestampRange {
    pub fn new(start: TimestampValue, end: TimestampValue) -> ValkeyResult<Self> {
        if start > end {
            return Err(ValkeyError::Str("ERR invalid timestamp range: start > end"));
        }
        Ok(TimestampRange { start, end })
    }

    pub fn get_series_range(&self, series: &TimeSeries, check_retention: bool) -> (Timestamp, Timestamp) {
        use TimestampValue::*;

        // In case a retention is set shouldn't return chunks older than the retention
        let mut start_timestamp = self.start.as_series_timestamp(series);
        let end_timestamp = if let Relative(delta) = self.end {
            start_timestamp.wrapping_add(delta)
        } else {
            self.end.as_series_timestamp(series)
        };

        if check_retention && !series.retention.is_zero() {
            let retention_ms = series.retention.as_millis() as i64;
            let earliest = series.last_timestamp.wrapping_sub(retention_ms);
            start_timestamp = start_timestamp.max(earliest);
        }

        (start_timestamp, end_timestamp)
    }

    pub fn get_timestamps(&self) -> (Timestamp, Timestamp) {
        use TimestampValue::*;

        let start_timestamp = self.start.as_timestamp();
        let end_timestamp = if let Relative(delta) = self.end {
            start_timestamp.wrapping_add(delta)
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
            start: TimestampValue::Earliest,
            end: TimestampValue::Latest,
        }
    }
}

pub(crate) fn normalize_range_args(
    start: Option<TimestampValue>,
    end: Option<TimestampValue>,
) -> ValkeyResult<(Timestamp, Timestamp)> {
    let config = get_global_settings();
    let now = current_time_millis();

    let start = if let Some(val) = start {
        val.as_timestamp()
    } else {
        let ts = now - (config.default_step.as_millis() as i64); // todo: how to avoid overflow?
        ts as Timestamp
    };

    let end = if let Some(val) = end {
        val.as_timestamp()
    } else {
        now
    };

    if start > end {
        return Err(ValkeyError::Str(
            "ERR end timestamp must not be before start time",
        ));
    }

    Ok((start, end))
}