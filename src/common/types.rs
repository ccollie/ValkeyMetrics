use crate::common::parse_timestamp;
use metricsql_engine::TimestampTrait;
use redis_module::{RedisError, RedisResult, RedisString};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Display;

pub trait AToAny: 'static {
    fn as_any(&self) -> &dyn Any;
}

impl<T: 'static> AToAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}


pub type Timestamp = metricsql_engine::prelude::Timestamp;
pub type PooledTimestampVec = metricsql_common::pool::PooledVecI64;
pub type PooledValuesVec = metricsql_common::pool::PooledVecF64;

/// Represents a data point in time series.
#[derive(Debug, Deserialize, Serialize)]
pub struct Sample {
    /// Timestamp from epoch.
    pub(crate) timestamp: Timestamp,

    /// Value for this data point.
    pub(crate) value: f64,
}

impl Sample {
    /// Create a new DataPoint from given time and value.
    pub fn new(time: Timestamp, value: f64) -> Self {
        Sample {
            timestamp: time,
            value,
        }
    }

    /// Get time.
    pub fn get_time(&self) -> i64 {
        self.timestamp
    }

    /// Get value.
    pub fn get_value(&self) -> f64 {
        self.value
    }
}

impl Clone for Sample {
    fn clone(&self) -> Sample {
        Sample {
            timestamp: self.get_time(),
            value: self.get_value(),
        }
    }
}

impl PartialEq for Sample {
    #[inline]
    fn eq(&self, other: &Sample) -> bool {
        // Two data points are equal if their times are equal, and their values are either equal or are NaN.

        if self.timestamp == other.timestamp {
            if self.value.is_nan() {
                return other.value.is_nan();
            } else {
                return self.value == other.value;
            }
        }
        false
    }
}

impl Eq for Sample {}

impl Ord for Sample {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for Sample {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub const SAMPLE_SIZE: usize = std::mem::size_of::<Sample>();
pub const MAX_TIMESTAMP: i64 = 253402300799;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Copy)]
pub enum TimestampRangeValue {
    Earliest,
    Latest,
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
}

impl TryFrom<&str> for TimestampRangeValue {
    type Error = RedisError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "-" => Ok(TimestampRangeValue::Earliest),
            "+" => Ok(TimestampRangeValue::Latest),
            "*" => Ok(TimestampRangeValue::Now),
            _ => {
                let ts =
                    parse_timestamp(value).map_err(|_| RedisError::Str("invalid timestamp"))?;
                if ts < 0 {
                    return Err(RedisError::Str(
                        "TSDB: invalid timestamp, must be a non-negative integer",
                    ));
                }
                Ok(TimestampRangeValue::Value(ts))
            }
        }
    }
}

impl TryFrom<&RedisString> for TimestampRangeValue {
    type Error = RedisError;

    fn try_from(value: &RedisString) -> Result<Self, Self::Error> {
        if value.len() == 1 {
            let bytes = value.as_slice();
            match bytes[0] {
                b'-' => return Ok(TimestampRangeValue::Earliest),
                b'+' => return Ok(TimestampRangeValue::Latest),
                b'*' => return Ok(TimestampRangeValue::Now),
                _ => {}
            }
        }
        return if let Ok(int_val) = value.parse_integer() {
            if int_val < 0 {
                return Err(RedisError::Str(
                    "TSDB: invalid timestamp, must be a non-negative integer",
                ));
            }
            Ok(TimestampRangeValue::Value(int_val))
        } else {
            let date_str = value.to_string_lossy();
            let ts =
                parse_timestamp(&date_str).map_err(|_| RedisError::Str("invalid timestamp"))?;
            Ok(TimestampRangeValue::Value(ts))
        };
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
    pub fn new(start: TimestampRangeValue, end: TimestampRangeValue) -> RedisResult<Self> {
        if start > end {
            return Err(RedisError::Str("invalid timestamp range: start > end"));
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
