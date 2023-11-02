use std::cmp::Ordering;
use ahash::AHashMap;
use redis_module::RedisString;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use get_size::GetSize;

mod chunk;
mod compressed_chunk;
mod constants;
mod dedup;
mod encoding;
mod merge;
mod slice;
pub mod time_series;
mod uncompressed_chunk;
mod utils;
mod series_data;
mod defrag;

use crate::error::{TsdbError, TsdbResult};
pub(super) use chunk::*;
pub(crate) use constants::*;
pub(crate) use slice::*;
pub(crate) use series_data::*;
pub(crate) use defrag::*;

pub type Timestamp = metricsql_engine::prelude::Timestamp;

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
            return if self.value.is_nan() {
                other.value.is_nan()
            } else {
                self.value == other.value
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


#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[derive(GetSize)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    pub fn new<S: Into<String>>(key: S, value: String) -> Self {
        Self {
            name: key.into(),
            value,
        }
    }
}

impl PartialOrd for Label {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.name == other.name {
            return Some(self.value.cmp(&other.value));
        }
        Some(self.name.cmp(&other.name))
    }
}


#[non_exhaustive]
#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
#[derive(GetSize)]
pub enum Encoding {
    #[default]
    Compressed,
    Uncompressed,
}

impl Encoding {
    pub fn is_compressed(&self) -> bool {
        match self {
            Encoding::Compressed => true,
            Encoding::Uncompressed => false,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Encoding::Compressed => "COMPRESSED",
            Encoding::Uncompressed => "UNCOMPRESSED",
        }
    }
}

impl Display for Encoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for Encoding {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("compressed") => Ok(Encoding::Compressed),
            s if s.eq_ignore_ascii_case("uncompressed") => Ok(Encoding::Uncompressed),
            _ => Err(format!("invalid encoding: {}", s)),
        }
    }
}

#[derive(Debug, Default, PartialEq, Deserialize, Serialize, Clone, Copy)]
#[derive(GetSize)]
pub enum DuplicatePolicy {
    /// ignore any newly reported value and reply with an error
    #[default]
    Block,
    /// ignore any newly reported value
    KeepFirst,
    /// overwrite the existing value with the new value
    KeepLast,
    /// only override if the value is lower than the existing value
    Min,
    /// only override if the value is higher than the existing value
    Max,
    /// append the new value to the existing value
    Sum,
}

impl Display for DuplicatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DuplicatePolicy::Block => write!(f, "block"),
            DuplicatePolicy::KeepFirst => write!(f, "first"),
            DuplicatePolicy::KeepLast => write!(f, "last"),
            DuplicatePolicy::Min => write!(f, "min"),
            DuplicatePolicy::Max => write!(f, "max"),
            DuplicatePolicy::Sum => write!(f, "sum"),
        }
    }
}

impl DuplicatePolicy {
    pub fn value_on_duplicate(self, ts: Timestamp, old: f64, new: f64) -> TsdbResult<f64> {
        use DuplicatePolicy::*;
        let has_nan = old.is_nan() || new.is_nan();
        if has_nan && self != Block {
            // take the valid sample regardless of policy
            let value = if new.is_nan() { old } else { new };
            return Ok(value);
        }
        Ok(match self {
            Block => {
                // todo: format storage as iso-8601 or rfc3339
                let msg = format!("{new} @ {ts}");
                return Err(TsdbError::DuplicateSample(msg));
            }
            KeepFirst => old,
            KeepLast => new,
            Min => old.min(new),
            Max => old.max(new),
            Sum => old + new,
        })
    }
}

impl FromStr for DuplicatePolicy {
    type Err = TsdbError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use DuplicatePolicy::*;

        match s {
            s if s.eq_ignore_ascii_case("block") => Ok(Block),
            s if s.eq_ignore_ascii_case("first") => Ok(KeepFirst),
            s if s.eq_ignore_ascii_case("keepfirst") => Ok(KeepFirst),
            s if s.eq_ignore_ascii_case("last") => Ok(KeepLast),
            s if s.eq_ignore_ascii_case("keeplast") => Ok(KeepLast),
            s if s.eq_ignore_ascii_case("min") => Ok(Min),
            s if s.eq_ignore_ascii_case("max") => Ok(Max),
            s if s.eq_ignore_ascii_case("sum") => Ok(Sum),
            _ => Err(TsdbError::General(format!("invalid duplicate policy: {s}"))),
        }
    }
}
impl From<&str> for DuplicatePolicy {
    fn from(s: &str) -> Self {
        DuplicatePolicy::from_str(s).unwrap()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DuplicateStatus {
    Ok,
    Err,
    Deduped,
}

#[derive(Debug, Default, Clone)]
pub struct TimeSeriesOptions {
    pub metric_name: Option<String>,
    pub encoding: Option<Encoding>,
    pub chunk_size: Option<usize>,
    pub retention: Option<Duration>,
    pub duplicate_policy: Option<DuplicatePolicy>,
    pub dedupe_interval: Option<Duration>,
    pub labels: Option<AHashMap<String, String>>,
}

impl TimeSeriesOptions {
    pub fn new(key: &RedisString) -> Self {
        Self {
            metric_name: Some(key.to_string()),
            encoding: None,
            chunk_size: Some(DEFAULT_CHUNK_SIZE_BYTES),
            retention: None,
            duplicate_policy: None,
            dedupe_interval: None,
            labels: Default::default(),
        }
    }

    pub fn encoding(&mut self, encoding: Encoding) {
        self.encoding = Some(encoding);
    }

    pub fn chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = Some(chunk_size);
    }

    pub fn retention(&mut self, retention: Duration) {
        self.retention = Some(retention);
    }

    pub fn duplicate_policy(&mut self, duplicate_policy: DuplicatePolicy) {
        self.duplicate_policy = Some(duplicate_policy);
    }

    pub fn labels(&mut self, labels: AHashMap<String, String>) {
        self.labels = Some(labels);
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct ValueFilter {
    pub min: f64,
    pub max: f64,
}

impl ValueFilter {
    pub(crate) fn new(min: f64, max: f64) -> TsdbResult<Self> {
        if min > max {
            return Err(TsdbError::General("ERR invalid range".to_string()));
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use crate::error::TsdbError;
    use crate::storage::DuplicatePolicy;

    #[test]
    fn test_duplicate_policy_parse() {
        assert!(matches!(DuplicatePolicy::from_str("block"), Ok(DuplicatePolicy::Block)));
        assert!(matches!(DuplicatePolicy::from_str("last"), Ok(DuplicatePolicy::KeepLast)));
        assert!(matches!(DuplicatePolicy::from_str("keepLast"), Ok(DuplicatePolicy::KeepLast)));
        assert!(matches!(DuplicatePolicy::from_str("first"), Ok(DuplicatePolicy::KeepFirst)));
        assert!(matches!(DuplicatePolicy::from_str("KeEpFIRst"), Ok(DuplicatePolicy::KeepFirst)));
        assert!(matches!(DuplicatePolicy::from_str("min"), Ok(DuplicatePolicy::Min)));
        assert!(matches!(DuplicatePolicy::from_str("max"), Ok(DuplicatePolicy::Max)));
        assert!(matches!(DuplicatePolicy::from_str("sum"), Ok(DuplicatePolicy::Sum)));
    }

    #[test]
    fn test_duplicate_policy_handle_duplicate() {
        let dp = DuplicatePolicy::Block;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert!(matches!(dp.value_on_duplicate(ts, old, new), Err(TsdbError::DuplicateSample(_))));

        let dp = DuplicatePolicy::KeepFirst;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.value_on_duplicate(ts, old, new).unwrap(), old);

        let dp = DuplicatePolicy::KeepLast;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.value_on_duplicate(ts, old, new).unwrap(), new);

        let dp = DuplicatePolicy::Min;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.value_on_duplicate(ts, old, new).unwrap(), old);

        let dp = DuplicatePolicy::Max;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.value_on_duplicate(ts, old, new).unwrap(), new);

        let dp = DuplicatePolicy::Sum;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.value_on_duplicate(ts, old, new).unwrap(), old + new);
    }

    #[test]
    fn test_duplicate_policy_handle_nan() {
        use DuplicatePolicy::*;

        let dp = Block;
        let ts = 0;
        let old = 1.0;
        let new = f64::NAN;
        assert!(matches!(dp.value_on_duplicate(ts, old, new), Err(TsdbError::DuplicateSample(_))));

        let policies = [KeepFirst, KeepLast, Min, Max, Sum];
        for policy in policies {
            assert_eq!(policy.value_on_duplicate(ts, 10.0, f64::NAN).unwrap(), 10.0);
            assert_eq!(policy.value_on_duplicate(ts, f64::NAN, 8.0).unwrap(), 8.0);
        }
    }
}