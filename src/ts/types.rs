use std::cmp::Ordering;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;

use ahash::AHashMap;
use redis_module::RedisString;
use serde::{Deserialize, Serialize};

use crate::module::DEFAULT_CHUNK_SIZE_BYTES;
use crate::ts::DuplicatePolicy;

pub type Timestamp = metricsql_engine::prelude::Timestamp;
pub type Labels = AHashMap<String, String>;

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
        Sample { timestamp: time, value }
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

#[non_exhaustive]
#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
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
        match s.to_ascii_lowercase().as_str() {
            "compressed" => Ok(Encoding::Compressed),
            "uncompressed" => Ok(Encoding::Uncompressed),
            _ => Err(format!("invalid encoding: {}", s)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeSeriesOptions {
    pub metric_name: String,
    pub encoding: Option<Encoding>,
    pub chunk_size: Option<usize>,
    pub retention: Option<Duration>,
    pub dedupe_interval: Option<Duration>,
    pub duplicate_policy: Option<DuplicatePolicy>,
    pub labels: AHashMap<String, String>,
}

impl TimeSeriesOptions {
    pub fn new(key: &RedisString) -> Self {
        Self {
            metric_name: key.to_string(),
            encoding: None,
            chunk_size: Some(DEFAULT_CHUNK_SIZE_BYTES),
            retention: None,
            dedupe_interval: None,
            duplicate_policy: None,
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
    pub fn dedupe_interval(&mut self, interval: Duration) {
        self.dedupe_interval = Some(interval);
    }
    pub fn labels(&mut self, labels: AHashMap<String, String>) {
        self.labels = labels;
    }
}