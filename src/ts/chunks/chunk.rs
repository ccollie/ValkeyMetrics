use std::fmt::Display;
use serde::{Deserialize, Serialize};
use crate::error::{TsdbError, TsdbResult};
use crate::ts::{DuplicatePolicy, Sample};
use crate::ts::utils::trim_data;

// see also https://github.com/influxdata/influxdb/tree/main/influxdb_tsm/src
pub(crate) type Timestamp = metricsql_engine::prelude::Timestamp;

#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Compression {
    Uncompressed,
    PCodec,
    #[default]
    Quantile,
}

impl Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::Uncompressed => write!(f, "uncompressed"),
            Compression::PCodec => write!(f, "pcodec"),
            Compression::Quantile => write!(f, "quantile"),
        }
    }
}

impl TryFrom<&str> for Compression {
    type Error = TsdbError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "uncompressed" => Ok(Compression::Uncompressed),
            "pcodec" => Ok(Compression::PCodec),
            "quantile" => Ok(Compression::Quantile),
            _ => Err(TsdbError::InvalidCompression(s.to_string())),
        }
    }
}

pub struct DataPage<'a> {
    pub timestamps: &'a [Timestamp],
    pub values: &'a [f64],
}

impl<'a> DataPage<'a> {
    pub fn new(timestamps: &'a [Timestamp], values: &'a [f64]) -> Self {
        Self {
            timestamps,
            values,
        }
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn first_timestamp(&self) -> Option<i64> {
        if self.timestamps.is_empty() {
            return None;
        }
        Some(self.timestamps[0])
    }

    pub fn overlaps(&self, start_ts: i64, end_ts: i64) -> bool {
        let ts = &self.timestamps[0..];
        if ts.is_empty() {
            return false;
        }
        return start_ts <= ts[ts.len() - 1] && end_ts >= ts[0];
    }

    pub fn contains_timestamp(&self, ts: Timestamp) -> bool {
        let timestamps = &self.timestamps[0..];
        if timestamps.is_empty() {
            return false;
        }
        return ts >= timestamps[0] && ts <= timestamps[timestamps.len() - 1];
    }

    pub fn trim(&self, start_ts: Timestamp, end_ts: Timestamp) -> (&[i64], &[f64]) {
        trim_data(self.timestamps, self.values, start_ts, end_ts)
    }

}

#[derive(PartialEq)]
pub enum ChunkRangeVisitorContinuation {
    Continue,
    Stop,
}

/// Trait that implements the [Visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern)
/// to efficiently walk a chunk range. [`pre_visit`](ChunkRangeVisitor::pre_visit) is called
/// before the range iteration begins, and then [`post_visit`](ChunkRangeVisitor::post_visit) is called
/// after all samples in the range have been visited. [`visit`](ChunkRangeVisitor::visit) is called
/// for all data pages in the range.
pub trait ChunkRangeVisitor {
    /// Called before any children are visited. Return `Ok(false)` to cut short the iteration
    /// (skip traversing and return).
    fn pre_visit(&mut self, chunk: &impl TimesSeriesBlock) -> Result<ChunkRangeVisitorContinuation, TsdbError> {
        Ok(ChunkRangeVisitorContinuation::Continue)
    }

    fn visit(&mut self, page: &DataPage, finished: bool) -> Result<ChunkRangeVisitorContinuation, TsdbError>;

    /// Called after all sanples are visited. Return `Ok(false)` to cut short the iteration
    /// (skip traversing and return).
    fn post_visit(&mut self, _plan: &impl TimesSeriesBlock) -> TsdbResult<()> {
        Ok(())
    }
}


pub trait TimesSeriesBlock {
    fn first_timestamp(&self) -> Timestamp;
    fn last_timestamp(&self) -> Timestamp;
    fn num_samples(&self) -> usize;
    fn last_value(&self) -> f64;
    fn size(&self) -> usize;
    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize>;
    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()>;
    fn upsert_sample(&mut self, sample: &mut Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize>;
    fn split(&mut self) -> TsdbResult<Self> where Self: Sized;
    fn overlaps(&self, start_ts: i64, end_ts: i64) -> bool {
        self.first_timestamp() <= end_ts && self.last_timestamp() >= start_ts
    }
}