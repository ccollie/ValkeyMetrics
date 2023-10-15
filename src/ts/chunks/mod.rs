pub mod uncompressed;
pub mod compressed;
mod chunk;

use serde::{Deserialize, Serialize};
pub use chunk::*;
use crate::error::TsdbResult;

use crate::ts::{DuplicatePolicy, Sample};
use crate::ts::chunks::compressed::quantile::QuantileCompressedChunk;
use crate::ts::chunks::uncompressed::UncompressedChunk;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimeSeriesChunk {
    Uncompressed(UncompressedChunk),
    QuantileCompressed(QuantileCompressedChunk),
}

impl TimeSeriesChunk {
    pub fn new(compression: Compression, timestamps: &[Timestamp], values: &[f64]) -> TsdbResult<Self> {
        use TimeSeriesChunk::*;
        match compression {
            Compression::Uncompressed => {
                let chunk = UncompressedChunk::new(timestamps.to_vec(), values.to_vec());
                Ok(Uncompressed(chunk))
            },
            Compression::Quantile => {
                let chunk = QuantileCompressedChunk::new(timestamps, values)?;
                Ok(QuantileCompressed(chunk))
            },
            _ => panic!("unsupported compression type"),
        }
    }

    pub fn compression(&self) -> Compression {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(_) => Compression::Uncompressed,
            QuantileCompressed(_) => Compression::Quantile,
        }
    }

    pub fn is_empty(&self) -> bool {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.is_empty(),
            QuantileCompressed(chunk) => chunk.is_empty(),
        }
    }

    pub fn is_timestamp_in_range(&self, ts: Timestamp) -> bool {
        ts >= self.first_timestamp() && ts <= self.last_timestamp()
    }

    pub fn is_contained_by_range(&self, start_ts: Timestamp, end_ts: Timestamp) -> bool {
        self.first_timestamp() >= start_ts && self.last_timestamp() <= end_ts
    }

    pub fn overlaps(&self, start_time: i64, end_time: i64) -> bool {
        let first_time = self.first_timestamp();
        let last_time = self.last_timestamp();
        first_time <= end_time && last_time >= start_time
    }

    pub fn iterate_range<F, State>(&self, state: &mut State, start: Timestamp, end: Timestamp, f: F) -> TsdbResult<()>
        where F: FnMut(&mut State, &[i64], &[f64], bool) -> TsdbResult<bool> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => {
                chunk.iterate_range(start, end, state, f)
            },
            QuantileCompressed(chunk) => {
                chunk.iterate_range(start, end, state, f)
            },
        }
    }
}

impl TimesSeriesBlock for TimeSeriesChunk {
    fn first_timestamp(&self) -> Timestamp {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(uncompressed) => uncompressed.first_timestamp(),
            QuantileCompressed(chunk) => chunk.first_timestamp()
        }
    }

    fn last_timestamp(&self) -> Timestamp {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.last_timestamp(),
            QuantileCompressed(chunk) => chunk.last_timestamp()
        }
    }

    fn num_samples(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.num_samples(),
            QuantileCompressed(chunk) => chunk.num_samples()
        }
    }

    fn last_value(&self) -> f64 {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.last_value(),
            QuantileCompressed(chunk) => chunk.last_value()
        }
    }

    fn size(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.size(),
            QuantileCompressed(chunk) => chunk.size()
        }
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.remove_range(start_ts, end_ts),
            QuantileCompressed(chunk) => chunk.remove_range(start_ts, end_ts)
        }
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.add_sample(sample),
            QuantileCompressed(chunk) => chunk.add_sample(sample)
        }
    }

    fn upsert_sample(&mut self, sample: &mut Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.upsert_sample(sample, dp_policy),
            QuantileCompressed(chunk) => chunk.upsert_sample(sample, dp_policy)
        }
    }

    fn split(&mut self) -> TsdbResult<Self> where Self: Sized {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => Ok(Uncompressed(chunk.split()?)),
            QuantileCompressed(chunk) => Ok(QuantileCompressed(chunk.split()?))
        }
    }
}