use crate::error::{TsdbError, TsdbResult};
use crate::ts::{DuplicatePolicy};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use ahash::AHashSet;
use redis_module::{RedisError, RedisResult};
use crate::common::types::{Sample, Timestamp};
use crate::index::RedisContext;
use crate::ts::compressed_chunk::CompressedChunk;
use crate::ts::uncompressed_chunk::UncompressedChunk;

pub const MIN_CHUNK_SIZE: usize = 48;
pub const MAX_CHUNK_SIZE: usize = 1048576;

#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ChunkCompression {
    Uncompressed,
    #[default]
    Compressed,
}

impl Display for ChunkCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkCompression::Uncompressed => write!(f, "uncompressed"),
            ChunkCompression::Compressed => write!(f, "compressed"),
        }
    }
}

impl TryFrom<&str> for ChunkCompression {
    type Error = TsdbError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "uncompressed" => Ok(ChunkCompression::Uncompressed),
            "quantile" => Ok(ChunkCompression::Compressed),
            _ => Err(TsdbError::InvalidCompression(s.to_string())),
        }
    }
}

pub trait Chunk: Sized {
    fn first_timestamp(&self) -> Timestamp;
    fn last_timestamp(&self) -> Timestamp;
    fn num_samples(&self) -> usize;
    fn last_value(&self) -> f64;
    fn size(&self) -> usize;
    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize>;
    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()>;

    fn get_range(&self, start: Timestamp, end: Timestamp, timestamps: &mut Vec<i64>, values: &mut Vec<f64>) -> TsdbResult<()>;

    fn upsert_sample(
        &mut self,
        sample: &mut Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize>;
    fn split(&mut self) -> TsdbResult<Self>;
    fn overlaps(&self, start_ts: i64, end_ts: i64) -> bool {
        self.first_timestamp() <= end_ts && self.last_timestamp() >= start_ts
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimeSeriesChunk {
    Uncompressed(UncompressedChunk),
    Compressed(CompressedChunk),
}

impl TimeSeriesChunk {
    pub fn new(compression: ChunkCompression, chunk_size: usize, timestamps: &[Timestamp], values: &[f64]) -> TsdbResult<Self> {
        use TimeSeriesChunk::*;
        match compression {
            ChunkCompression::Uncompressed => {
                let chunk = UncompressedChunk::new(chunk_size,timestamps.to_vec(), values.to_vec());
                Ok(Uncompressed(chunk))
            },
            ChunkCompression::Compressed => {
                let chunk = CompressedChunk::with_values(chunk_size, timestamps, values)?;
                Ok(Compressed(chunk))
            },
            _ => panic!("unsupported compression type"),
        }
    }

    pub fn compression(&self) -> ChunkCompression {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(_) => ChunkCompression::Uncompressed,
            Compressed(_) => ChunkCompression::Compressed,
        }
    }

    pub fn is_compressed(&self) -> bool {
        matches!(self, TimeSeriesChunk::Uncompressed(_))
    }

    pub fn is_empty(&self) -> bool {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.is_empty(),
            Compressed(chunk) => chunk.is_empty(),
        }
    }

    pub fn is_full(&self) -> bool {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.is_full(),
            Compressed(chunk) => chunk.is_full(),
        }
    }

    pub fn clear(&mut self) {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.clear(),
            Compressed(chunk) => chunk.clear(),
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

    pub fn process_range<F, State>(&mut self, state: &mut State, start: Timestamp, end: Timestamp, f: F) -> TsdbResult<()>
        where F: FnMut(&mut State, &[i64], &[f64]) -> TsdbResult<()> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => {
                chunk.process_range(start, end, state, f)
            },
            Compressed(chunk) => {
                chunk.process_range(start, end, state, f)
            },
        }
    }

    pub fn merge_samples(
        &mut self,
        samples: &[Sample],
        min_timestamp: Timestamp,
        duplicate_policy: DuplicatePolicy,
        duplicates: &mut AHashSet<Timestamp>) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => {
                chunk.merge_samples(samples, min_timestamp, duplicate_policy, duplicates)
            },
            Compressed(chunk) => {
                chunk.merge_samples(samples, min_timestamp, duplicate_policy, duplicates)
            },
        }
    }
}

impl Chunk for TimeSeriesChunk {
    fn first_timestamp(&self) -> Timestamp {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(uncompressed) => uncompressed.first_timestamp(),
            Compressed(chunk) => chunk.first_timestamp()
        }
    }

    fn last_timestamp(&self) -> Timestamp {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.last_timestamp(),
            Compressed(chunk) => chunk.last_timestamp()
        }
    }

    fn num_samples(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.num_samples(),
            Compressed(chunk) => chunk.num_samples()
        }
    }

    fn last_value(&self) -> f64 {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.last_value(),
            Compressed(chunk) => chunk.last_value()
        }
    }

    fn size(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.size(),
            Compressed(chunk) => chunk.size()
        }
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.remove_range(start_ts, end_ts),
            Compressed(chunk) => chunk.remove_range(start_ts, end_ts)
        }
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.add_sample(sample),
            Compressed(chunk) => chunk.add_sample(sample)
        }
    }

    fn get_range(&self, start: Timestamp, end: Timestamp, timestamps: &mut Vec<i64>, values: &mut Vec<f64>) -> TsdbResult<()> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.get_range(start, end, timestamps, values),
            Compressed(chunk) => chunk.get_range(start, end, timestamps, values)
        }
    }

    fn upsert_sample(&mut self, sample: &mut Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.upsert_sample(sample, dp_policy),
            Compressed(chunk) => chunk.upsert_sample(sample, dp_policy)
        }
    }

    fn split(&mut self) -> TsdbResult<Self> where Self: Sized {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => Ok(Uncompressed(chunk.split()?)),
            Compressed(chunk) => Ok(Compressed(chunk.split()?))
        }
    }
}

pub(crate) fn validate_chunk_size(ctx: &RedisContext, chunk_size_bytes: usize) -> RedisResult<()> {
    fn get_error_result() -> RedisResult<()> {
        let msg = format!("TSDB: CHUNK_SIZE value must be a multiple of 2 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]");
        Err(RedisError::String(msg))
    }

    if chunk_size_bytes < MIN_CHUNK_SIZE {
        return get_error_result()
    }

    if chunk_size_bytes > MAX_CHUNK_SIZE {
        return get_error_result()
    }

    if chunk_size_bytes % 2 != 0 {
        return get_error_result()
    }

    Ok(())
}
