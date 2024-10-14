use super::{GorillaChunk, PcoChunk, UncompressedChunk};
use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::module::types::ValueFilter;
use crate::series::{DuplicatePolicy, Sample};
use ahash::AHashSet;
use get_size::GetSize;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::mem::size_of;
use valkey_module::error::{Error, GenericError};
use valkey_module::{raw, RedisModuleIO};

pub const MIN_CHUNK_SIZE: usize = 48;
pub const MAX_CHUNK_SIZE: usize = 1048576;

#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[derive(GetSize)]
#[non_exhaustive]
pub enum ChunkCompression {
    Uncompressed = 1,
    #[default]
    Gorilla = 2,
    Pco = 4,
}

impl ChunkCompression {
    pub fn name(&self) -> &'static str {
        match self {
            ChunkCompression::Uncompressed => "uncompressed",
            ChunkCompression::Gorilla => "gorilla",
            ChunkCompression::Pco => "pco",
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

impl Display for ChunkCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl TryFrom<u8> for ChunkCompression {
    type Error = TsdbError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ChunkCompression::Uncompressed),
            2 => Ok(ChunkCompression::Gorilla),
            4 => Ok(ChunkCompression::Pco),
            _ => Err(TsdbError::InvalidCompression(value.to_string())),
        }
    }
}

impl TryFrom<&str> for ChunkCompression {
    type Error = TsdbError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            s if s.eq_ignore_ascii_case("uncompressed") => Ok(ChunkCompression::Uncompressed),
            s if s.eq_ignore_ascii_case("gorilla") => Ok(ChunkCompression::Gorilla),
            s if s.eq_ignore_ascii_case("pco") => Ok(ChunkCompression::Pco),
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
    fn get_range(
        &self,
        start: Timestamp,
        end: Timestamp,
    ) -> TsdbResult<Vec<Sample>>;

    fn upsert_sample(
        &mut self,
        sample: &mut Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize>;
    fn split(&mut self) -> TsdbResult<Self>;
    fn overlaps(&self, start_ts: i64, end_ts: i64) -> bool {
        self.first_timestamp() <= end_ts && self.last_timestamp() >= start_ts
    }
    fn rdb_save(&self, rdb: *mut RedisModuleIO);
    fn rdb_load(rdb: *mut RedisModuleIO) -> Result<Self, Error>;
}

#[derive(Debug, Clone, PartialEq)]
#[derive(GetSize)]
pub enum TimeSeriesChunk {
    Uncompressed(UncompressedChunk),
    Gorilla(GorillaChunk),
    Pco(PcoChunk),
}

impl TimeSeriesChunk {
    pub fn new(
        compression: ChunkCompression,
        chunk_size: usize,
        samples: &[Sample],
    ) -> TsdbResult<Self> {
        use TimeSeriesChunk::*;
        match compression {
            ChunkCompression::Uncompressed => {
                let chunk = UncompressedChunk::new(chunk_size, samples);
                Ok(Uncompressed(chunk))
            }
            ChunkCompression::Gorilla => {
                let chunk = GorillaChunk::with_values(chunk_size, samples)?;
                Ok(Gorilla(chunk))
            }
            ChunkCompression::Pco => {
                let chunk = PcoChunk::with_values(chunk_size, samples)?;
                Ok(Pco(chunk))
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.is_empty(),
            Gorilla(chunk) => chunk.is_empty(),
            Pco(chunk) => chunk.is_empty(),
        }
    }

    pub fn max_size_in_bytes(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.max_size,
            Gorilla(chunk) => chunk.max_size,
            Pco(chunk) => chunk.max_size,
        }
    }

    pub fn is_full(&self) -> bool {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.is_full(),
            Gorilla(chunk) => chunk.is_full(),
            Pco(chunk) => chunk.is_full(),
        }
    }

    pub fn bytes_per_sample(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.bytes_per_sample(),
            Gorilla(chunk) => chunk.bytes_per_sample(),
            Pco(chunk) => chunk.bytes_per_sample(),
        }
    }

    pub fn utilization(&self) -> f64 {
        let used = self.size();
        let total = self.max_size_in_bytes();
        used as f64 / total as f64
    }

    /// Get an estimate of the remaining capacity in number of samples
    pub fn estimate_remaining_sample_capacity(&self) -> usize {
        let used = self.size();
        let total = self.max_size_in_bytes();
        if used >= total {
            return 0;
        }
        let remaining = total - used;
        let bytes_per_sample = self.bytes_per_sample();
        remaining / bytes_per_sample
    }

    pub fn clear(&mut self) {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.clear(),
            Gorilla(chunk) => chunk.clear(),
            Pco(chunk) => chunk.clear(),
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

    // pub fn process_range<F, State, R>(
    //     &self,
    //     state: &mut State,
    //     start: Timestamp,
    //     end: Timestamp,
    //     f: F,
    // ) -> TsdbResult<R>
    // where
    //     F: FnMut(&mut State, &[i64], &[f64]) -> TsdbResult<R>,
    // {
    //     use TimeSeriesChunk::*;
    //     match self {
    //         Uncompressed(chunk) => chunk.process_range(start, end, state, f),
    //         Gorilla(chunk) => chunk.process_range(start, end, state, f),
    //         Pco(chunk) => chunk.process_range(start, end, state, f),
    //     }
    // }

    // todo: make this a trait method
    pub fn iter(&self) -> Box<dyn Iterator<Item = Sample> + '_> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => Box::new(chunk.iter()),
            Gorilla( chunk) => Box::new(chunk.iter()),
            Pco(chunk) => Box::new(chunk.iter())
        }
    }

    pub fn range_iter<'a>(
        &'a self,
        start: Timestamp,
        end: Timestamp,
    ) -> Box<dyn Iterator<Item = Sample> + 'a> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => Box::new(chunk.range_iter(start, end)),
            Gorilla( chunk) => Box::new(chunk.range_iter(start, end)),
            Pco(chunk) => Box::new(chunk.range_iter(start, end)),
        }
    }

    pub fn get_samples(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        let mut samples = Vec::with_capacity(16);
        for sample in self.range_iter(start, end) {
            samples.push(sample);
        }
        Ok(samples)
    }

    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() || timestamps.is_empty() {
            return Ok(vec![]);
        }
        match self {
            TimeSeriesChunk::Uncompressed(chunk) => chunk.samples_by_timestamps(timestamps),
            TimeSeriesChunk::Gorilla(chunk) => chunk.samples_by_timestamps(timestamps),
            TimeSeriesChunk::Pco(chunk) => chunk.samples_by_timestamps(timestamps),
        }
    }

    pub fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.set_data(samples),
            Gorilla(chunk) => chunk.set_data(samples),
            Pco(chunk) => chunk.set_data(samples),
        }
    }

    pub fn merge(
        &mut self,
        other: &mut Self,
        retention_threshold: Timestamp,
        duplicate_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {
        self.merge_range(
            other,
            other.first_timestamp(),
            other.last_timestamp(),
            retention_threshold,
            duplicate_policy,
        )
    }

    /// Merge a range of samples from another chunk into this chunk.
    /// If the chunk is full or the other chunk is empty, returns 0.
    /// Duplicate values are handled according to `duplicate_policy`.
    /// Samples with timestamps before `retention_threshold` will be ignored, whether
    /// they fall with the given range [start_ts..end_ts].
    /// Returns the number of samples merged.
    pub fn merge_range(
        &mut self,
        other: &mut Self,
        start_ts: Timestamp,
        end_ts: Timestamp,
        retention_threshold: Timestamp,
        duplicate_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {
        if self.is_full() || other.is_empty() {
            return Ok(0);
        }

        let min_timestamp = retention_threshold.max(start_ts);
        let samples = other.get_samples(min_timestamp, end_ts)?;
        let mut duplicates = AHashSet::new();

        self.merge_samples(&samples, duplicate_policy, &mut duplicates)

    }

    // todo: move to trait method
    pub fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: DuplicatePolicy,
        blocked: &mut AHashSet<Timestamp>
    ) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => {
                chunk.merge_samples(samples, dp_policy, blocked)
            }
            Gorilla(chunk) => {
                chunk.merge_samples(samples, dp_policy, blocked)
            }
            Pco(chunk) => {
                chunk.merge_samples(samples, dp_policy, blocked)
            }
        }
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() +
            self.get_heap_size()
    }

}

impl Chunk for TimeSeriesChunk {
    fn first_timestamp(&self) -> Timestamp {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(uncompressed) => uncompressed.first_timestamp(),
            Gorilla(gorilla) => gorilla.first_timestamp(),
            Pco(compressed) => compressed.first_timestamp(),
        }
    }

    fn last_timestamp(&self) -> Timestamp {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.last_timestamp(),
            Gorilla(chunk) => chunk.last_timestamp(),
            Pco(chunk) => chunk.last_timestamp(),
        }
    }

    fn num_samples(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.num_samples(),
            Gorilla(chunk) => chunk.num_samples(),
            Pco(chunk) => chunk.num_samples(),
        }
    }

    fn last_value(&self) -> f64 {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.last_value(),
            Gorilla(chunk) => chunk.last_value(),
            Pco(chunk) => chunk.last_value(),
        }
    }

    fn size(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.size(),
            Gorilla(chunk) => chunk.size(),
            Pco(chunk) => chunk.size(),
        }
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.remove_range(start_ts, end_ts),
            Gorilla(chunk) => chunk.remove_range(start_ts, end_ts),
            Pco(chunk) => chunk.remove_range(start_ts, end_ts),
        }
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.add_sample(sample),
            Gorilla(chunk) => chunk.add_sample(sample),
            Pco(chunk) => chunk.add_sample(sample),
        }
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.get_range(start, end),
            Gorilla(chunk) => chunk.get_range(start, end),
            Pco(chunk) => chunk.get_range(start, end),
        }
    }

    fn upsert_sample(
        &mut self,
        sample: &mut Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.upsert_sample(sample, dp_policy),
            Gorilla(chunk) => chunk.upsert_sample(sample, dp_policy),
            Pco(chunk) => chunk.upsert_sample(sample, dp_policy),
        }
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => Ok(Uncompressed(chunk.split()?)),
            Gorilla(chunk) => Ok(Gorilla(chunk.split()?)),
            Pco(chunk) => Ok(Pco(chunk.split()?)),
        }
    }

    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => {
                raw::save_unsigned(rdb, ChunkCompression::Uncompressed as u8 as u64);
                chunk.rdb_save(rdb);
            },
            Gorilla(chunk) => {
                raw::save_unsigned(rdb, ChunkCompression::Gorilla as u8 as u64);
                chunk.rdb_save(rdb)
            },
            Pco(chunk) => {
                raw::save_unsigned(rdb, ChunkCompression::Pco as u8 as u64);
                chunk.rdb_save(rdb)
            },
        }
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> Result<Self, Error> {
        let compression = ChunkCompression::try_from(raw::load_unsigned(rdb)? as u8)
            .map_err(|_e| Error::Generic(GenericError::new("Error loading chunk compression marker")))?;

        let chunk = match compression {
            ChunkCompression::Uncompressed => {
                TimeSeriesChunk::Uncompressed(UncompressedChunk::rdb_load(rdb)?)
            }
            ChunkCompression::Gorilla => {
                TimeSeriesChunk::Gorilla(GorillaChunk::rdb_load(rdb)?)
            }
            ChunkCompression::Pco => {
                TimeSeriesChunk::Pco(PcoChunk::rdb_load(rdb)?)
            }
        };
        Ok(chunk)
    }
}

pub(crate) struct ChunkSampleIterator<'a> {
    inner: Box<dyn Iterator<Item = Sample> + 'a>,
    start: Timestamp,
    end: Timestamp,
    is_init: bool,
}

impl<'a> ChunkSampleIterator<'a> {
    pub fn new(chunk: &'a TimeSeriesChunk,
           start: Timestamp,
           end: Timestamp,
           value_filter: &'a Option<ValueFilter>,
           ts_filter: &'a Option<Vec<Timestamp>>,
    ) -> Self {
        let iter = if chunk.overlaps(start, end) {
            Self::get_inner_iter(chunk, start, end, ts_filter, value_filter)
        } else {
            // use an empty iterator
            Box::new(std::iter::empty::<Sample>())
        };

        Self {
            inner: Box::new(iter),
            start,
            end,
            is_init: false,
        }
    }

    fn get_inner_iter(chunk: &'a TimeSeriesChunk,
                      start: Timestamp,
                      end: Timestamp,
                      ts_filter: &'a Option<Vec<Timestamp>>,
                      value_filter: &'a Option<ValueFilter>) -> Box<dyn Iterator<Item = Sample> + 'a> {
        match (ts_filter, value_filter) {
            (Some(timestamps), Some(value_filter)) => {
                Box::new(chunk.samples_by_timestamps(timestamps)
                    .unwrap_or_else(|_e| {
                        // todo: properly handle error and log
                        vec![]
                    })
                    .into_iter()
                    .filter(move |sample| sample.value >= value_filter.min && sample.value <= value_filter.max)
                )
            }
            (Some(timestamps), None) => {
                Box::new(chunk.samples_by_timestamps(timestamps)
                    .unwrap_or_else(|_e| {
                        // todo: properly handle error and log
                        vec![]
                    }).into_iter()
                )
            }
            (None, Some(filter)) => {
                Box::new(
                    chunk.range_iter(start, end)
                        .filter(|sample| sample.value >= filter.min && sample.value <= filter.max)
                )
            }
            _ => {
                Box::new(chunk.range_iter(start, end))
            }
        }
    }
}

// todo: implement next_chunk
impl<'a> Iterator for ChunkSampleIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_init {
            self.is_init = true;
            for sample in self.inner.by_ref() {
                if sample.timestamp > self.end {
                    return None;
                }
                if sample.timestamp < self.start {
                    continue;
                }
                return Some(sample);
            }
        }
        if let Some(sample) = self.inner.by_ref().next() {
            if sample.timestamp > self.end {
                return None;
            }
            return Some(sample);
        }
        None
    }
}

pub(crate) fn validate_chunk_size(chunk_size_bytes: usize) -> TsdbResult<()> {
    fn get_error_result() -> TsdbResult<()> {
        let msg = format!("TSDB: CHUNK_SIZE value must be a multiple of 2 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]");
        Err(TsdbError::InvalidConfiguration(msg))
    }

    if chunk_size_bytes < MIN_CHUNK_SIZE {
        return get_error_result();
    }

    if chunk_size_bytes > MAX_CHUNK_SIZE {
        return get_error_result();
    }

    if chunk_size_bytes % 2 != 0 {
        return get_error_result();
    }

    Ok(())
}

pub fn merge_by_capacity(
    dest: &mut TimeSeriesChunk,
    src: &mut TimeSeriesChunk,
    min_timestamp: Timestamp,
    duplicate_policy: DuplicatePolicy,
) -> TsdbResult<Option<usize>> {
    if src.is_empty() {
        return Ok(None);
    }

    // check if previous block has capacity, and if so merge into it
    let count = src.num_samples();
    let remaining_capacity = dest.estimate_remaining_sample_capacity();
    // if there is enough capacity in the previous block, merge the last block into it,
    // but only if there's sufficient space to justify the overhead of compression
    if remaining_capacity >= count {
        // copy all from last_chunk
        let res = dest.merge(src, min_timestamp, duplicate_policy)?;
        // reuse last block
        src.clear();
        return Ok(Some(res));
    } else if remaining_capacity > count / 4 {
        // do a partial merge
        let samples = src.get_range(src.first_timestamp(), src.last_timestamp())?;
        let (left, right) = samples.split_at(remaining_capacity);
        let mut duplicates = AHashSet::new();
        let res = dest.merge_samples(
            left,
            duplicate_policy,
            &mut duplicates,
        )?;
        src.clear();
        src.merge_samples(right, duplicate_policy, &mut duplicates)?;
        return Ok(Some(res));
    }
    Ok(None)
}


#[cfg(test)]
mod tests {
}