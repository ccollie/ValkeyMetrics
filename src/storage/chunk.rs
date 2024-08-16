use crate::common::types::{PooledTimestampVec, PooledValuesVec, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::storage::compressed_chunk::CompressedChunk;
use crate::storage::merge::merge;
use crate::storage::uncompressed_chunk::UncompressedChunk;
use crate::storage::utils::get_timestamp_index;
use crate::storage::{DuplicatePolicy, Sample, SeriesSlice};
use ahash::AHashSet;
use metricsql_common::pool::{get_pooled_vec_f64, get_pooled_vec_i64};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use get_size::GetSize;

pub const MIN_CHUNK_SIZE: usize = 48;
pub const MAX_CHUNK_SIZE: usize = 1048576;

#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[derive(GetSize)]
#[non_exhaustive]
pub enum ChunkCompression {
    Uncompressed = 0,
    #[default]
    Compressed = 1,
}

impl ChunkCompression {
    pub fn is_compressed(&self) -> bool {
        matches!(self, ChunkCompression::Compressed)
    }

    pub fn is_uncompressed(&self) -> bool {
        matches!(self, ChunkCompression::Uncompressed)
    }

    pub fn name(&self) -> &'static str {
        match self {
            ChunkCompression::Uncompressed => "uncompressed",
            ChunkCompression::Compressed => "compressed",
        }
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
            0 => Ok(ChunkCompression::Uncompressed),
            1 => Ok(ChunkCompression::Compressed),
            _ => Err(TsdbError::InvalidCompression(value.to_string())),
        }
    }
}

impl TryFrom<&str> for ChunkCompression {
    type Error = TsdbError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            s if s.eq_ignore_ascii_case("uncompressed") => Ok(ChunkCompression::Uncompressed),
            s if s.eq_ignore_ascii_case("compressed") => Ok(ChunkCompression::Compressed),
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
        timestamps: &mut Vec<i64>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()>;

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
#[derive(GetSize)]
pub enum TimeSeriesChunk {
    Uncompressed(UncompressedChunk),
    Compressed(CompressedChunk),
}

impl TimeSeriesChunk {
    pub fn new(
        compression: ChunkCompression,
        chunk_size: usize,
        timestamps: &[Timestamp],
        values: &[f64],
    ) -> TsdbResult<Self> {
        use TimeSeriesChunk::*;
        match compression {
            ChunkCompression::Uncompressed => {
                let chunk =
                    UncompressedChunk::new(chunk_size, timestamps.to_vec(), values.to_vec());
                Ok(Uncompressed(chunk))
            }
            ChunkCompression::Compressed => {
                let chunk = CompressedChunk::with_values(chunk_size, timestamps, values)?;
                Ok(Compressed(chunk))
            }
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

    pub fn max_size_in_bytes(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.max_size,
            Compressed(chunk) => chunk.max_size,
        }
    }

    pub fn is_full(&self) -> bool {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.is_full(),
            Compressed(chunk) => chunk.is_full(),
        }
    }

    pub fn bytes_per_sample(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.bytes_per_sample(),
            Compressed(chunk) => chunk.bytes_per_sample(),
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

    pub fn process_range<F, State, R>(
        &self,
        state: &mut State,
        start: Timestamp,
        end: Timestamp,
        f: F,
    ) -> TsdbResult<R>
    where
        F: FnMut(&mut State, &[i64], &[f64]) -> TsdbResult<R>,
    {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.process_range(start, end, state, f),
            Compressed(chunk) => chunk.process_range(start, end, state, f),
        }
    }

    pub fn iter_range(
        &self,
        start: Timestamp,
        end: Timestamp,
    ) -> impl IntoIterator<Item = Sample> + '_ {
        SampleIterator::new(self, start, end)
    }

    pub fn get_samples(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        let mut samples = Vec::new();
        self.process_range(&mut samples, start, end, |samples, timestamps, values| {
            samples.reserve(timestamps.len());
            for i in 0..timestamps.len() {
                samples.push(Sample::new(timestamps[i], values[i]));
            }
            Ok(())
        })?;
        Ok(samples)
    }

    pub fn set_data(&mut self, timestamps: &[i64], values: &[f64]) -> TsdbResult<()> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.set_data(timestamps, values),
            Compressed(chunk) => chunk.set_data(timestamps, values),
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
    /// If the other chunk is compressed, it will be decompressed first.
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

        let min_timestamp = retention_threshold;
        let mut timestamps = get_pooled_vec_i64(other.num_samples());
        let mut values = get_pooled_vec_f64(self.num_samples());
        other.get_range(min_timestamp.max(start_ts), end_ts, &mut timestamps, &mut values)?;

        let mut duplicates = AHashSet::new();
        let slice = SeriesSlice::new(timestamps.as_slice(), values.as_slice());

        self.merge_slice(slice, retention_threshold, duplicate_policy,  &mut duplicates)
    }

    pub fn merge_slice(
        &mut self,
        samples: SeriesSlice,
        min_timestamp: Timestamp,
        duplicate_policy: DuplicatePolicy,
        duplicates: &mut AHashSet<Timestamp>,
    ) -> TsdbResult<usize> {
        if samples.is_empty() {
            return Ok(0);
        }
        let (res, timestamps, values) =
            self.merge_slice_internal(samples, min_timestamp, duplicate_policy, duplicates)?;

        self.set_data(timestamps.as_slice(), values.as_slice())?;

        Ok(res)
    }

    fn merge_slice_internal(
        &self,
        samples: SeriesSlice,
        min_timestamp: Timestamp,
        duplicate_policy: DuplicatePolicy,
        duplicates: &mut AHashSet<Timestamp>,
    ) -> TsdbResult<(usize, PooledTimestampVec, PooledValuesVec)> {
        struct State<'a> {
            timestamps: PooledTimestampVec,
            values: PooledValuesVec,
            other: &'a SeriesSlice<'a>,
            duplicates: &'a mut AHashSet<Timestamp>,
        }

        let resolve_len = samples.len() + self.num_samples();

        let mut state = State {
            timestamps: get_pooled_vec_i64(resolve_len),
            values: get_pooled_vec_f64(resolve_len),
            other: &samples,
            duplicates,
        };

        let res = self.process_range(
            &mut state,
            min_timestamp,
            self.last_timestamp(),
            |state, timestamps, values| {
                let slice = SeriesSlice::new(timestamps, values);
                let other = state.other.clone();
                let res = merge(
                    &mut state.timestamps,
                    &mut state.values,
                    slice,
                    other,
                    min_timestamp,
                    duplicate_policy,
                    state.duplicates,
                );
                Ok(res)
            },
        )?;

        Ok((res, state.timestamps, state.values))
    }

    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() +
            self.get_heap_size()
    }
}

impl Chunk for TimeSeriesChunk {
    fn first_timestamp(&self) -> Timestamp {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(uncompressed) => uncompressed.first_timestamp(),
            Compressed(chunk) => chunk.first_timestamp(),
        }
    }

    fn last_timestamp(&self) -> Timestamp {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.last_timestamp(),
            Compressed(chunk) => chunk.last_timestamp(),
        }
    }

    fn num_samples(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.num_samples(),
            Compressed(chunk) => chunk.num_samples(),
        }
    }

    fn last_value(&self) -> f64 {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.last_value(),
            Compressed(chunk) => chunk.last_value(),
        }
    }

    fn size(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.size(),
            Compressed(chunk) => chunk.size(),
        }
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.remove_range(start_ts, end_ts),
            Compressed(chunk) => chunk.remove_range(start_ts, end_ts),
        }
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.add_sample(sample),
            Compressed(chunk) => chunk.add_sample(sample),
        }
    }

    fn get_range(
        &self,
        start: Timestamp,
        end: Timestamp,
        timestamps: &mut Vec<i64>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.get_range(start, end, timestamps, values),
            Compressed(chunk) => chunk.get_range(start, end, timestamps, values),
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
            Compressed(chunk) => chunk.upsert_sample(sample, dp_policy),
        }
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => Ok(Uncompressed(chunk.split()?)),
            Compressed(chunk) => Ok(Compressed(chunk.split()?)),
        }
    }
}

struct SampleIterator<'a> {
    chunk: &'a TimeSeriesChunk,
    timestamps: PooledTimestampVec,
    values: PooledValuesVec,
    sample_index: usize,
    start: Timestamp,
    end: Timestamp,
    is_init: bool,
}

impl<'a> SampleIterator<'a> {
    fn new(chunk: &'a TimeSeriesChunk, start: Timestamp, end: Timestamp) -> Self {
        let capacity = chunk.num_samples();
        let timestamps = get_pooled_vec_i64(capacity);
        let values = get_pooled_vec_f64(capacity);

        Self {
            chunk,
            timestamps,
            values,
            sample_index: 0,
            start,
            end,
            is_init: false,
        }
    }
}

// todo: implement next_chunk
impl<'a> Iterator for SampleIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_init {
            self.is_init = true;
            let first = self.chunk.first_timestamp();
            if first > self.end {
                return None;
            }
            if self.chunk
                .get_range(self.start, self.end, &mut self.timestamps, &mut self.values).is_err()
            {
                return None;
            }
            if let Some(idx) = get_timestamp_index(&self.timestamps, self.start) {
                self.sample_index = idx;
            } else {
                return None;
            }
        }
        if self.sample_index >= self.timestamps.len() {
            return None;
        }
        let timestamp = self.timestamps[self.sample_index];
        let value = self.values[self.sample_index];
        self.sample_index += 1;
        Some(Sample::new(timestamp, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let count = self.timestamps.len();
        let lower = count - self.sample_index;
        (lower, Some(count))
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
        let mut timestamps = get_pooled_vec_i64(count);
        let mut values = get_pooled_vec_f64(count);
        src.get_range(
            src.first_timestamp(),
            src.last_timestamp(),
            &mut timestamps,
            &mut values,
        )?;
        let slice = SeriesSlice::new(&timestamps, &values);
        let (left, right) = slice.split_at(remaining_capacity);
        let mut duplicates = AHashSet::new();
        let res = dest.merge_slice(
            left,
            min_timestamp,
            duplicate_policy,
            &mut duplicates,
        )?;
        src.set_data(right.timestamps, right.values)?;
        return Ok(Some(res));
    }
    Ok(None)
}


#[cfg(test)]
mod tests {
    use rand::Rng;
    use crate::error::TsdbError;
    use crate::tests::generators::create_rng;
    use crate::storage::{Chunk, Sample, TimeSeriesChunk};

    pub fn saturate_chunk(chunk: &mut TimeSeriesChunk) {
        let mut rng = create_rng(None).unwrap();
        let mut ts: i64 = 10000;
        loop {
            let sample = Sample {
                timestamp: ts,
                value: rng.gen_range(0.0..100.0),
            };
            ts += rng.gen_range(1000..20000);
            match chunk.add_sample(&sample) {
                Ok(_) => {}
                Err(TsdbError::CapacityFull(_)) => {
                    break
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        }
    }
}