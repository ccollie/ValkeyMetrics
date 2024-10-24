use crate::common::types::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::iter::SampleIter;
use crate::series::types::ValueFilter;
use crate::series::utils::{filter_samples_by_date_range, filter_samples_by_value};
use crate::series::{Chunk, ChunkCompression, DuplicatePolicy, GorillaChunk, PcoChunk, UncompressedChunk, SPLIT_FACTOR};
use core::mem::size_of;
use get_size::GetSize;
use valkey_module::error::{Error, GenericError};
use valkey_module::RedisModuleIO;

#[derive(Debug, Clone, PartialEq)]
#[derive(GetSize)]
pub enum TimeSeriesChunk {
    Uncompressed(UncompressedChunk),
    Gorilla(GorillaChunk),
    Pco(PcoChunk),
}

impl TimeSeriesChunk {
    pub fn new(compression: ChunkCompression, chunk_size: usize) -> Self {
        use TimeSeriesChunk::*;
        use crate::series::{GorillaChunk, PcoChunk, UncompressedChunk};
        match compression {
            ChunkCompression::Uncompressed => {
                let chunk = UncompressedChunk::with_max_size(chunk_size);
                Uncompressed(chunk)
            }
            ChunkCompression::Gorilla => {
                let chunk = GorillaChunk::with_max_size(chunk_size);
                Gorilla(chunk)
            }
            ChunkCompression::Pco => {
                Pco(PcoChunk::with_max_size(chunk_size))
            }
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
        let total = self.max_size();
        used as f64 / total as f64
    }

    /// Get an estimate of the remaining capacity in number of samples
    pub fn estimate_remaining_sample_capacity(&self) -> usize {
        let used = self.size();
        let total = self.max_size();
        if used >= total {
            return 0;
        }
        let remaining = total - used;
        let bytes_per_sample = self.bytes_per_sample();
        if bytes_per_sample == 0 {
            return 0;
        }
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

    // todo: make this a trait method
    pub fn iter(&self) -> Box<dyn Iterator<Item = Sample> + '_> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => Box::new(chunk.iter()),
            Gorilla( chunk) => Box::new(chunk.iter()),
            Pco(chunk) => Box::new(chunk.iter())
        }
    }

    pub fn range_iter(
        &self,
        start: Timestamp,
        end: Timestamp,
    ) -> SampleIter {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.range_iter(start, end),
            Gorilla( chunk) => chunk.range_iter(start, end),
            Pco(chunk) => chunk.range_iter(start, end),
        }
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

    pub(crate) fn get_range_filtered(
        &self,
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
        timestamp_filter: &Option<Vec<Timestamp>>,
        value_filter: &Option<ValueFilter>
    ) -> Vec<Sample> {

        fn get_by_range(chunk: &TimeSeriesChunk, start_timestamp: Timestamp, end_timestamp: Timestamp) -> Vec<Sample> {
            // todo: raise error
            chunk.get_range(start_timestamp, end_timestamp)
                .unwrap_or_default()
                .into_iter()
                .collect()
        }

        match (timestamp_filter, value_filter) {
            (Some(ts_filter), Some(value_filter)) => {
                let mut samples = self.samples_by_timestamps(ts_filter)
                    .unwrap_or_default()
                    .into_iter()
                    .collect();

                filter_samples_by_date_range(&mut samples, start_timestamp, end_timestamp);
                filter_samples_by_value(&mut samples, value_filter);
                samples
            }
            (None, Some(value_filter)) => {
                let mut samples = get_by_range(self, start_timestamp, end_timestamp);
                filter_samples_by_value(&mut samples, value_filter);
                samples
            }
            (Some(ts_filter), None) => {
                let mut samples = self.samples_by_timestamps(ts_filter)
                    .unwrap_or_default()
                    .into_iter()
                    .collect();

                filter_samples_by_date_range(&mut samples, start_timestamp, end_timestamp);
                samples
            }
            (None, None) => {
                get_by_range(self, start_timestamp, end_timestamp)
            }
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
        duplicate_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<usize> {
        let min_timestamp = retention_threshold.max(other.first_timestamp());
        self.merge_range(
            other,
            min_timestamp,
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
        duplicate_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<usize> {
        if self.is_full() || other.is_empty() {
            return Ok(0);
        }

        let min_timestamp = retention_threshold.max(start_ts);
        let samples = other.get_range(min_timestamp, end_ts)?;
        self.merge_samples(&samples, duplicate_policy)

    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() +
            self.get_heap_size()
    }

    pub(crate) fn upsert(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<(usize, Option<TimeSeriesChunk>)> {
        if self.size() as f64 > self.max_size() as f64 * SPLIT_FACTOR {
            let mut new_chunk = self.split()?;
            let size = new_chunk.upsert_sample(sample, dp_policy)?;
            Ok((size, Some(new_chunk)))
        } else {
            let size = self.upsert_sample(sample, dp_policy)?;
            Ok((size, None))
        }
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

    fn len(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.len(),
            Gorilla(chunk) => chunk.len(),
            Pco(chunk) => chunk.len(),
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

    fn max_size(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.max_size(),
            Gorilla(chunk) => chunk.max_size(),
            Pco(chunk) => chunk.max_size(),
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
        sample:  Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.upsert_sample(sample, dp_policy),
            Gorilla(chunk) => chunk.upsert_sample(sample, dp_policy),
            Pco(chunk) => chunk.upsert_sample(sample, dp_policy),
        }
    }

     fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;

        debug_assert!(!samples.is_empty());

        match self {
            Uncompressed(chunk) => {
                chunk.merge_samples(samples, dp_policy)
            }
            Gorilla(chunk) => {
                chunk.merge_samples(samples, dp_policy)
            }
            Pco(chunk) => {
                chunk.merge_samples(samples, dp_policy)
            }
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
                valkey_module::save_unsigned(rdb, ChunkCompression::Uncompressed as u8 as u64);
                chunk.rdb_save(rdb);
            },
            Gorilla(chunk) => {
                valkey_module::save_unsigned(rdb, ChunkCompression::Gorilla as u8 as u64);
                chunk.rdb_save(rdb)
            },
            Pco(chunk) => {
                valkey_module::save_unsigned(rdb, ChunkCompression::Pco as u8 as u64);
                chunk.rdb_save(rdb)
            },
        }
    }

    fn rdb_load(rdb: *mut RedisModuleIO, _encver: i32) -> Result<Self, Error> {
        let compression = ChunkCompression::try_from(valkey_module::load_unsigned(rdb)? as u8)
            .map_err(|_e| Error::Generic(GenericError::new("Error loading chunk compression marker")))?;

        let chunk = match compression {
            ChunkCompression::Uncompressed => {
                TimeSeriesChunk::Uncompressed(UncompressedChunk::rdb_load(rdb, _encver)?)
            }
            ChunkCompression::Gorilla => {
                TimeSeriesChunk::Gorilla(GorillaChunk::rdb_load(rdb, _encver)?)
            }
            ChunkCompression::Pco => {
                TimeSeriesChunk::Pco(PcoChunk::rdb_load(rdb, _encver)?)
            }
        };
        Ok(chunk)
    }
}
