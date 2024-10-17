use std::collections::BTreeSet;
use super::pco_utils::{encode_with_options, pco_decode, pco_encode, CompressorConfig};
use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::series::chunks::Chunk;
use crate::series::serialization::{rdb_load_usize, rdb_save_usize};
use crate::series::{DuplicatePolicy, Sample, DEFAULT_CHUNK_SIZE_BYTES, VEC_BASE_SIZE};
use ahash::AHashSet;
use get_size::GetSize;
use metricsql_common::pool::{get_pooled_vec_f64, get_pooled_vec_i64, PooledVecF64, PooledVecI64};
use pco::DEFAULT_COMPRESSION_LEVEL;
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use valkey_module::raw;
use crate::iter::SampleIter;
use crate::series::utils::trim_to_range_inclusive;

/// items above this count will cause value and timestamp encoding/decoding to happen in parallel
pub(in crate::series) const COMPRESSION_PARALLELIZATION_THRESHOLD: usize = 1024;

/// `CompressedBlock` holds information about location and time range of a block of compressed data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[derive(GetSize)]
pub struct PcoChunk {
    pub min_time: Timestamp,
    pub max_time: Timestamp,
    pub max_size: usize,
    pub last_value: f64,
    /// number of compressed samples
    pub count: usize,
    pub timestamps: Vec<u8>,
    pub values: Vec<u8>,
}

impl Default for PcoChunk {
    fn default() -> Self {
        Self {
            min_time: 0,
            max_time: i64::MAX,
            max_size: DEFAULT_CHUNK_SIZE_BYTES,
            last_value: 0.0,
            count: 0,
            timestamps: Vec::new(),
            values: Vec::new(),
        }
    }
}

impl PcoChunk {
    pub fn with_max_size(max_size: usize) -> Self {
        let mut res = Self::default();
        res.max_size = max_size;
        res
    }

    pub fn with_values(max_size: usize, samples: &[Sample]) -> TsdbResult<Self> {
        debug_assert!(!samples.is_empty());
        let mut res = Self::default();
        res.max_size = max_size;

        let count = samples.len();
        if count > 0 {
            let mut timestamps = get_pooled_vec_i64(count);
            let mut values = get_pooled_vec_f64(count);
            for sample in samples {
                timestamps.push(sample.timestamp);
                values.push(sample.value);
            }

            res.compress(&timestamps, &values)?;
        }

        Ok(res)
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn is_full(&self) -> bool {
        self.data_size() >= self.max_size
    }

    pub fn clear(&mut self) {
        self.count = 0;
        self.timestamps.clear();
        self.values.clear();
        self.min_time = 0;
        self.max_time = 0;
        self.last_value = f64::NAN; // todo - use option instead
    }

    pub fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        if samples.is_empty() {
            self.clear();
            return Ok(());
        }
        let mut timestamps = get_pooled_vec_i64(samples.len());
        let mut values = get_pooled_vec_f64(samples.len());

        for sample in samples {
            timestamps.push(sample.timestamp);
            values.push(sample.value);
        }

        self.compress(&timestamps, &values)?;
        // todo: complain if size > max_size
        Ok(())
    }

    fn compress(&mut self, timestamps: &[Timestamp], values: &[f64]) -> TsdbResult<()> {
        if timestamps.is_empty() {
            self.clear();
            return Ok(());
        }
        debug_assert_eq!(timestamps.len(), values.len());
        // todo: validate range
        self.min_time = timestamps[0];
        self.max_time = timestamps[timestamps.len() - 1];
        self.count = timestamps.len();
        self.last_value = values[values.len() - 1];
        if timestamps.len() > COMPRESSION_PARALLELIZATION_THRESHOLD {
            // use rayon to run compression in parallel
            // first we steal the result buffers to avoid allocation and issues with the BC
            let mut t_data = std::mem::take(&mut self.timestamps);
            let mut v_data = std::mem::take(&mut self.values);

            t_data.clear();
            v_data.clear();

            // then we compress in parallel
            let _ = rayon::join(
                || compress_timestamps(&mut t_data, timestamps).ok(),
                || compress_values(&mut v_data, values).ok(),
            );
            // then we put the buffers back
            self.timestamps = t_data;
            self.values = v_data;
        } else {
            self.timestamps.clear();
            self.values.clear();
            compress_timestamps(&mut self.timestamps, timestamps)?;
            compress_values(&mut self.values, values)?;
        }
        self.count = timestamps.len();
        self.timestamps.shrink_to_fit();
        self.values.shrink_to_fit();
        Ok(())
    }

    fn decompress(&self) -> TsdbResult<Option<(PooledVecI64, PooledVecF64)>> {
        if self.is_empty() {
            return Ok(None);
        }
        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values = get_pooled_vec_f64(self.count);

        self.decompress_internal(&mut timestamps, &mut values)?;
        Ok(Some((timestamps, values)))
    }

    fn decompress_internal(&self, timestamps: &mut Vec<Timestamp>, values: &mut Vec<f64>) -> TsdbResult<()> {
        timestamps.reserve(self.count);
        values.reserve(self.count);
        // todo: dynamically calculate cutoff or just use chili
        if self.values.len() > 2048 {
            // todo: return errors as appropriate
            let _ = rayon::join(
                || decompress_timestamps(&self.timestamps, timestamps).ok(),
                || decompress_values(&self.values, values).ok(),
            );
        } else {
            decompress_timestamps(&self.timestamps, timestamps)?;
            decompress_values(&self.values, values)?
        }
        Ok(())
    }

    #[cfg(test)]
    fn decompress_samples(&self) -> TsdbResult<Vec<Sample>> {
        if let Some((timestamps, values)) = self.decompress()? {
            let samples = timestamps.iter().zip(values.iter())
                .map(|(ts, value)| Sample { timestamp: *ts, value: *value })
                .collect();
            Ok(samples)
        } else {
            Ok(vec![])
        }
    }

    pub fn timestamp_compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = self.timestamps.len() as f64;
        let uncompressed_size = (self.count * size_of::<i64>()) as f64;
        uncompressed_size / compressed_size
    }

    pub fn value_compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = self.values.len() as f64;
        let uncompressed_size = (self.count * size_of::<f64>()) as f64;
        uncompressed_size / compressed_size
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = (self.timestamps.len() + self.values.len()) as f64;
        let uncompressed_size = (self.count * size_of::<Sample>()) as f64;
        uncompressed_size / compressed_size
    }

    pub(crate) fn process_range<F, State, R>(
        &self,
        start: Timestamp,
        end: Timestamp,
        state: &mut State,
        mut f: F,
    ) -> TsdbResult<R>
    where
        F: FnMut(&mut State, &[i64], &[f64]) -> TsdbResult<R>,
    {
        let mut handle_empty_range = |state: &mut State| -> TsdbResult<R> {
            let mut timestamps = vec![];
            let mut values = vec![];
            f(state, &mut timestamps, &mut values)
        };

        // special case of range exceeding block range
        if self.is_empty() || start > self.max_time || end < self.min_time {
            return handle_empty_range(state);
        }

        if let Some((mut timestamps, mut values)) = self.decompress()? {
            trim_to_range_inclusive(&mut timestamps, &mut values, start, end);
            f(state, &timestamps, &values)
        } else {
            handle_empty_range(state)
        }
    }

    pub fn data_size(&self) -> usize {
        self.timestamps.get_heap_size() +
            self.values.get_heap_size() +
            2 * VEC_BASE_SIZE
    }

    pub fn bytes_per_sample(&self) -> usize {
        if self.count == 0 {
            return 0;
        }
        self.data_size() / self.count
    }

    /// estimate remaining capacity based on the current data size and chunk max_size
    pub fn remaining_capacity(&self) -> usize {
        self.max_size - self.data_size()
    }

    /// Estimate the number of samples that can be stored in the remaining capacity
    /// Note that for low sample counts this will be very inaccurate
    pub fn remaining_samples(&self) -> usize {
        if self.count == 0 {
            return 0;
        }
        self.remaining_capacity() / self.bytes_per_sample()
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.get_heap_size()
    }

    pub fn iter(&self) -> impl Iterator<Item = Sample> + '_ {
        PcoChunkIterator::new(self)
    }

    pub fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> SampleIter {
        PcoChunkIterator::new_range(self, start_ts, end_ts).into()
    }

    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>>  {
        if self.num_samples() == 0 || timestamps.is_empty() {
            return Ok(vec![]);
        }
        let mut state = timestamps;
        let last_timestamp = timestamps[timestamps.len() - 1] - 1i64;
        let first_timestamp = timestamps[0];

        self.process_range(first_timestamp, last_timestamp, &mut state, |state, timestamps, values| {
            let mut samples = Vec::with_capacity(timestamps.len());
            for ts in state.iter() {
                if let Ok(i) = timestamps.binary_search(ts) {
                    samples.push(Sample {
                        timestamp: timestamps[i],
                        value: values[i],
                    })
                }
            }
            Ok(samples)
        })
    }


}


impl Chunk for PcoChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.min_time
    }
    fn last_timestamp(&self) -> Timestamp {
        self.max_time
    }
    fn num_samples(&self) -> usize {
        self.count
    }
    fn last_value(&self) -> f64 {
        self.last_value
    }
    fn size(&self) -> usize {
        self.data_size()
    }
    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        if self.is_empty() {
            return Ok(0);
        }
        if start_ts > self.max_time || end_ts < self.min_time {
            return Ok(0);
        }
        if let Some((mut timestamps, mut values)) = self.decompress()? {
            let count = timestamps.len();
            remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

            let removed_count = count - timestamps.len();
            if timestamps.is_empty() {
                self.clear();
                Ok(removed_count)
            } else {
                self.compress(&timestamps, &values)?;
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_full() {
            return Err(TsdbError::CapacityFull(self.max_size));
        }

        if let Some((mut timestamps, mut values)) = self.decompress()? {
            timestamps.push(sample.timestamp);
            values.push(sample.value);
            self.compress(&timestamps, &values)
        } else {
            let timestamps = vec![sample.timestamp];
            let values = vec![sample.value];
            self.compress(&timestamps, &values)
        }
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        if let Some((mut timestamps, mut values)) = self.decompress()? {
            trim_to_range_inclusive(&mut timestamps, &mut values, start, end);

            Ok(timestamps.iter()
                .zip(values.iter())
                .filter_map(|(timestamp, value)| {
                    if *timestamp >= start && *timestamp <= end {
                        Some(Sample { timestamp: *timestamp, value: *value })
                    } else {
                        None
                    }
                })
                .collect())
        } else {
            Ok(vec![])
        }
    }

    fn upsert_sample(
        &mut self,
        sample: Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {

        // we don't do streaming compression, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old one
        if let Some((mut timestamps, mut values)) = self.decompress()? {

            let ts = sample.timestamp;
            let (pos, duplicate_found) = get_timestamp_index(&timestamps, ts, 0);

            if duplicate_found {
                values[pos] = dp_policy.duplicate_value(ts, values[pos], sample.value)?;
            } else {
                if pos == timestamps.len() {
                    timestamps.push(ts);
                    values.push(sample.value);
                } else {
                    timestamps.insert(pos, ts);
                    values.insert(pos, sample.value);
                };
            }

            self.compress(&timestamps, &values)?;
            Ok(timestamps.len())
        } else {
            self.add_sample(&sample)?;
            Ok(1)
        }
    }

     fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: DuplicatePolicy,
        blocked: &mut BTreeSet<Timestamp>
    ) -> TsdbResult<usize> {
        if samples.len() == 1 {
            let first = samples[0];
            if self.is_empty() {
                self.add_sample(&first)?;
            } else {
                self.upsert_sample(first, dp_policy)?;
            }
            return Ok(self.count)
        }

        if self.is_empty() {
            // we don't do streaming compression, so we have to accumulate all the samples
            // in a new chunk and then swap it with the old one
            let mut timestamps = get_pooled_vec_i64(self.count);
            let mut values = get_pooled_vec_f64(self.count);

            for sample in samples {
                timestamps.push(sample.timestamp);
                values.push(sample.value);
            }

            self.compress(&timestamps, &values)?;
            return Ok(timestamps.len())
        }

        if let Some((mut timestamps, mut values)) = self.decompress()? {
            let mut start_pos = 0;
            for sample in samples {
                let ts = sample.timestamp;
                let (pos, found) = get_timestamp_index(&timestamps, ts, start_pos);
                start_pos = pos + 1;
                if found {
                    if let Ok(val) = dp_policy.duplicate_value(ts, values[pos], sample.value) {
                        values[pos] = val;
                    } else {
                        blocked.insert(ts);
                    }
                } else {
                    timestamps.insert(pos, sample.timestamp);
                    values.insert(pos, sample.value);
                }
            }

            self.compress(&timestamps, &values)?;
        }

        Ok(self.count)
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let mut result = Self::default();
        result.max_size = self.max_size;

        if self.is_empty() {
            return Ok(result);
        }

        let mid = self.num_samples() / 2;

        // this compression method does not do streaming compression, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old
        if let Some((timestamps, mut values)) = self.decompress()? {
            let (left_timestamps, right_timestamps)  = timestamps.split_at(mid);
            let (left_values, right_values) = values.split_at_mut(mid);

            self.compress(left_timestamps, left_values)?;

            result.compress(right_timestamps, right_values)?;
        }

        Ok(result)
    }

    fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        raw::save_signed(rdb, self.min_time);
        raw::save_signed(rdb, self.max_time);
        rdb_save_usize(rdb, self.max_size);
        raw::save_double(rdb, self.last_value);
        rdb_save_usize(rdb, self.count);
        raw::save_slice(rdb, &self.timestamps);
        raw::save_slice(rdb, &self.values);
    }

    fn rdb_load(rdb: *mut raw::RedisModuleIO, _encver: i32) -> Result<Self, valkey_module::error::Error> {
        let min_time = raw::load_signed(rdb)?;
        let max_time = raw::load_signed(rdb)?;
        let max_size = rdb_load_usize(rdb)?;
        let last_value = raw::load_double(rdb)?;
        let count = rdb_load_usize(rdb)?;
        let ts = raw::load_string_buffer(rdb)?;
        let vals = raw::load_string_buffer(rdb)?;
        let timestamps: Vec<u8> = Vec::from(ts.as_ref());
        let values: Vec<u8> = Vec::from(vals.as_ref());

        Ok(Self {
            min_time,
            max_time,
            max_size,
            last_value,
            count,
            timestamps,
            values,
        })
    }
}

fn get_timestamp_index(timestamps: &[Timestamp], ts: Timestamp, start_ofs: usize) -> (usize, bool) {
    let stamps = &timestamps[start_ofs..];
    if stamps.len() <= 16 {
        // If the vectors are small, perform a linear search.
        match stamps.iter().position(|x| *x >= ts){
            Some(pos) => (pos + start_ofs, false),
            None => (timestamps.len() - 1, true),
        }
    } else {
        match stamps.binary_search(&ts) {
            Ok(pos) => (pos + start_ofs, true),
            Err(idx) => (idx + start_ofs, false),
        }
    }
}

fn remove_values_in_range(
    timestamps: &mut Vec<Timestamp>,
    values: &mut Vec<f64>,
    start_ts: Timestamp,
    end_ts: Timestamp,
) {
    debug_assert_eq!(timestamps.len(), values.len(), "Timestamps and scores vectors must be of the same length");

    let (start_index, end_index) = if timestamps.len() <= 32 {
        // If the vectors are small, perform a linear search.
        let start_index = timestamps.iter()
            .position(|x| *x >= start_ts)
            .unwrap_or(0);

        let end_index = timestamps.iter()
            .rposition(|x| *x <= end_ts)
            .unwrap_or(timestamps.len());

        (start_index, end_index)
    } else {
        let slice = timestamps.as_slice();

        let start_index = slice.partition_point(|x| *x < start_ts);
        // had issue with partition_point not returning the correct index for end_ts
        let end_index = slice[start_index..].binary_search_by_key(&end_ts, |x| *x)
            .map(|index| index + start_index)
            .unwrap_or(slice.len() - 1);

        (start_index, end_index)
    };

    if start_index == end_index {
        let ts = timestamps.get(start_index).unwrap();
        if *ts >= start_ts  && *ts <= end_ts {
            timestamps.remove(start_index);
            values.remove(start_index);
        }
        return
    }

    timestamps.drain(start_index..=end_index);
    values.drain(start_index..=end_index);
}

fn compress_values(compressed: &mut Vec<u8>, values: &[f64]) -> TsdbResult<()> {
    if values.is_empty() {
        return Ok(());
    }
    pco_encode(values, compressed)
        .map_err(|e| TsdbError::CannotSerialize(format!("values: {}", e)))
}

fn decompress_values(compressed: &[u8], dst: &mut Vec<f64>) -> TsdbResult<()> {
    if compressed.is_empty() {
        return Ok(());
    }
    pco_decode(compressed, dst)
        .map_err(|e| TsdbError::CannotDeserialize(format!("values: {}", e)))
}

fn compress_timestamps(compressed: &mut Vec<u8>, timestamps: &[Timestamp]) -> TsdbResult<()> {
    if timestamps.is_empty() {
        return Ok(());
    }
    let config = CompressorConfig {
        compression_level: DEFAULT_COMPRESSION_LEVEL,
        delta_encoding_order: 2
    };
    encode_with_options(timestamps, compressed, config)
        .map_err(|e| TsdbError::CannotSerialize(format!("timestamps: {}", e)))
}

fn decompress_timestamps(compressed: &[u8], dst: &mut Vec<i64>) -> TsdbResult<()> {
    if compressed.is_empty() {
        return Ok(());
    }
    pco_decode(compressed, dst)
        .map_err(|e| TsdbError::CannotDeserialize(format!("timestamps: {}", e)))
}


pub struct PcoChunkIterator<'a> {
    chunk: &'a PcoChunk,
    timestamps: Vec<Timestamp>,
    values: Vec<f64>,
    idx: usize,
    start: Timestamp,
    end: Timestamp,
    is_init: bool,
}

impl<'a> PcoChunkIterator<'a> {
    fn new(chunk: &'a PcoChunk) -> Self {
        Self {
            chunk,
            timestamps: vec![],
            values: vec![],
            idx: 0,
            start: i64::MAX,
            end: i64::MIN,
            is_init: false,
        }
    }

    fn new_range(chunk: &'a PcoChunk, start_ts: Timestamp, end_ts: Timestamp) -> Self {
        let mut iter = PcoChunkIterator::new(chunk);
        iter.start = start_ts;
        iter.end = end_ts;
        iter
    }

    fn has_range(&self) -> bool {
        self.start < self.end
    }

    fn init(&mut self) {
        self.is_init = true;

        match self.chunk.decompress_internal(&mut self.timestamps, &mut self.values) {
            Ok(_) => {
                if self.has_range() {
                    trim_to_range_inclusive(&mut self.timestamps, &mut self.values, self.start, self.end);
                    // should we do this, or will the allocator cause jitter
                    self.timestamps.shrink_to_fit();
                    self.values.shrink_to_fit();
                }
            }
            Err(e) => {
                // todo: log
            }
        }
    }
}

impl<'a> Iterator for PcoChunkIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_init {
            self.init();
        }
        if self.idx >= self.timestamps.len() {
            return None;
        }
        let timestamp = self.timestamps[self.idx];
        let value = self.values[self.idx];
        self.idx += 1;
        Some(Sample { timestamp, value })
    }
}
#[cfg(test)]
mod tests {
    use crate::common::types::Timestamp;
    use crate::series::test_utils::generate_random_samples;

    use crate::error::TsdbError;
    use crate::series::chunks::pco::pco_chunk::remove_values_in_range;
    use crate::series::chunks::Chunk;
    use crate::series::chunks::PcoChunk;
    use crate::series::{DuplicatePolicy, Sample};

    fn decompress(chunk: &PcoChunk) -> Vec<Sample> {
        chunk.iter().collect()
    }

    fn saturate_chunk(chunk: &mut PcoChunk) {
        loop {
            let samples = generate_random_samples(0, 250);
            for sample in samples {
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

    fn compare_chunks(chunk1: &PcoChunk, chunk2: &PcoChunk) {
        assert_eq!(chunk1.min_time, chunk2.min_time, "min_time");
        assert_eq!(chunk1.max_time, chunk2.max_time);
        assert_eq!(chunk1.max_size, chunk2.max_size);
        assert_eq!(chunk1.last_value, chunk2.last_value);
        assert_eq!(chunk1.count, chunk2.count, "mismatched counts {} vs {}", chunk1.count, chunk2.count);
        assert_eq!(chunk1.timestamps, chunk2.timestamps);
        assert_eq!(chunk1.values, chunk2.values);
    }

    #[test]
    fn test_chunk_compress() {
        let mut chunk = PcoChunk::default();
        let data = generate_random_samples(0, 1000);

        chunk.set_data(&data).unwrap();
        assert_eq!(chunk.num_samples(), data.len());
        assert_eq!(chunk.min_time, data[0].timestamp);
        assert_eq!(chunk.max_time, data[data.len() - 1].timestamp);
        assert_eq!(chunk.last_value, data[data.len() - 1].value);
        assert!(chunk.timestamps.len() > 0);
        assert!(chunk.values.len() > 0);
    }

    #[test]
    fn test_compress_decompress() {
        let mut chunk = PcoChunk::default();
        let data = generate_random_samples(0, 1000);
        chunk.set_data(&data).unwrap();
        let actual = chunk.decompress_samples().unwrap();
        assert_eq!(actual, data);
    }

    #[test]
    fn test_clear() {
        let mut chunk = PcoChunk::default();
        let data = generate_random_samples(0, 1000);

        chunk.set_data(&data).unwrap();
        assert_eq!(chunk.num_samples(), data.len());
        chunk.clear();
        assert_eq!(chunk.num_samples(), 0);
        assert_eq!(chunk.min_time, 0);
        assert_eq!(chunk.max_time, 0);
        assert!(chunk.last_value.is_nan());
        assert_eq!(chunk.timestamps.len(), 0);
        assert_eq!(chunk.values.len(), 0);
    }

    #[test]
    fn test_upsert() {
        for chunk_size in (64..8192).step_by(64) {
            let mut data = generate_random_samples(0, 500);
            let mut chunk = PcoChunk::with_max_size(chunk_size);

            let data_len = data.len();
            for sample in data.into_iter() {
                chunk.upsert_sample(sample, DuplicatePolicy::KeepLast).unwrap();
            }
            assert_eq!(chunk.num_samples(), data_len);
        }
    }

    #[test]
    fn test_upsert_while_at_capacity() {
        let mut chunk = PcoChunk::with_max_size(4096);
        saturate_chunk(&mut chunk);

        let timestamp = chunk.last_timestamp();

        // return an error on insert
        let mut sample = Sample {
            timestamp: 0,
            value: 1.0,
        };

        assert!(chunk.upsert_sample(sample, DuplicatePolicy::KeepLast).is_err());

        // should update value for duplicate timestamp
        sample.timestamp = timestamp;
        let res = chunk.upsert_sample(sample, DuplicatePolicy::KeepLast);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }

    #[test]
    fn test_split() {
        let mut chunk = PcoChunk::default();
        let data = generate_random_samples(0, 500);
        chunk.set_data(&data).unwrap();

        let count = data.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.num_samples(), mid);
        assert_eq!(right.num_samples(), mid);

        let (left_samples, right_samples) = data.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed, right_samples);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed, left_samples);
    }

    #[test]
    fn test_split_odd() {
        let mut chunk = PcoChunk::default();
        let samples = generate_random_samples(1, 51);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        let count = samples.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.num_samples(), mid);
        assert_eq!(right.num_samples(), mid + 1);

        let (left_samples, right_samples) = samples.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed, right_samples);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed, left_samples);
    }

    #[test]
    fn test_remove_values_in_range_single_timestamp() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 3;
        let end_ts = 3;

        remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![1, 2, 4, 5]);
        assert_eq!(values, vec![1.0, 2.0, 4.0, 5.0]);
    }

    #[test]
    fn test_remove_values_in_range_all_encompassing() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 0;
        let end_ts = 6;

        remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

        assert!(timestamps.is_empty(), "All timestamps should be removed");
        assert!(values.is_empty(), "All values should be removed");
    }

    #[test]
    fn test_remove_values_in_range_33_elements() {
        let mut timestamps: Vec<Timestamp> = (0..33).collect();
        let mut values: Vec<f64> = (0..33).map(|x| x as f64).collect();

        let start_ts = 10;
        let end_ts = 20;

        remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps.len(), 22);
        assert_eq!(values.len(), 22);

        for i in 0..10 {
            assert_eq!(timestamps[i], i as Timestamp);
            assert_eq!(values[i], i as f64);
        }

        for i in 10..22 {
            assert_eq!(timestamps[i], (i + 11) as Timestamp);
            assert_eq!(values[i], (i + 11) as f64);
        }
    }

    #[test]
    fn test_remove_values_in_range_start_equals_end() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 3;
        let end_ts = 3;

        remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![1, 2, 4, 5]);
        assert_eq!(values, vec![1.0, 2.0, 4.0, 5.0]);
    }

    #[test]
    fn test_remove_values_in_range_end_after_last_timestamp() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 3;
        let end_ts = 10;

        remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![1, 2]);
        assert_eq!(values, vec![1.0, 2.0]);
    }

    #[test]
fn test_range_iter_partial_overlap() {
    let samples = vec![
        Sample { timestamp: 100, value: 1.0 },
        Sample { timestamp: 200, value: 2.0 },
        Sample { timestamp: 300, value: 3.0 },
        Sample { timestamp: 400, value: 4.0 },
        Sample { timestamp: 500, value: 5.0 },
    ];
    let chunk = PcoChunk::with_values(1000, &samples).unwrap();

    let result: Vec<Sample> = chunk.range_iter(150, 450).collect();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], Sample { timestamp: 200, value: 2.0 });
    assert_eq!(result[1], Sample { timestamp: 300, value: 3.0 });
    assert_eq!(result[2], Sample { timestamp: 400, value: 4.0 });
}

    #[test]
    fn test_range_iter_exact_boundaries() {
        let mut chunk = PcoChunk::default();
        let samples = vec![
            Sample { timestamp: 100, value: 1.0 },
            Sample { timestamp: 200, value: 2.0 },
            Sample { timestamp: 300, value: 3.0 },
            Sample { timestamp: 400, value: 4.0 },
            Sample { timestamp: 500, value: 5.0 },
        ];
        chunk.set_data(&samples).unwrap();

        let result: Vec<Sample> = chunk.range_iter(200, 400).collect();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Sample { timestamp: 200, value: 2.0 });
        assert_eq!(result[1], Sample { timestamp: 300, value: 3.0 });
        assert_eq!(result[2], Sample { timestamp: 400, value: 4.0 });
    }

    #[test]
    fn test_range_iter_performance_on_large_chunks() {
        let mut chunk = PcoChunk::default();
        let num_samples = 1_000_000;
        let samples: Vec<Sample> = (0..num_samples)
            .map(|i| Sample { timestamp: i as i64, value: i as f64 })
            .collect();
        chunk.set_data(&samples).unwrap();

        let start_time = std::time::Instant::now();
        let range_samples: Vec<Sample> = chunk.range_iter(250_000, 750_000).collect();
        let duration = start_time.elapsed();

        assert_eq!(range_samples.len(), 500_001);
        assert!(duration.as_millis() < 1000, "Range iteration took too long: {:?}", duration);
    }

    #[test]
    fn test_range_iter_empty_chunk() {
        let chunk = PcoChunk::default();
        let start_ts = 0;
        let end_ts = 100;

        let samples: Vec<Sample> = chunk.range_iter(start_ts, end_ts).collect();

        assert!(samples.is_empty(), "Range iterator should return no samples for an empty chunk");
    }
}