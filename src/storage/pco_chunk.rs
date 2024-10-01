use get_size::GetSize;
use metricsql_common::pool::{get_pooled_vec_f64, get_pooled_vec_i64};
use serde::{Deserialize, Serialize};
use std::mem::size_of;

use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::storage::chunk::Chunk;
use crate::storage::utils::{get_timestamp_index_bounds, trim_vec_data};
use crate::storage::{DuplicatePolicy, Sample, SeriesSlice, DEFAULT_CHUNK_SIZE_BYTES, VEC_BASE_SIZE};
use metricsql_encoding::encoders::pco::{
    decode as pco_decode,
    encode as pco_encode,
    encode_with_options as pco_encode_with_options,
    CompressorConfig
};
use pco::DEFAULT_COMPRESSION_LEVEL;
use valkey_module::raw;

/// items above this count will cause value and timestamp encoding/decoding to happen in parallel
pub(super) const COMPRESSION_PARALLELIZATION_THRESHOLD: usize = 1024;

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

    pub fn with_values(
        max_size: usize,
        timestamps: &[Timestamp],
        values: &[f64],
    ) -> TsdbResult<Self> {
        debug_assert_eq!(timestamps.len(), values.len());
        let mut res = Self::default();
        res.max_size = max_size;
        res.compress(timestamps, values)?;
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

    pub fn set_data(&mut self, timestamps: &[i64], values: &[f64]) -> TsdbResult<()> {
        debug_assert_eq!(timestamps.len(), values.len());
        self.compress(timestamps, values)?;
        // todo: complain if size > max_size
        Ok(())
    }

    pub(super) fn compress(&mut self, timestamps: &[Timestamp], values: &[f64]) -> TsdbResult<()> {
        if timestamps.is_empty() {
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

        self.timestamps.shrink_to_fit();
        self.values.shrink_to_fit();
        Ok(())
    }

    pub(super) fn decompress(
        &self,
        timestamps: &mut Vec<Timestamp>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        if self.is_empty() {
            return Ok(());
        }
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
        let sample_size = size_of::<i64>() + size_of::<f64>();
        let uncompressed_size = (self.count * sample_size) as f64;
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
        let mut handle_empty = |state: &mut State| -> TsdbResult<R> {
            let mut timestamps = vec![];
            let mut values = vec![];
            f(state, &mut timestamps, &mut values)
        };

        if self.is_empty() {
            return handle_empty(state);
        }

        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values = get_pooled_vec_f64(self.count);
        self.decompress(&mut timestamps, &mut values)?;
        // special case of range exceeding block range
        if start <= self.min_time && end >= self.max_time {
            return f(state, &timestamps, &values);
        }

        trim_vec_data(&mut timestamps, &mut values, start, end);
        f(state, &timestamps, &values)
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

    pub fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> impl Iterator<Item = Sample> + '_ {
        PcoChunkIterator::new_range(self, start_ts, end_ts)
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
        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values = get_pooled_vec_f64(self.count);
        self.decompress(&mut timestamps, &mut values)?;

        if let Some((start_idx, end_idx)) = get_timestamp_index_bounds(&timestamps, start_ts, end_ts) {
            let save_count = self.count;
            let timestamp_slice = &timestamps[start_idx..end_idx];
            let value_slice = &values[start_idx..end_idx];
            self.compress(timestamp_slice, value_slice)?;
            Ok(save_count - self.count)
        } else {
            self.count = 0;
            self.timestamps.clear();
            self.values.clear();
            self.min_time = 0;
            self.max_time = 0;
            Ok(0)
        }
    }
    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_full() {
            return Err(TsdbError::CapacityFull(self.max_size));
        }
        if self.is_empty() {
            let timestamps = vec![sample.timestamp];
            let values = vec![sample.value];
            return self.compress(&timestamps, &values);
        }
        let mut timestamps = get_pooled_vec_i64(self.count.min(4));
        let mut values = get_pooled_vec_f64(self.count.min(4));
        self.decompress(&mut timestamps, &mut values)?;
        timestamps.push(sample.timestamp);
        values.push(sample.value);
        self.compress(&timestamps, &values)
    }

    fn get_range(
        &self,
        start: Timestamp,
        end: Timestamp,
        timestamps: &mut Vec<Timestamp>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        if self.is_empty() {
            return Ok(());
        }
        self.decompress(timestamps, values)?;
        trim_vec_data(timestamps, values, start, end);
        Ok(())
    }

    fn upsert_sample(
        &mut self,
        sample: &mut Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;

        if self.is_empty() {
            self.add_sample(sample)?;
            return Ok(1)
        }

        // we currently don't do streaming compression, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old one
        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values = get_pooled_vec_f64(self.count);
        self.decompress(&mut timestamps, &mut values)?;

        match timestamps.binary_search(&ts) {
            Ok(pos) => {
                duplicate_found = true;
                values[pos] = dp_policy.value_on_duplicate(ts, values[pos], sample.value)?;
            }
            Err(idx) => {
                timestamps.insert(idx, ts);
                values.insert(idx, sample.value);
            }
        };

        let mut size = timestamps.len();
        if duplicate_found {
            size -= 1;
        }

        self.compress(&timestamps, &values)?;

        Ok(size)
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
        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values = get_pooled_vec_f64(self.count);
        self.decompress(&mut timestamps, &mut values)?;

        let slice = SeriesSlice::new(&timestamps, &values);
        let (left, right) = slice.split_at(mid);
        self.compress(left.timestamps, left.values)?;

        result.compress(right.timestamps, right.values)?;

        Ok(result)
    }

    fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        raw::save_signed(rdb, self.min_time);
        raw::save_signed(rdb, self.max_time);
        raw::save_unsigned(rdb, self.max_size as u64);
        raw::save_double(rdb, self.last_value);
        raw::save_unsigned(rdb, self.count as u64);
        raw::save_slice(rdb, &self.timestamps);
        raw::save_slice(rdb, &self.values);
    }

    fn rdb_load(rdb: *mut raw::RedisModuleIO) -> Result<Self, valkey_module::error::Error> {
        let min_time = raw::load_signed(rdb)?;
        let max_time = raw::load_signed(rdb)?;
        let max_size = raw::load_unsigned(rdb)? as usize;
        let last_value = raw::load_double(rdb)?;
        let count = raw::load_unsigned(rdb)? as usize;
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
    pco_encode_with_options(timestamps, compressed, config)
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
        let capacity = self.chunk.num_samples();
        let mut timestamps = Vec::with_capacity(capacity);
        let mut values = Vec::with_capacity(capacity);

        match self.chunk.decompress(&mut timestamps, &mut values) {
            Ok(_) => {
                if self.has_range() {
                    if let Some((start_index, end_index)) = get_timestamp_index_bounds(&timestamps, self.start, self.end) {
                        timestamps.drain(end_index..);
                        values.drain(end_index..);
                        self.idx = start_index;
                    } else {
                        // dates are out of range
                        timestamps = vec![];
                        values = vec![];
                    }
                }
                self.timestamps = timestamps;
                self.values = values;
            }
            Err(_idx) => {
                // todo: log
            }
        }
    }
}

impl<'a> Iterator for PcoChunkIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_init {
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
    use std::time::Duration;

    use rand::Rng;

    use crate::error::TsdbError;
    use crate::storage::chunk::Chunk;
    use crate::storage::pco_chunk::PcoChunk;
    use crate::storage::series_data::SeriesData;
    use crate::storage::{DuplicatePolicy, Sample};
    use crate::tests::generators::{create_rng, generate_series_data, generate_timestamps, GeneratorOptions, RandAlgo};

    fn decompress(chunk: &PcoChunk) -> SeriesData {
        let mut timestamps = vec![];
        let mut values = vec![];
        chunk.decompress(&mut timestamps, &mut values).unwrap();
        SeriesData::new_with_data(timestamps, values)
    }

    pub(crate) fn saturate_compressed_chunk(chunk: &mut PcoChunk) {
        let mut rng = create_rng(None).unwrap();
        let mut ts: i64 = 1;
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

    fn populate_series_data(chunk: &mut PcoChunk, samples: usize) {
        let mut options = GeneratorOptions::default();
        options.samples = samples;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();
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
        let mut options = GeneratorOptions::default();
        options.samples = 1000;
        options.range = 0.0..100.0;
        options.typ = RandAlgo::Uniform;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();
        assert_eq!(chunk.num_samples(), data.len());
        assert_eq!(chunk.min_time, data.timestamps[0]);
        assert_eq!(chunk.max_time, data.timestamps[data.len() - 1]);
        assert_eq!(chunk.last_value, data.values[data.len() - 1]);
        assert!(chunk.timestamps.len() > 0);
        assert!(chunk.values.len() > 0);
    }

    #[test]
    fn test_compress_decompress() {
        let mut chunk = PcoChunk::default();
        let timestamps = generate_timestamps(1000, 1000, Duration::from_secs(5));
        let values = timestamps.iter().map(|x| *x as f64).collect::<Vec<f64>>();

        chunk.set_data(&timestamps, &values).unwrap();
        let mut timestamps2 = vec![];
        let mut values2 = vec![];
        chunk.decompress(&mut timestamps2, &mut values2).unwrap();
        assert_eq!(timestamps, timestamps2);
        assert_eq!(values, values2);
    }

    #[test]
    fn test_clear() {
        let mut chunk = PcoChunk::default();
        let mut options = GeneratorOptions::default();
        options.samples = 500;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();
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
            let mut options = GeneratorOptions::default();
            options.samples = 500;
            let data = generate_series_data(&options).unwrap();
            let mut chunk = PcoChunk::with_max_size(chunk_size);

            for mut sample in data.iter() {
                chunk.upsert_sample(&mut sample, DuplicatePolicy::KeepLast).unwrap();
            }
            assert_eq!(chunk.num_samples(), data.len());
        }
    }

    #[test]
    fn test_upsert_while_at_capacity() {
        let mut chunk = PcoChunk::with_max_size(4096);
        saturate_compressed_chunk(&mut chunk);

        let timestamp = chunk.last_timestamp();

        // return an error on insert
        let mut sample = Sample {
            timestamp: 0,
            value: 1.0,
        };

        assert!(chunk.upsert_sample(&mut sample, DuplicatePolicy::KeepLast).is_err());

        // should update value for duplicate timestamp
        sample.timestamp = timestamp;
        let res = chunk.upsert_sample(&mut sample, DuplicatePolicy::KeepLast);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }

    #[test]
    fn test_split() {
        let mut chunk = PcoChunk::default();
        let mut options = GeneratorOptions::default();
        options.samples = 500;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();

        let count = data.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.num_samples(), mid);
        assert_eq!(right.num_samples(), mid);

        let (l_times, r_times) = data.timestamps.split_at(mid);
        let (l_values, r_values) = data.values.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed.timestamps, r_times);
        assert_eq!(right_decompressed.values, r_values);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed.timestamps, l_times);
        assert_eq!(left_decompressed.values, l_values);
    }

    #[test]
    fn test_split_odd() {
        let mut chunk = PcoChunk::default();
        let mut options = GeneratorOptions::default();
        options.samples = 51;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();

        let count = data.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.num_samples(), mid);
        assert_eq!(right.num_samples(), mid + 1);

        let (l_times, r_times) = data.timestamps.split_at(mid);
        let (l_values, r_values) = data.values.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed.timestamps, r_times);
        assert_eq!(right_decompressed.values, r_values);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed.timestamps, l_times);
        assert_eq!(left_decompressed.values, l_values);
    }

}