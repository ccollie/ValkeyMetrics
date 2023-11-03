use get_size::GetSize;
use metricsql_common::pool::{get_pooled_vec_f64, get_pooled_vec_i64};
use serde::{Deserialize, Serialize};

use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::storage::{DEFAULT_CHUNK_SIZE_BYTES, DuplicatePolicy, F64_SIZE, I64_SIZE, Sample, SeriesSlice, VEC_BASE_SIZE};
use crate::storage::chunk::Chunk;
use crate::storage::serialization::{compress_timestamps, compress_values, COMPRESSION_PARALLELIZATION_THRESHOLD, CompressionOptions, decompress_timestamps, decompress_values};
use crate::storage::utils::{get_timestamp_index_bounds, trim_vec_data};

/// `CompressedBlock` holds information about location and time range of a block of compressed data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[derive(GetSize)]
pub struct CompressedChunk {
    pub min_time: i64,
    pub max_time: i64,
    pub max_size: usize,
    pub last_value: f64,
    pub options: CompressionOptions,
    /// number of compressed samples
    pub count: usize,
    pub timestamps: Vec<u8>,
    pub values: Vec<u8>,
}

impl Default for CompressedChunk {
    fn default() -> Self {
        Self {
            min_time: 0,
            max_time: i64::MAX,
            max_size: DEFAULT_CHUNK_SIZE_BYTES,
            last_value: 0.0,
            options: CompressionOptions::default(),
            count: 0,
            timestamps: Vec::new(),
            values: Vec::new(),
        }
    }
}

impl CompressedChunk {
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
                || compress_timestamps(&mut t_data, timestamps, &self.options).ok(),
                || compress_values(&mut v_data, values, &self.options).ok(),
            );
            // then we put the buffers back
            self.timestamps = t_data;
            self.values = v_data;
        } else {
            self.timestamps.clear();
            self.values.clear();
            compress_timestamps(&mut self.timestamps, timestamps, &self.options)?;
            compress_values(&mut self.values, values, &self.options)?;
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
        // todo: dynamically calculate cutoff
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
        let uncompressed_size = self.count as f64 * I64_SIZE as f64;
        uncompressed_size / compressed_size
    }

    pub fn value_compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = self.values.len() as f64;
        let uncompressed_size = self.count as f64 * F64_SIZE as f64;
        uncompressed_size / compressed_size
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = (self.timestamps.len() + self.values.len()) as f64;
        let uncompressed_size = (self.count * (I64_SIZE + F64_SIZE)) as f64;
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
            return f(state, &mut timestamps, &mut values);
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
        std::mem::size_of::<Self>() +
        self.get_heap_size()
    }
}

impl Chunk for CompressedChunk {
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
        result.options = self.options.clone();

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

        result.compress(&right.timestamps, &right.values)?;

        Ok(result)
    }
}



#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::Rng;

    use crate::error::TsdbError;
    use crate::storage::{DuplicatePolicy, Sample};
    use crate::storage::chunk::Chunk;
    use crate::storage::compressed_chunk::{CompressedChunk, decompress_timestamps};
    use crate::storage::serialization::{compress_timestamps, CompressionOptimization, CompressionOptions};
    use crate::storage::series_data::SeriesData;
    use crate::tests::generators::{create_rng, generate_series_data, generate_timestamps, GeneratorOptions, RandAlgo};

    fn decompress(chunk: &CompressedChunk) -> SeriesData {
        let mut timestamps = vec![];
        let mut values = vec![];
        chunk.decompress(&mut timestamps, &mut values).unwrap();
        SeriesData::new_with_data(timestamps, values)
    }

    pub(crate) fn saturate_compressed_chunk(chunk: &mut CompressedChunk) {
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

    fn populate_series_data(chunk: &mut CompressedChunk, samples: usize) {
        let mut options = GeneratorOptions::default();
        options.samples = samples;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();
    }

    fn compare_chunks(chunk1: &CompressedChunk, chunk2: &CompressedChunk) {
        assert_eq!(chunk1.min_time, chunk2.min_time, "min_time");
        assert_eq!(chunk1.max_time, chunk2.max_time);
        assert_eq!(chunk1.max_size, chunk2.max_size);
        assert_eq!(chunk1.last_value, chunk2.last_value);
        assert_eq!(chunk1.options, chunk2.options);
        assert_eq!(chunk1.count, chunk2.count, "mismatched counts {} vs {}", chunk1.count, chunk2.count);
        assert_eq!(chunk1.timestamps, chunk2.timestamps);
        assert_eq!(chunk1.values, chunk2.values);
    }

    #[test]
    fn test_compress_timestamps() {
        use CompressionOptimization::*;

        fn run_test(option: &CompressionOptions) {
            let timestamps: Vec<i64> = generate_timestamps(1000, 1000, Duration::from_secs(5));

            let mut dst: Vec<u8> = Vec::with_capacity(1000);
            assert!(compress_timestamps(&mut dst, &timestamps, &option).is_ok());

            let mut decompressed: Vec<i64> = Vec::with_capacity(1000);
            assert!(decompress_timestamps(&dst, &mut decompressed).is_ok());

            assert_eq!(timestamps, decompressed);
        }

        let mut options = CompressionOptions::default();
        options.optimization = Speed;
        run_test(&options);

        options.optimization = Size;
        run_test(&options);
    }

    #[test]
    fn test_compress_timestamps_size_optimization() {

        fn compress(option: CompressionOptimization) -> Vec<u8> {
            let timestamps: Vec<i64> = generate_timestamps(1000, 1000, Duration::from_secs(5));
            let mut dst: Vec<u8> = Vec::with_capacity(1000);
            let options = CompressionOptions {
                optimization: option,
                ..Default::default()
            };
            assert!(compress_timestamps(&mut dst, &timestamps, &options).is_ok());
            dst
        }

        let normal = compress(CompressionOptimization::default());
        let speed = compress(CompressionOptimization::Speed);
        let size = compress(CompressionOptimization::Size);

        assert!(size.len() < normal.len());
        assert!(size.len() < speed.len());
    }
        #[test]
    fn test_chunk_compress() {
        let mut chunk = CompressedChunk::default();
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
        let mut chunk = CompressedChunk::default();
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
        let mut chunk = CompressedChunk::default();
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
            let mut chunk = CompressedChunk::with_max_size(chunk_size);

            for mut sample in data.iter() {
                chunk.upsert_sample(&mut sample, DuplicatePolicy::KeepLast).unwrap();
            }
            assert_eq!(chunk.num_samples(), data.len());
        }
    }

    #[test]
    fn test_upsert_while_at_capacity() {
        let mut chunk = CompressedChunk::with_max_size(4096);
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
        let mut chunk = CompressedChunk::default();
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
        let mut chunk = CompressedChunk::default();
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