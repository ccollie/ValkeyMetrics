use std::cmp::Ordering;
use crate::common::current_time_millis;
use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use super::{XOREncoder, XORIterator};
use crate::series::chunks::chunk::Chunk;
use crate::series::{DuplicatePolicy, Sample, DEFAULT_CHUNK_SIZE_BYTES};
use get_size::GetSize;
use metricsql_common::pool::{get_pooled_vec_f64, get_pooled_vec_i64};
use std::mem::size_of;
use std::ops::ControlFlow;
use valkey_module::error::Error as ValkeyError;
use valkey_module::raw;

/// `GorillaChunk` holds information about location and time range of a block of compressed data.
#[derive(Debug, Clone, PartialEq)]
#[derive(GetSize)]
pub struct GorillaChunk {
    xor_encoder: XOREncoder,
    first_timestamp: Timestamp,
    pub max_size: usize,
}

impl Default for GorillaChunk {
    fn default() -> Self {
        Self::with_max_size(DEFAULT_CHUNK_SIZE_BYTES)
    }
}

impl GorillaChunk {
    pub fn with_max_size(max_size: usize) -> Self {
        let now = current_time_millis();
        Self {
            xor_encoder: XOREncoder::new(),
            first_timestamp: now,
            max_size,
        }
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
        self.num_samples()
    }

    pub fn is_empty(&self) -> bool {
        self.num_samples() == 0
    }

    pub fn is_full(&self) -> bool {
        let usage = self.xor_encoder.get_size();
        usage >= self.max_size
    }

    pub fn clear(&mut self) {
        self.xor_encoder.clear();
        self.first_timestamp = 0;
    }

    pub fn set_data(&mut self, timestamps: &[i64], values: &[f64]) -> TsdbResult<()> {
        debug_assert_eq!(timestamps.len(), values.len());
        self.compress(timestamps, values)?;
        // todo: complain if size > max_size
        Ok(())
    }

    pub(in crate::series) fn compress(&mut self, timestamps: &[Timestamp], values: &[f64]) -> TsdbResult<()> {
        debug_assert_eq!(timestamps.len(), values.len());
        let mut encoder = XOREncoder::new();
        for (ts, value) in timestamps.iter().zip(values.iter()) {
            let sample = Sample::new(*ts, *value);
            push_sample(&mut encoder, &sample)?;
        }
        self.xor_encoder = encoder;
        Ok(())
    }

    pub(in crate::series) fn decompress(
        &self,
        timestamps: &mut Vec<Timestamp>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        if self.is_empty() {
            return Ok(());
        }
        timestamps.reserve(self.num_samples());
        values.reserve(self.num_samples());
        for item in self.xor_encoder.iter() {
            let sample = item?;
            timestamps.push(sample.timestamp);
            values.push(sample.value);
        }
        Ok(())
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = self.xor_encoder.buf_len();
        let uncompressed_size = self.num_samples() * (size_of::<i64>() + size_of::<f64>());
        (uncompressed_size / compressed_size) as f64
    }

    pub fn process_samples<F, State>(&self, state: &mut State, mut f: F) -> TsdbResult<()>
    where
        F: FnMut(&mut State, &Sample) -> ControlFlow<()>,
    {
        for value in self.xor_encoder.iter() {
            let sample = value?;
            match f(state, &sample) {
                ControlFlow::Break(_) => break,
                ControlFlow::Continue(_) => continue,
            }
        }

        Ok(())
    }

    pub fn process_samples_in_range<F, State>(
        &self,
        state: &mut State,
        start_ts: Timestamp,
        end_ts: Timestamp,
        mut f: F
    ) -> TsdbResult<()>
    where
        F: FnMut(&mut State, &Sample) -> ControlFlow<()>,
    {
        for sample in self.range_iter(start_ts, end_ts) {
            match f(state, &sample) {
                ControlFlow::Break(_) => break,
                ControlFlow::Continue(_) => continue,
            }
        }

        Ok(())
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
        if self.is_empty() {
            let mut timestamps = vec![];
            let mut values = vec![];
            return f(state, &mut timestamps, &mut values)
        }

        let count = self.num_samples();
        let mut timestamps = get_pooled_vec_i64(count);
        let mut values = get_pooled_vec_f64(count);

        let mut inner_state = (&mut timestamps, &mut values);

        self.process_samples_in_range(&mut inner_state, start, end, |state, sample| {
            state.0.push(sample.timestamp);
            state.1.push(sample.value);
            ControlFlow::Continue(())
        })?;

        f(state, &timestamps, &values)
    }

    pub fn data_size(&self) -> usize {
        self.xor_encoder.get_size()
    }

    pub fn bytes_per_sample(&self) -> usize {
        let count = self.num_samples();
        if  count == 0 {
            return 0;
        }
        self.data_size() / count
    }

    /// estimate remaining capacity based on the current data size and chunk max_size
    pub fn remaining_capacity(&self) -> usize {
        self.max_size - self.data_size()
    }

    /// Estimate the number of samples that can be stored in the remaining capacity
    /// Note that for low sample counts this will be very inaccurate
    pub fn remaining_samples(&self) -> usize {
        if self.num_samples() == 0 {
            return 0;
        }
        self.remaining_capacity() / self.bytes_per_sample()
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.get_heap_size()
    }

    fn buf(&self) -> &[u8] {
        &self.xor_encoder.writer.writer
    }

    pub fn iter(&self) -> impl Iterator<Item = Sample> + '_ {
        ChunkIter::new(self)
    }

    pub fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> RangeChunkIter {
        RangeChunkIter::new(self, start_ts, end_ts)
    }

    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>>  {
        if self.num_samples() == 0 || timestamps.is_empty() {
            return Ok(vec![]);
        }
        let mut samples = Vec::with_capacity(timestamps.len());
        let mut timestamps = timestamps;
        let first_timestamp = timestamps[0].max(self.first_timestamp);
        let last_timestamp = timestamps[timestamps.len() - 1].min(self.last_timestamp());

        for sample in self.range_iter(first_timestamp, last_timestamp) {
            let first_ts = timestamps[0];
            match sample.timestamp.cmp(&first_ts) {
                Ordering::Less => continue,
                Ordering::Equal => {
                    timestamps = &timestamps[1..];
                    samples.push(sample);
                }
                Ordering::Greater => {
                    timestamps = &timestamps[1..];
                    if timestamps.is_empty() {
                        break;
                    }
                }
            }
        }

        Ok(samples)
    }

    fn is_range_covering_full_period(&self, start_ts: Timestamp, end_ts: Timestamp) -> bool {
        start_ts <= self.first_timestamp() && end_ts >= self.last_timestamp()
    }

    fn populate_encoder_range(
        &self,
        encoder: &mut XOREncoder,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> TsdbResult<()> {
        for value in self.xor_encoder.iter() {
            let sample = value?;
            if sample.timestamp < start_ts || sample.timestamp >= end_ts {
                push_sample(encoder, &sample)?;
            }
        }
        Ok(())
    }
}

impl Chunk for GorillaChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.first_timestamp
    }
    fn last_timestamp(&self) -> Timestamp {
        self.xor_encoder.timestamp
    }
    fn num_samples(&self) -> usize {
        self.xor_encoder.num_samples
    }
    fn last_value(&self) -> f64 {
        self.xor_encoder.value
    }
    fn size(&self) -> usize {
        self.data_size()
    }
    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        if self.is_empty() {
            return Ok(0);
        }

        if self.is_range_covering_full_period(start_ts, end_ts) {
            self.clear();
            return Ok(0);
        }

        let old_sample_count = self.xor_encoder.num_samples;
        let mut new_encoder = XOREncoder::new();

        self.populate_encoder_range(&mut new_encoder, start_ts, end_ts)?;

        self.xor_encoder = new_encoder;

        Ok(self.num_samples() - old_sample_count)
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_full() {
            return Err(TsdbError::CapacityFull(self.max_size));
        }

        push_sample(&mut self.xor_encoder, sample)?;

        self.first_timestamp = self.first_timestamp.min(sample.timestamp);

        Ok(())
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

        for sample in self.xor_encoder.iter() {
            let sample = sample?;
            if sample.timestamp >= start {
                timestamps.push(sample.timestamp);
                values.push(sample.value);
            }
            if sample.timestamp > end {
                break;
            }
        }

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

        let count = self.num_samples();
        let mut xor_encoder = XOREncoder::new();

        let mut iter = self.xor_encoder.iter();

        let mut current = Sample::default();

        // skip previous samples
        for item in iter.by_ref() {
            current = item?;
            if current.timestamp >= ts {
                break;
            }
            push_sample(&mut xor_encoder, &current)?;
        }

        if current.timestamp == ts {
            duplicate_found = true;
            current.value = dp_policy.value_on_duplicate(ts, current.value, sample.value)?;
            iter.next();
        } else {
            push_sample(&mut xor_encoder, sample)?;
        }

        for item in iter {
            current = item?;
            push_sample(&mut xor_encoder, &current)?;
        }

        // todo: do a self.encoder.buf.take()
        self.xor_encoder = xor_encoder;
        let size = if duplicate_found { count } else { count + 1 };
        Ok(size)
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let mut left_chunk = XOREncoder::new();
        let mut right_chunk = GorillaChunk::default();

        if self.is_empty() {
            return Ok(self.clone());
        }

        let mid = self.num_samples() / 2;
        for (i, value) in self.xor_encoder.iter().enumerate() {
            let sample = value?;
            if i < mid {
                // todo: handle min and max timestamps
                push_sample(&mut left_chunk, &sample)?;
            } else {
                push_sample(&mut right_chunk.xor_encoder, &sample)?;
            }
        }
        self.xor_encoder = left_chunk;

        Ok(right_chunk)
    }

    fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        raw::save_unsigned(rdb, self.max_size as u64);
        raw::save_signed(rdb, self.first_timestamp);
        self.xor_encoder.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut raw::RedisModuleIO) -> Result<Self, ValkeyError> {
        let max_size = raw::load_unsigned(rdb)? as usize;
        let first_timestamp = raw::load_signed(rdb)?;
        let xor_encoder = XOREncoder::rdb_load(rdb)?;
        let chunk = GorillaChunk {
            xor_encoder,
            first_timestamp,
            max_size,
        };
        Ok(chunk)
    }
}

fn push_sample(encoder: &mut XOREncoder, sample: &Sample) -> TsdbResult<()> {
    encoder.add_sample(sample)
        .map_err(|e| {
            println!("Error adding sample: {:?}", e);
            TsdbError::CannotAddSample(*sample)
        })
}

pub(crate) struct ChunkIter<'a> {
    inner: XORIterator<'a>,
}

impl<'a> ChunkIter<'a> {
    pub fn new(chunk: &'a GorillaChunk) -> Self {
        let inner = XORIterator::new(&chunk.xor_encoder);
        Self { inner }
    }
}

impl<'a> Iterator for ChunkIter<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(sample)) => Some(sample),
            Some(Err(err)) => {
                #[cfg(debug_assertions)]
                eprintln!("Error decoding sample: {:?}", err);
                None
            },
            None => None,
        }
    }
}


pub(crate) struct RangeChunkIter<'a> {
    inner: XORIterator<'a>,
    start: Timestamp,
    end: Timestamp,
    init: bool
}

impl<'a> RangeChunkIter<'a> {
    pub fn new(chunk: &'a GorillaChunk, start: Timestamp, end: Timestamp) -> Self {
        let inner = XORIterator::new(&chunk.xor_encoder);
        Self { inner, start, end, init: false }
    }

    fn next_internal(&mut self) -> Option<Sample> {
        match self.inner.next() {
            Some(Ok(sample)) => Some(sample),
            Some(Err(err)) => {
                #[cfg(debug_assertions)]
                eprintln!("Error decoding sample: {:?}", err);
                None
            },
            None => None,
        }
    }
}

impl<'a> Iterator for RangeChunkIter<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.init {
            self.init = true;

            while let Some(sample) = self.next_internal() {
                if sample.timestamp > self.end {
                    return None;
                }
                if sample.timestamp < self.start {
                    continue;
                }
                return Some(sample);
            }

        }
        self.next_internal()
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::time::Duration;

    use crate::error::TsdbError;
    use crate::series::chunks::chunk::Chunk;
    use crate::series::chunks::gorilla::gorilla_chunk::GorillaChunk;
    use crate::series::series_data::SeriesData;
    use crate::series::{DuplicatePolicy, Sample};
    use crate::tests::generators::{create_rng, generate_series_data, generate_timestamps, GeneratorOptions, RandAlgo};

    fn decompress(chunk: &GorillaChunk) -> SeriesData {
        let mut timestamps = vec![];
        let mut values = vec![];
        chunk.decompress(&mut timestamps, &mut values).unwrap();
        SeriesData::new_with_data(timestamps, values)
    }

    pub(crate) fn saturate_compressed_chunk(chunk: &mut GorillaChunk) {
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

    fn populate_series_data(chunk: &mut GorillaChunk, samples: usize) {
        let mut options = GeneratorOptions::default();
        options.samples = samples;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();
    }

    fn compare_chunks(chunk1: &GorillaChunk, chunk2: &GorillaChunk) {
        assert_eq!(chunk1.xor_encoder, chunk2.xor_encoder, "xor chunks do not match");
        assert_eq!(chunk1.max_size, chunk2.max_size);
    }

    #[test]
    fn test_chunk_compress() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let mut options = GeneratorOptions::default();
        options.samples = 1000;
        options.range = 0.0..100.0;
        options.typ = RandAlgo::MackeyGlass;
  //    options.significant_digits = Some(8);
        let data = generate_series_data(&options).unwrap();
        for sample in data.iter() {
            chunk.add_sample(&sample).unwrap();
        }
        assert_eq!(chunk.num_samples(), data.len());
        assert_eq!(chunk.first_timestamp(), data.timestamps[0]);
        assert_eq!(chunk.last_timestamp(), data.timestamps[data.len() - 1]);
        assert_eq!(chunk.last_value(), data.values[data.len() - 1]);
    }

    #[test]
    fn test_compress_decompress() {
        let mut chunk = GorillaChunk::default();
        let timestamps = generate_timestamps(500, 1000, Duration::from_secs(5));
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
        let mut chunk = GorillaChunk::default();
        let mut options = GeneratorOptions::default();
        options.samples = 500;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();
        assert_eq!(chunk.num_samples(), data.len());
        chunk.clear();
        assert_eq!(chunk.num_samples(), 0);
        assert_eq!(chunk.first_timestamp(), 0);
        assert_eq!(chunk.last_timestamp(), 0);
    }

    #[test]
    fn test_upsert() {
        for chunk_size in (64..8192).step_by(64) {
            let mut options = GeneratorOptions::default();
            options.samples = 200;
            let data = generate_series_data(&options).unwrap();
            let mut chunk = GorillaChunk::with_max_size(chunk_size);

            for mut sample in data.iter() {
                chunk.upsert_sample(&mut sample, DuplicatePolicy::KeepLast).unwrap();
            }
            assert_eq!(chunk.num_samples(), data.len());
        }
    }

    #[test]
    fn test_upsert_while_at_capacity() {
        let mut chunk = GorillaChunk::with_max_size(4096);
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
        let mut chunk = GorillaChunk::default();
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
        let mut chunk = GorillaChunk::default();
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