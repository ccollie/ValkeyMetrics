use super::{XOREncoder, XORIterator};
use crate::common::current_time_millis;
use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::iter::SampleIter;
use crate::series::chunks::chunk::Chunk;
use crate::series::serialization::{rdb_load_timestamp, rdb_load_usize, rdb_save_timestamp, rdb_save_usize};
use crate::series::{DuplicatePolicy, Sample, DEFAULT_CHUNK_SIZE_BYTES};
use get_size::GetSize;
use metricsql_common::pool::{get_pooled_vec_f64, get_pooled_vec_i64};
use std::cmp::Ordering;
use std::collections::BTreeSet;
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

    pub fn with_values(max_size: usize, samples: &[Sample]) -> TsdbResult<Self> {
        let mut res = Self::default();
        res.max_size = max_size;

        let count = samples.len();
        if count > 0 {
            res.set_data(samples)?;
        }

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

    pub fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        debug_assert!(!samples.is_empty());
        self.compress(samples)
        // todo: complain if size > max_size
    }

    fn compress(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        let mut encoder = XOREncoder::new();
        for sample in samples {
            push_sample(&mut encoder, sample)?;
        }
        self.xor_encoder = encoder;
        Ok(())
    }

    fn decompress(&self) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() {
            return Ok(vec![]);
        }

        let mut values: Vec<Sample> = Vec::with_capacity(self.num_samples());
        for item in self.xor_encoder.iter() {
            values.push(item?);
        }

        Ok(values)
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = self.xor_encoder.buf().len();
        let uncompressed_size = self.num_samples() * (size_of::<i64>() + size_of::<f64>());
        (uncompressed_size / compressed_size) as f64
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

    pub fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> SampleIter {
        GorillaChunkIterator::new(self, start_ts, end_ts).into()
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
    fn max_size(&self) -> usize {
        self.max_size
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

        for value in self.xor_encoder.iter() {
            let sample = value?;
            if sample.timestamp < start_ts || sample.timestamp > end_ts {
                push_sample(&mut new_encoder, &sample)?;
            }
        }

        self.xor_encoder = new_encoder;
        let new_count = self.num_samples();

        Ok(old_sample_count - new_count)
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_full() {
            return Err(TsdbError::CapacityFull(self.max_size));
        }

        push_sample(&mut self.xor_encoder, sample)?;

        self.first_timestamp = self.first_timestamp.min(sample.timestamp);

        Ok(())
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() {
            return Ok(vec![]);
        }

        let mut samples = Vec::new();
        for sample in self.xor_encoder.iter() {
            let sample = sample?;
            if sample.timestamp > end {
                break;
            }
            if sample.timestamp >= start {
                samples.push(sample);
            }
        }

        Ok(samples)
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;

        if self.is_empty() {
            self.add_sample(&sample)?;
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
            current.value = dp_policy.duplicate_value(ts, current.value, sample.value)?;
            push_sample(&mut xor_encoder, &current)?;
            iter.next();
        } else {
            push_sample(&mut xor_encoder, &sample)?;
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

    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: DuplicatePolicy,
        blocked: &mut BTreeSet<Timestamp>,
    ) -> TsdbResult<usize> {
        if samples.len() == 1 {
            let first = samples[0];
            if self.is_empty() {
                self.add_sample(&first)?;
                return Ok(1);
            }
            return self.upsert_sample(first, dp_policy);
        } else if self.is_empty() {
            return match self.set_data(samples) {
                Ok(_) => Ok(self.num_samples()),
                Err(e) => Err(e),
            };
        }

        let mut count = self.num_samples();
        let mut xor_encoder = XOREncoder::new();

        let mut left = samples.iter().peekable();
        let mut right = self.iter().peekable();

        loop {
            let merged = match (left.peek(), right.peek()) {
                (Some(l), Some(r)) => {
                    if l.timestamp == r.timestamp {
                        let ts = l.timestamp;
                        if let Ok(value) = dp_policy.duplicate_value(ts, r.value, l.value) {
                            left.next();
                            Sample {
                                timestamp: ts,
                                value,
                            }
                        } else {
                            blocked.insert(ts);
                            left.next();
                            continue;
                        }
                    } else if l.timestamp < r.timestamp {
                        let res = *(*l);
                        left.next();
                        res
                    } else {
                        let res = *r;
                        right.next();
                        res
                    }
                }
                (Some(l), None) => {
                    let res = **l;
                    left.next();
                    res
                }
                (None, Some(r)) => {
                    let res = *r;
                    right.next();
                    res
                }
                (None, None) => break,
            };
            count += 1;
            push_sample(&mut xor_encoder, &merged)?;
        }

        // the right iterator holds a reference to self, so manually drop it to prevent an issue
        // below with the borrow checker
        drop(right);

        self.xor_encoder = xor_encoder;
        Ok(count)
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
        rdb_save_usize(rdb, self.max_size);
        rdb_save_timestamp(rdb, self.first_timestamp);
        self.xor_encoder.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut raw::RedisModuleIO, _encver: i32) -> Result<Self, ValkeyError> {
        let max_size = rdb_load_usize(rdb)?;
        let first_timestamp = rdb_load_timestamp(rdb)?;
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


pub struct GorillaChunkIterator<'a> {
    inner: XORIterator<'a>,
    start: Timestamp,
    end: Timestamp,
    init: bool
}

impl<'a> GorillaChunkIterator<'a> {
    pub fn new(chunk: &'a GorillaChunk, start: Timestamp, end: Timestamp) -> Self {
        let inner = XORIterator::new(&chunk.xor_encoder);
        Self { inner, start, end, init: false }
    }

    fn next_internal(&mut self) -> Option<Sample> {
        match self.inner.next() {
            Some(Ok(sample)) => {
                if sample.timestamp > self.end {
                    return None;
                }
                Some(sample)
            },
            Some(Err(err)) => {
                #[cfg(debug_assertions)]
                eprintln!("Error decoding sample: {:?}", err);
                None
            },
            None => None,
        }
    }
}

impl<'a> Iterator for GorillaChunkIterator<'a> {
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
    use crate::error::TsdbError;
    use crate::series::chunks::chunk::Chunk;
    use crate::series::chunks::gorilla::gorilla_chunk::GorillaChunk;
    use crate::series::test_utils::generate_random_samples;
    use crate::series::{DuplicatePolicy, Sample};
    use crate::tests::generators::GeneratorOptions;

    fn decompress(chunk: &GorillaChunk) -> Vec<Sample> {
        chunk.iter().collect()
    }

    fn compare_chunks(chunk1: &GorillaChunk, chunk2: &GorillaChunk) {
        assert_eq!(chunk1.xor_encoder, chunk2.xor_encoder, "xor chunks do not match");
        assert_eq!(chunk1.max_size, chunk2.max_size);
    }

    #[test]
    fn test_chunk_compress() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let options = GeneratorOptions::default();
  //    options.significant_digits = Some(8);
        let data = generate_random_samples(0, 1000);

        for sample in data.iter() {
            chunk.add_sample(&sample).unwrap();
        }
        assert_eq!(chunk.num_samples(), data.len());
        assert_eq!(chunk.first_timestamp(), data[0].timestamp);
        assert_eq!(chunk.last_timestamp(), data[data.len() - 1].timestamp);
        assert_eq!(chunk.last_value(), data[data.len() - 1].value);
    }

    #[test]
    fn test_compress_decompress() {
        let mut chunk = GorillaChunk::default();
        let expected = generate_random_samples(0, 1000);

        chunk.set_data(&expected).unwrap();
        let actual = chunk.decompress().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_clear() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let data = generate_random_samples(0, 500);

        for datum in data.iter() {
            chunk.add_sample(datum).unwrap();
        }

        assert_eq!(chunk.num_samples(), data.len());
        chunk.clear();
        assert_eq!(chunk.num_samples(), 0);
        assert_eq!(chunk.first_timestamp(), 0);
        assert_eq!(chunk.last_timestamp(), 0);
    }

    #[test]
    fn test_upsert() {
        for chunk_size in (64..8192).step_by(64) {
            const SAMPLE_COUNT: usize = 200;
            let mut samples = generate_random_samples(0, SAMPLE_COUNT);
            let mut chunk = GorillaChunk::with_max_size(chunk_size);

            let sample_count = samples.len();
            for sample in samples.into_iter() {
                chunk.upsert_sample(sample, DuplicatePolicy::KeepLast).unwrap();
            }
            assert_eq!(chunk.num_samples(), sample_count);
        }
    }

    #[test]
    fn test_upsert_while_at_capacity() {
        let mut chunk = GorillaChunk::with_max_size(4096);

        let mut ts = 1000;
        let mut value: f64 = 1.0;

        loop {
            let sample = Sample {
                timestamp: ts,
                value
            };
            ts += 1000;
            value *= 2.0;

            match chunk.add_sample(&sample) {
                Ok(_) => {}
                Err(TsdbError::CapacityFull(_)) => {
                    break
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        }

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
        const COUNT: usize = 500;
        let samples = generate_random_samples(0, COUNT);
        let mut chunk = GorillaChunk::with_max_size(16384);

        for sample in samples.iter() {
            chunk.add_sample(&sample).unwrap();
        }

        let count = samples.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.num_samples(), mid);
        assert_eq!(right.num_samples(), mid);

        let (left_samples, right_samples) = samples.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed, right_samples);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed, left_samples);
    }

    #[test]
    fn test_split_odd() {
        const COUNT: usize = 51;
        let samples = generate_random_samples(0, COUNT);
        let mut chunk = GorillaChunk::default();

        for sample in samples.iter() {
            chunk.add_sample(&sample).unwrap();
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

}