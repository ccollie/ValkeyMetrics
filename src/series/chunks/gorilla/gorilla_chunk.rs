use super::{GorillaEncoder, GorillaIterator};
use crate::common::current_time_millis;
use crate::common::types::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::iterators::SampleIter;
use crate::series::chunks::chunk::Chunk;
use crate::series::merge::merge_samples;
use crate::series::{DuplicatePolicy, SampleAddResult, SERIES_SETTINGS};
use get_size::GetSize;
use std::cmp::Ordering;
use std::mem::size_of;

/// `GorillaChunk` is a chunk of timeseries data encoded using Gorilla XOR encoding.
#[derive(Debug, Clone, PartialEq, GetSize)]
pub struct GorillaChunk {
    pub(crate) xor_encoder: GorillaEncoder,
    pub(crate) first_timestamp: Timestamp,
    pub max_size: usize,
}

impl Default for GorillaChunk {
    fn default() -> Self {
        Self::with_max_size(SERIES_SETTINGS.chunk_size_bytes)
    }
}

impl GorillaChunk {
    pub fn with_max_size(max_size: usize) -> Self {
        let now = current_time_millis();
        Self {
            xor_encoder: GorillaEncoder::new(),
            first_timestamp: now,
            max_size,
        }
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
        let mut encoder = GorillaEncoder::new();
        for sample in samples {
            push_sample(&mut encoder, sample)?;
        }
        self.xor_encoder = encoder;
        Ok(())
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = self.xor_encoder.buf().len();
        let uncompressed_size = self.len() * (size_of::<i64>() + size_of::<f64>());
        (uncompressed_size / compressed_size) as f64
    }

    pub fn data_size(&self) -> usize {
        self.xor_encoder.get_size()
    }

    pub fn bytes_per_sample(&self) -> usize {
        let mut count = self.len();
        if count == 0 {
            // estimate 50%
            count = 2;
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
        if self.len() == 0 {
            return 0;
        }
        self.remaining_capacity() / self.bytes_per_sample()
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.get_heap_size()
    }

    fn buf(&self) -> &[u8] {
        self.xor_encoder.buf()
    }

    pub fn iter(&self) -> SampleIter {
        self.range_iter(i64::MIN, i64::MAX)
    }

    pub fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> SampleIter {
        GorillaChunkIterator::new(self, start_ts, end_ts).into()
    }

    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>> {
        if self.len() == 0 || timestamps.is_empty() {
            return Ok(vec![]);
        }
        let mut samples = Vec::with_capacity(timestamps.len());
        let mut timestamps = timestamps;

        let mut first_ts = timestamps[0];

        let first_timestamp = first_ts.max(self.first_timestamp);
        let last_timestamp = timestamps[timestamps.len() - 1].min(self.last_timestamp());

        for sample in self.range_iter(first_timestamp, last_timestamp) {
            match sample.timestamp.cmp(&first_ts) {
                Ordering::Less => continue,
                Ordering::Equal => {
                    timestamps = &timestamps[1..];
                    samples.push(sample);
                    if timestamps.is_empty() {
                        break;
                    }
                    first_ts = timestamps[0];
                }
                Ordering::Greater => {
                    timestamps = &timestamps[1..];
                    if timestamps.is_empty() {
                        break;
                    }
                    first_ts = timestamps[0];
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
    fn len(&self) -> usize {
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
        let mut new_encoder = GorillaEncoder::new();

        for value in self.xor_encoder.iter() {
            let sample = value?;
            if sample.timestamp < start_ts || sample.timestamp > end_ts {
                push_sample(&mut new_encoder, &sample)?;
            }
        }

        self.xor_encoder = new_encoder;
        let new_count = self.len();

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

        let samples = self.range_iter(start, end).collect();
        Ok(samples)
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;

        if self.is_empty() {
            self.add_sample(&sample)?;
            return Ok(1);
        }

        let count = self.len();
        let mut xor_encoder = GorillaEncoder::new();

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
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        fn add_sample(
            chunk: &mut GorillaChunk,
            sample: &Sample,
            res: &mut Vec<SampleAddResult>,
        ) -> TsdbResult<()> {
            match chunk.add_sample(sample) {
                Ok(_) => {
                    res.push(SampleAddResult::Ok(sample.timestamp));
                    Ok(())
                }
                err @ Err(TsdbError::CapacityFull(_)) => Err(err.unwrap_err()),
                Err(_e) => {
                    // todo: log error
                    res.push(SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE));
                    Ok(())
                }
            }
        }

        let mut result = Vec::with_capacity(samples.len());

        // we assume that samples are sorted. Try to optimize by seeing if all samples are past the
        // current chunk's last timestamp.
        let first = samples[0];
        if self.is_empty() || first.timestamp > self.last_timestamp() {
            // set_data
            for sample in samples.iter() {
                add_sample(self, sample, &mut result)?;
            }
            return Ok(result);
        }

        struct MergeState {
            count: usize,
            xor_encoder: GorillaEncoder,
            result: Vec<SampleAddResult>,
        }

        let mut merge_state = MergeState {
            count: 0,
            xor_encoder: GorillaEncoder::new(),
            result: Vec::with_capacity(samples.len()),
        };

        let left = SampleIter::Slice(samples.iter());
        let right = self.iter();

        merge_samples(
            left,
            right,
            dp_policy,
            &mut merge_state,
            |state, sample, is_duplicate| {
                if !is_duplicate {
                    state.count += 1;
                    push_sample(&mut state.xor_encoder, &sample)?;
                    state.result.push(SampleAddResult::Ok(sample.timestamp));
                } else {
                    state.result.push(SampleAddResult::Duplicate);
                }
                Ok(())
            },
        )?;

        self.xor_encoder = merge_state.xor_encoder;
        Ok(merge_state.result)
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let mut left_chunk = GorillaEncoder::new();
        let mut right_chunk = GorillaChunk::default();

        if self.is_empty() {
            return Ok(self.clone());
        }

        let mid = self.len() / 2;
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
}

fn push_sample(encoder: &mut GorillaEncoder, sample: &Sample) -> TsdbResult<()> {
    encoder.add_sample(sample).map_err(|e| {
        println!("Error adding sample: {:?}", e);
        TsdbError::CannotAddSample(*sample)
    })
}

pub(crate) struct ChunkIter<'a> {
    inner: GorillaIterator<'a>,
}

impl<'a> ChunkIter<'a> {
    pub fn new(chunk: &'a GorillaChunk) -> Self {
        let inner = GorillaIterator::new(&chunk.xor_encoder);
        Self { inner }
    }
}

impl Iterator for ChunkIter<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(sample)) => Some(sample),
            Some(Err(_err)) => {
                #[cfg(debug_assertions)]
                eprintln!("Error decoding sample: {:?}", _err);
                None
            }
            None => None,
        }
    }
}

pub struct GorillaChunkIterator<'a> {
    inner: GorillaIterator<'a>,
    start: Timestamp,
    end: Timestamp,
    init: bool,
}

impl<'a> GorillaChunkIterator<'a> {
    pub fn new(chunk: &'a GorillaChunk, start: Timestamp, end: Timestamp) -> Self {
        let inner = GorillaIterator::new(&chunk.xor_encoder);
        Self {
            inner,
            start,
            end,
            init: false,
        }
    }

    fn next_internal(&mut self) -> Option<Sample> {
        match self.inner.next() {
            Some(Ok(sample)) => {
                if sample.timestamp > self.end {
                    return None;
                }
                Some(sample)
            }
            Some(Err(err)) => {
                #[cfg(debug_assertions)]
                eprintln!("Error decoding sample: {:?}", err);
                None
            }
            None => None,
        }
    }
}

impl Iterator for GorillaChunkIterator<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.init {
            self.init = true;

            while let Some(sample) = self.next_internal() {
                if sample.timestamp < self.start {
                    continue;
                }
                if sample.timestamp <= self.end {
                    return Some(sample);   
                }
            }

            return None;
        }
        self.next_internal()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::types::Sample;
    use crate::error::TsdbError;
    use crate::series::chunks::chunk::Chunk;
    use crate::series::chunks::gorilla::gorilla_chunk::GorillaChunk;
    use crate::series::test_utils::generate_random_samples;
    use crate::series::DuplicatePolicy;
    use crate::tests::generators::GeneratorOptions;

    fn decompress(chunk: &GorillaChunk) -> Vec<Sample> {
        chunk.iter().collect()
    }

    fn compare_chunks(chunk1: &GorillaChunk, chunk2: &GorillaChunk) {
        assert_eq!(
            chunk1.xor_encoder, chunk2.xor_encoder,
            "xor chunks do not match"
        );
        assert_eq!(chunk1.max_size, chunk2.max_size);
    }

    #[test]
    fn test_chunk_compress() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let options = GeneratorOptions::default();
        //    options.significant_digits = Some(8);
        let data = generate_random_samples(0, 1000);

        for sample in data.iter() {
            chunk.add_sample(sample).unwrap();
        }
        assert_eq!(chunk.len(), data.len());
        assert_eq!(chunk.first_timestamp(), data[0].timestamp);
        assert_eq!(chunk.last_timestamp(), data[data.len() - 1].timestamp);
        assert_eq!(chunk.last_value(), data[data.len() - 1].value);
    }

    #[test]
    fn test_clear() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let data = generate_random_samples(0, 500);

        for datum in data.iter() {
            chunk.add_sample(datum).unwrap();
        }

        assert_eq!(chunk.len(), data.len());
        chunk.clear();
        assert_eq!(chunk.len(), 0);
        assert_eq!(chunk.first_timestamp(), 0);
        assert_eq!(chunk.last_timestamp(), 0);
    }

    #[test]
    fn test_upsert() {
        for chunk_size in (64..8192).step_by(64) {
            const SAMPLE_COUNT: usize = 200;
            let samples = generate_random_samples(0, SAMPLE_COUNT);
            let mut chunk = GorillaChunk::with_max_size(chunk_size);

            let sample_count = samples.len();
            for sample in samples.into_iter() {
                chunk
                    .upsert_sample(sample, DuplicatePolicy::KeepLast)
                    .unwrap();
            }
            assert_eq!(chunk.len(), sample_count);
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
                value,
            };
            ts += 1000;
            value *= 2.0;

            if let Err(e) = chunk.add_sample(&sample) {
                if let TsdbError::CapacityFull(_) = e {
                    break;
                } else {
                    panic!("unexpected error: {:?}", e);
                }
            }
        }

        let timestamp = chunk.last_timestamp();

        // return an error on insert
        let mut sample = Sample {
            timestamp: 0,
            value: 1.0,
        };

        assert!(chunk
            .upsert_sample(sample, DuplicatePolicy::KeepLast)
            .is_err());

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
            chunk.add_sample(sample).unwrap();
        }

        let count = samples.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.len(), mid);
        assert_eq!(right.len(), mid);

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
            chunk.add_sample(sample).unwrap();
        }

        let count = samples.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.len(), mid);
        assert_eq!(right.len(), mid + 1);

        let (left_samples, right_samples) = samples.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed, right_samples);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed, left_samples);
    }

    #[test]
    fn test_iter() {
        let mut chunk = GorillaChunk::default();
        let options = GeneratorOptions::default();
        let data = generate_random_samples(0, 1000);

        chunk.set_data(&data).unwrap();

        let actual: Vec<_> = chunk.iter().collect();
        assert_eq!(actual, data);
    }

    #[test]
    fn test_remove_range() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let samples = generate_random_samples(0, 100);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        // Remove a range that covers the first half of the samples
        let start_ts = samples[0].timestamp;
        let mid_ts = samples[samples.len() / 2].timestamp;
        let removed_count = chunk.remove_range(start_ts, mid_ts).unwrap();
        assert_eq!(removed_count, samples.len() / 2);

        // Ensure the remaining samples are correct
        let remaining_samples: Vec<_> = chunk.iter().collect();
        let expected_samples = &samples[samples.len() / 2..];
        assert_eq!(remaining_samples, expected_samples);

        // Remove a range that covers the remaining samples
        let end_ts = samples[samples.len() - 1].timestamp;
        let removed_count = chunk.remove_range(mid_ts, end_ts).unwrap();
        assert_eq!(removed_count, samples.len() / 2);

        // Ensure the chunk is empty
        assert!(chunk.is_empty());
    }

    #[test]
    fn test_remove_range_no_overlap() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let samples = generate_random_samples(0, 100);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        // Attempt to remove a range that does not overlap with any samples
        let start_ts = samples[samples.len() - 1].timestamp + 1;
        let end_ts = start_ts + 1000;
        let removed_count = chunk.remove_range(start_ts, end_ts).unwrap();
        assert_eq!(removed_count, 0);

        // Ensure all samples are still present
        let remaining_samples: Vec<_> = chunk.iter().collect();
        assert_eq!(remaining_samples, samples);
    }

    #[test]
    fn test_samples_by_timestamps() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let samples = generate_random_samples(0, 100);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        // Test with a subset of timestamps
        let timestamps: Vec<_> = samples.iter().map(|s| s.timestamp).collect();
        let selected_timestamps = &timestamps[10..20];
        let expected_samples: Vec<_> = samples[10..20].to_vec();

        let result_samples = chunk.samples_by_timestamps(selected_timestamps).unwrap();
        assert_eq!(result_samples, expected_samples);

        // Test with timestamps that are not present
        let missing_timestamps = vec![2000, 3000, 4000];
        let result_samples = chunk.samples_by_timestamps(&missing_timestamps).unwrap();
        assert!(result_samples.is_empty());

        // Test with an empty timestamp list
        let result_samples = chunk.samples_by_timestamps(&[]).unwrap();
        assert!(result_samples.is_empty());
    }

    #[test]
    fn test_samples_by_timestamps_partial_overlap() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let samples = generate_random_samples(0, 100);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        // Test with a mix of present and absent timestamps
        let timestamps = vec![
            samples[5].timestamp,
            2000, // not present
            samples[15].timestamp,
        ];
        let expected_samples = vec![samples[5].clone(), samples[15].clone()];

        let result_samples = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(result_samples, expected_samples);
    }
}
