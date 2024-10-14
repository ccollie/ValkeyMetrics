use core::mem::size_of;
use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::series::chunks::Chunk;
use crate::series::{DuplicatePolicy, Sample, SAMPLE_SIZE};
use ahash::AHashSet;
use get_size::GetSize;
use std::sync::LazyLock;
use valkey_module::raw;
use crate::iter::SampleSliceIter;
use crate::series::utils::get_sample_index_bounds;

// todo: move to constants
pub const MAX_UNCOMPRESSED_SAMPLES: usize = 256;


static EMPTY_VEC: LazyLock<Vec<Sample>> = LazyLock::new(||vec![]);


#[derive(Clone, Debug, PartialEq)]
pub struct UncompressedChunk {
    pub max_size: usize,
    pub samples: Vec<Sample>,
    max_elements: usize,
}

impl Default for UncompressedChunk {
    fn default() -> Self {
        Self {
            samples: Vec::default(),
            max_size: MAX_UNCOMPRESSED_SAMPLES * SAMPLE_SIZE,
            max_elements: MAX_UNCOMPRESSED_SAMPLES,
        }
    }
}

impl GetSize for UncompressedChunk {
    fn get_size(&self) -> usize {
        size_of::<usize>() +  // self.max_size
        size_of::<usize>() +  // self.max_elements
        self.samples.capacity() * size_of::<Sample>() // todo: add capacity
    }
}

impl UncompressedChunk {
    pub fn new(size: usize, samples: &[Sample]) -> Self {
        let max_elements = size / SAMPLE_SIZE;
        Self {
            samples: samples.to_vec(),
            max_size: size,
            max_elements,
        }
    }

    pub fn with_max_size(size: usize) -> Self {
        Self {
            max_size: size,
            max_elements: size / SAMPLE_SIZE,
            ..Default::default()
        }
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.max_elements
    }

    pub fn clear(&mut self) {
        self.samples.clear();
    }

    pub fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        self.samples = samples.to_vec();
        // todo: complain if size > max_size
        Ok(())
    }

    fn handle_insert(
        &mut self,
        sample: &mut Sample,
        policy: DuplicatePolicy,
    ) -> TsdbResult<()> {
        let ts = sample.timestamp;

        let (idx, found) = self.get_sample_index(ts);
        if found {
            // update value in case timestamp exists
            let current = self.samples.get_mut(idx).unwrap(); // todo: get_mut_unchecked
            current.value = policy.duplicate_value(ts, current.value, sample.value)?;
        } else if idx < self.samples.len() {
            self.samples.insert(idx, *sample);
        } else {
            self.samples.push(*sample);
        }
        Ok(())
    }

    pub fn bytes_per_sample(&self) -> usize {
        SAMPLE_SIZE
    }

    pub fn iter(&self) -> impl Iterator<Item=Sample> + '_ {
        self.samples.iter().cloned()
    }

    pub fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> SampleSliceIter<'_> {
        let slice = self.get_range_slice(start_ts, end_ts);
        SampleSliceIter::new(slice)
    }

    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>>  {
        if self.num_samples() == 0 || timestamps.is_empty() {
            return Ok(vec![]);
        }
        let first_timestamp = timestamps[0];
        if first_timestamp > self.last_timestamp() {
            return Ok(vec![]);
        }

        let mut samples = Vec::with_capacity(timestamps.len());
        for ts in timestamps.iter() {
            let (pos, found) = self.get_sample_index(*ts);
            if found {
                // todo: we know that the index in bounds, so use get_unchecked
                let sample = self.samples.get(pos).unwrap();
                samples.push(*sample);
            }
        }
        Ok(samples)
    }

    pub fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: DuplicatePolicy,
        blocked: &mut AHashSet<Timestamp>
    ) -> TsdbResult<usize> {

        if samples.len() == 1 {
            let mut first = samples[0];
            if self.is_empty() {
                self.add_sample(&first)?;
            } else {
                self.upsert_sample(&mut first, dp_policy)?;
            }
            return Ok(self.num_samples());
        }

        if self.is_empty() {
            self.samples.resize(samples.len(), Sample::default());
            self.samples.extend_from_slice(&samples);
            return Ok(samples.len())
        }

        for sample in samples {
            let ts = sample.timestamp;
            let (pos, found) = self.get_sample_index(ts);
            if found {
                let current = self.samples.get_mut(pos).unwrap(); // todo: get_mut_unchecked
                if let Ok(val) = dp_policy.duplicate_value(ts, current.value, sample.value) {
                    current.value = val;
                } else {
                    blocked.insert(ts);
                }
            } else {
                self.samples.insert(pos, *sample);
            }
        }

        Ok(self.samples.len())
    }

    fn get_sample_index(&self, ts: Timestamp) -> (usize, bool) {
        get_sample_index(&self.samples, ts)
    }

    fn get_range_slice(&self, start_ts: Timestamp, end_ts: Timestamp) -> &[Sample] {
        if let Some((start_idx, end_index)) = get_sample_index_bounds(&self.samples, start_ts, end_ts) {
            &self.samples[start_idx..end_index]
        } else {
            &EMPTY_VEC
        }
    }
}

fn get_sample_index(samples: &Vec<Sample>, ts: Timestamp) -> (usize, bool) {
    match samples.binary_search_by(|x| x.timestamp.cmp(&ts)) {
        Ok(pos) => (pos, true),
        Err(idx) => (idx, false)
    }
}

impl Chunk for UncompressedChunk {
    fn first_timestamp(&self) -> Timestamp {
        if self.samples.is_empty() {
            return 0;
        }
        self.samples[0].timestamp
    }

    fn last_timestamp(&self) -> Timestamp {
        if self.samples.is_empty() {
            return i64::MAX;
        }
        self.samples[self.samples.len() - 1].timestamp
    }

    fn num_samples(&self) -> usize {
        self.samples.len()
    }

    fn last_value(&self) -> f64 {
        if self.samples.is_empty() {
            return f64::MAX;
        }
        self.samples[self.samples.len() - 1].value
    }

    fn size(&self) -> usize {
        self.samples.len() * size_of::<Sample>()
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        self.samples.retain(|sample| -> bool {
            sample.timestamp >= start_ts && sample.timestamp <= end_ts
        });
        Ok(self.samples.len())
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_full() {
            return Err(TsdbError::CapacityFull(MAX_UNCOMPRESSED_SAMPLES));
        }
        self.samples.push(sample.clone());
        Ok(())
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        let slice = self.get_range_slice(start, end).to_vec();
        Ok(slice)
    }

    fn upsert_sample(
        &mut self,
        sample: &mut Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {
        let ts = sample.timestamp;

        let count = self.samples.len();
        if self.is_empty() {
            self.samples.push(*sample);
        } else {
            let last_sample = self.samples[count - 1];
            let last_ts = last_sample.timestamp;
            if ts > last_ts {
                self.samples.push(*sample);
            } else {
                self.handle_insert(sample, dp_policy)?;
            }
        }

        Ok(self.len() - count)
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let half = self.samples.len() / 2;
        let samples = std::mem::take(&mut self.samples);
        let (left, right) = samples.split_at(half);
        self.samples = left.to_vec();

        Ok(Self {
            max_size: self.max_size,
            samples: right.to_vec(),
            max_elements: self.max_elements,
        })
    }

    fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        // todo: compress ?
        raw::save_unsigned(rdb, self.max_size as u64);
        raw::save_unsigned(rdb, self.max_elements as u64);
        raw::save_unsigned(rdb, self.samples.len() as u64);
        for Sample { timestamp, value } in self.samples.iter() {
            raw::save_signed(rdb, *timestamp);
            raw::save_double(rdb, *value);
        }
    }

    fn rdb_load(rdb: *mut raw::RedisModuleIO) -> Result<Self, valkey_module::error::Error> {
        let max_size = raw::load_unsigned(rdb)? as usize;
        let max_elements = raw::load_unsigned(rdb)? as usize;
        let len = raw::load_unsigned(rdb)? as usize;
        let mut samples = Vec::with_capacity(len);
        for _ in 0..len {
            let ts = raw::load_signed(rdb)?;
            let val = raw::load_double(rdb)?;
            samples.push(Sample { timestamp: ts, value: val });
        }
        Ok(Self {
            max_size,
            samples,
            max_elements,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::error::TsdbError;
    use crate::series::chunks::uncompressed::UncompressedChunk;
    use crate::series::{Chunk, Sample};
    use crate::tests::generators::create_rng;
    use rand::Rng;

    pub(crate) fn saturate_uncompressed_chunk(chunk: &mut UncompressedChunk) {
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
}