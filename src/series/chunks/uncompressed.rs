use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::module::types::ValueFilter;
use crate::series::chunks::Chunk;
use crate::series::slice_iter::SeriesSliceIterator;
use crate::series::utils::get_timestamp_index_bounds;
use crate::series::{DuplicatePolicy, Sample, SAMPLE_SIZE};
use get_size::GetSize;
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use valkey_module::raw;

// todo: move to constants
pub const MAX_UNCOMPRESSED_SAMPLES: usize = 256;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[derive(GetSize)]
pub struct UncompressedChunk {
    pub max_size: usize,
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
    max_elements: usize,
}

impl Default for UncompressedChunk {
    fn default() -> Self {
        Self {
            max_size: MAX_UNCOMPRESSED_SAMPLES * SAMPLE_SIZE,
            timestamps: Vec::with_capacity(MAX_UNCOMPRESSED_SAMPLES),
            values: Vec::with_capacity(MAX_UNCOMPRESSED_SAMPLES),
            max_elements: MAX_UNCOMPRESSED_SAMPLES,
        }
    }
}

impl UncompressedChunk {
    pub fn new(size: usize, timestamps: Vec<i64>, values: Vec<f64>) -> Self {
        let max_elements = size / SAMPLE_SIZE;
        Self {
            timestamps,
            values,
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
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.max_elements
    }

    pub fn clear(&mut self) {
        self.timestamps.clear();
        self.values.clear();
    }

    pub fn set_data(&mut self, timestamps: &[i64], values: &[f64]) -> TsdbResult<()> {
        debug_assert_eq!(timestamps.len(), values.len());
        self.timestamps.clear();
        self.timestamps.extend_from_slice(timestamps);
        self.values.clear();
        self.values.extend_from_slice(values);
        // todo: complain if size > max_size
        Ok(())
    }

    fn handle_insert(
        &mut self,
        sample: &mut Sample,
        policy: DuplicatePolicy,
    ) -> TsdbResult<()> {
        let timestamps = &self.timestamps[0..];
        let ts = sample.timestamp;

        let (idx, found) = self.find_timestamp_index(ts);
        if found {
            // update value in case timestamp exists
            let ts = timestamps[idx];
            let value = self.values[idx];
            self.values[idx] = policy.value_on_duplicate(ts, value, sample.value)?;
        } else if idx < timestamps.len() {
            self.timestamps.insert(idx, ts);
            self.values.insert(idx, sample.value);
        } else {
            self.timestamps.push(ts);
            self.values.push(sample.value);
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
        if let Some((start_idx, end_idx)) = get_timestamp_index_bounds(&self.timestamps, start, end) {
            let timestamps = &self.timestamps[start_idx..end_idx];
            let values = &self.values[start_idx..end_idx];

            return f(state, timestamps, values)
        }

        let timestamps = vec![];
        let values = vec![];
        f(state, &timestamps, &values)
    }

    pub fn bytes_per_sample(&self) -> usize {
        SAMPLE_SIZE
    }

    fn find_timestamp_index(&self, ts: Timestamp) -> (usize, bool) {
        if self.len() > 128 {
            match self.timestamps.binary_search(&ts) {
                Ok(idx) => (idx, true),
                Err(idx) => (idx, false),
            }
        } else {
            match self.timestamps.iter().position(|&t| t == ts) {
                Some(idx) => (idx, true),
                None => (self.len(), false),
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item=Sample> + '_ {
        SeriesSliceIterator::new(&self.timestamps, &self.values)
    }

    pub fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> impl Iterator<Item=Sample> + '_ {
        let (first, _) = self.find_timestamp_index(start_ts);
        let (last, _) = self.find_timestamp_index(end_ts);
        let ts_slice = &self.timestamps[first..last];
        let values = &self.values[first..last];
        SeriesSliceIterator::new(ts_slice, values)
    }

    // this is unnecessarily complex.
    pub fn filtered_iter<'a>(&'a self,
                       start_ts: Timestamp,
                       end_ts: Timestamp,
                       ts_filter: &Option<Vec<Timestamp>>,
                       values_filter: &'a Option<ValueFilter>
                    ) -> Box<dyn Iterator<Item=Sample> + 'a> {

        match (ts_filter, values_filter) {
            (Some(timestamps), Some(values_filter)) => {
                Box::new(self.samples_by_timestamps(timestamps)
                    .unwrap_or_default() // todo: raise error
                    .into_iter()
                    .filter(move |sample: &Sample| -> bool {
                        sample.timestamp <= end_ts && sample.timestamp >= start_ts &&
                        sample.value >= values_filter.min && sample.value <= values_filter.max
                    }))
            }
            (Some(timestamps), None) => {
                Box::new(self.samples_by_timestamps(timestamps)
                    .unwrap_or_default() // todo: raise error
                    .into_iter()
                    .filter(move |sample: &Sample| -> bool {
                        sample.timestamp <= end_ts && sample.timestamp >= start_ts
                    }))
            }
            (None, Some(values_filter)) => {
                Box::new(self.timestamps.iter()
                    .zip(self.values.iter())
                    .filter(move |(timestamp, value)| -> bool {
                        **timestamp >= start_ts && **timestamp <= end_ts &&
                            **value >= values_filter.min && **value <= values_filter.max
                    })
                    .map(|(timestamp, value )| Sample { timestamp: *timestamp, value: *value }))
            }
            (None, None) => {
                Box::new(self.timestamps.iter()
                    .zip(self.values.iter())
                    .filter(move |(timestamp, _)| -> bool {
                        **timestamp >= start_ts && **timestamp <= end_ts
                    })
                    .map(|(timestamp, value )| Sample { timestamp: *timestamp, value: *value })
                )
            }
        }
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
            if let Ok(i) = timestamps.binary_search(ts) {
                samples.push(Sample {
                    timestamp: *ts,
                    value: self.values[i],
                })
            }
        }
        Ok(samples)
    }
}

impl Chunk for UncompressedChunk {
    fn first_timestamp(&self) -> Timestamp {
        if self.timestamps.is_empty() {
            return 0;
        }
        self.timestamps[0]
    }

    fn last_timestamp(&self) -> Timestamp {
        if self.timestamps.is_empty() {
            return i64::MAX;
        }
        self.timestamps[self.timestamps.len() - 1]
    }

    fn num_samples(&self) -> usize {
        self.timestamps.len()
    }

    fn last_value(&self) -> f64 {
        if self.values.is_empty() {
            return f64::MAX;
        }
        self.values[self.values.len() - 1]
    }

    fn size(&self) -> usize {
        let mut size = size_of::<Vec<Self>>() + size_of::<Vec<f64>>();
        size += self.timestamps.capacity() * size_of::<i64>();
        size += self.values.capacity() * size_of::<f64>();
        size
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        let start_idx = self.timestamps.binary_search(&start_ts).unwrap_or_else(|i| i);

        let end_idx = self
            .timestamps
            .iter()
            .rev()
            .position(|&ts| ts <= end_ts)
            .unwrap_or(0);

        if start_idx >= end_idx {
            return Ok(0);
        }

        let _ = self.values.drain(start_idx..end_idx);
        let iter = self.timestamps.drain(start_idx..end_idx);
        Ok(iter.count())
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_full() {
            return Err(TsdbError::CapacityFull(MAX_UNCOMPRESSED_SAMPLES));
        }
        self.timestamps.push(sample.timestamp);
        self.values.push(sample.value);
        Ok(())
    }

    fn get_range(
        &self,
        start: Timestamp,
        end: Timestamp,
        timestamps: &mut Vec<i64>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        let bounds = get_timestamp_index_bounds(&self.timestamps, start, end);
        if bounds.is_none() {
            return Ok(());
        }
        let (start_idx, end_index) = bounds.unwrap();
        let src_timestamps = &self.timestamps[start_idx..end_index];
        let src_values = &self.values[start_idx..end_index];
        let len = src_timestamps.len();
        timestamps.reserve(len);
        values.reserve(len);
        timestamps.extend_from_slice(src_timestamps);
        values.extend_from_slice(src_values);

        Ok(())
    }

    fn upsert_sample(
        &mut self,
        sample: &mut Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {
        let ts = sample.timestamp;

        let count = self.timestamps.len();
        if self.is_empty() {
            self.timestamps.push(ts);
            self.values.push(sample.value);
        } else {
            let last_ts = self.timestamps[self.timestamps.len() - 1];
            if ts > last_ts {
                self.timestamps.push(ts);
                self.values.push(sample.value);
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
        let half = self.timestamps.len() / 2;
        let new_timestamps = self.timestamps.split_off(half);
        let new_values = self.values.split_off(half);

        let res = Self::new(self.max_size, new_timestamps, new_values);
        Ok(res)
    }

    fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        // todo: compress ?
        raw::save_unsigned(rdb, self.max_size as u64);
        raw::save_unsigned(rdb, self.max_elements as u64);
        raw::save_unsigned(rdb, self.timestamps.len() as u64);
        for (ts, val) in self.timestamps.iter().zip(self.values.iter()) {
            raw::save_signed(rdb, *ts);
            raw::save_double(rdb, *val);
        }
    }

    fn rdb_load(rdb: *mut raw::RedisModuleIO) -> Result<Self, valkey_module::error::Error> {
        let max_size = raw::load_unsigned(rdb)? as usize;
        let max_elements = raw::load_unsigned(rdb)? as usize;
        let len = raw::load_unsigned(rdb)? as usize;
        let mut timestamps = Vec::with_capacity(len);
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            let ts = raw::load_signed(rdb)?;
            let val = raw::load_double(rdb)?;
            timestamps.push(ts);
            values.push(val);
        }
        Ok(Self {
            max_size,
            timestamps,
            values,
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