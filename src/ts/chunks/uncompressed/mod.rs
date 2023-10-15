use serde::{Deserialize, Serialize};
use crate::error::{TsdbError, TsdbResult};
use crate::ts::{DuplicatePolicy, DuplicateStatus, handle_duplicate_sample, Sample, Timestamp};
use crate::ts::chunks::{DataPage, TimesSeriesBlock};
use crate::ts::utils::{get_timestamp_index_bounds, trim_data};

// todo: move to constants
pub const MAX_UNCOMPRESSED_SAMPLES: usize = 256;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct UncompressedChunk {
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>
}

impl UncompressedChunk {
    pub fn new(timestamps: Vec<i64>, values: Vec<f64>) -> Self {
        Self {
            timestamps,
            values,
        }
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.timestamps.len() >= MAX_UNCOMPRESSED_SAMPLES
    }

    pub fn overlaps(&self, start_time: i64, end_time: i64) -> bool {
        let timestamps = &self.timestamps[0..];
        if timestamps.is_empty() {
            return false;
        }
        let first_time = timestamps[0];
        first_time <= end_time && timestamps[timestamps.len() - 1] >= start_time
    }

    /// Get the data points in the specified range (both range_start_time and range_end_time inclusive).
    pub fn get_samples_in_range(
        &self,
        start_time: i64,
        end_time: i64,
    ) -> Vec<Sample> {
        let mut result = Vec::new();

        for (time, val) in self.timestamps.iter().zip(self.values.iter()){
            let time = *time;
            if time > end_time {
                break;
            }
            if time >= start_time && time <= end_time {
                result.push(Sample::new(time, *val));
            }
        }

        result
    }

    fn handle_insert(&mut self, sample: &mut Sample, policy: DuplicatePolicy) -> Result<(), TsdbError> {
        let timestamps = &self.timestamps[0..];
        let ts = sample.timestamp;

        match timestamps.binary_search(&ts) {
            Ok(idx) => {
                // update value in case timestamp exists
                let ts = timestamps[idx];
                let value = self.values[idx];
                let current = Sample {
                    timestamp: ts,
                    value,
                };
                let cr = handle_duplicate_sample(policy, current, sample);
                if cr != DuplicateStatus::Ok {
                    // todo: format ts as iso-8601 or rfc3339
                    let msg = format!("{} @ {}", value, ts);
                    return Err(TsdbError::DuplicateSample(msg));
                }
                self.values[idx] = sample.value;
            }
            Err(idx) => {
                if idx < timestamps.len() {
                    self.timestamps.insert(idx, ts);
                    self.values.insert(idx, sample.value);
                } else {
                    self.timestamps.push(ts);
                    self.values.push(sample.value);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn iterate_range<F, State>(&self, start: Timestamp, end: Timestamp, state: &mut State, mut f: F) -> TsdbResult<()>
        where F: FnMut(&mut State, &[i64], &[f64], bool) -> TsdbResult<bool> {

        let (start_idx, end_idx) = get_timestamp_index_bounds(&self.timestamps, start, end);

        if start_idx <= end_idx {
            // todo(perf): use pool
            let timestamps = &self.timestamps[start_idx .. end_idx];
            let values = &self.values[start_idx .. end_idx];

            f(state, &timestamps, &values, true)?;
        } else {
            let timestamps = vec![];
            let values = vec![];
            f(state, &timestamps, &values, true)?;
        }
        Ok(())
    }

    pub fn iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> impl Iterator<Item=Sample> + '_  {
        SampleIter::new(self, start_ts, end_ts)
    }
}

impl TimesSeriesBlock for UncompressedChunk {

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
        let mut size = std::mem::size_of::<Vec<i64>>() + std::mem::size_of::<Vec<f64>>();
        size += self.timestamps.capacity() * std::mem::size_of::<i64>();
        size += self.values.capacity() * std::mem::size_of::<f64>();
        size
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        let start_idx = self.timestamps.iter()
            .position(|&ts| ts >= start_ts)
            .unwrap_or(self.timestamps.len());

        let end_idx = self.timestamps.iter().rev()
            .position(|&ts| ts <= end_ts)
            .unwrap_or(0);

        if start_idx >= end_idx {
            return Ok(0);
        }

        let _ = self.values.drain(start_idx .. end_idx);
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

    fn upsert_sample(&mut self, sample: &mut Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
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

        return Ok(self.len() - count);
    }

    fn split(&mut self) -> TsdbResult<Self> where Self: Sized {
        let half = self.timestamps.len() / 2;
        let (left_timestamps, right_timestamps) = self.timestamps.split_at(half);
        let (left_values, right_values) = self.values.split_at(half);
        let res = Self::new(right_timestamps.to_vec(), right_values.to_vec());

        let llen = left_timestamps.len();

        self.timestamps.truncate(llen);
        self.values.truncate(llen);

        Ok(res)
    }
}

struct SampleIter<'a> {
    chunk: &'a UncompressedChunk,
    index: usize,
    end_index: usize,
}

impl<'a> SampleIter<'a> {
    pub fn new(chunk: &'a UncompressedChunk, start: Timestamp, end: Timestamp) -> Self {
        let (start_index, end_index) = get_timestamp_index_bounds(&chunk.timestamps, start, end);
        Self {
            chunk,
            index: start_index,
            end_index,
        }
    }
}

impl<'a> Iterator for SampleIter<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.end_index {
            return None;
        }
        let ts = self.chunk.timestamps[self.index];
        let val = self.chunk.values[self.index];
        self.index += 1;
        Some(Sample::new(ts, val))
    }
}