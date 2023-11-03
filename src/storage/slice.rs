use crate::common::types::Timestamp;
use crate::storage::utils::{get_timestamp_index, get_timestamp_index_bounds};
use crate::storage::Sample;

#[derive(Debug, Clone)]
pub struct SeriesSlice<'a> {
    pub timestamps: &'a [i64],
    pub values: &'a [f64],
}

impl<'a> SeriesSlice<'a> {
    pub fn new(timestamps: &'a [i64], values: &'a [f64]) -> Self {
        Self { timestamps, values }
    }
    pub fn trim(&mut self, start_ts: Timestamp, end_ts: Timestamp) {
        if let Some((start_idx, end_idx)) = get_timestamp_index_bounds(self.timestamps, start_ts, end_ts) {
            self.timestamps = &self.timestamps[start_idx..end_idx];
            self.values = &self.values[start_idx..end_idx];
        }
    }

    pub fn skip_values_before(&mut self, ts: Timestamp) {
        if let Some(idx) = get_timestamp_index(self.timestamps, ts) {
            self.timestamps = &self.timestamps[idx..];
            self.values = &self.values[idx..];
        }
    }
    pub fn split_at(&self, n: usize) -> (Self, Self) {
        let (timestamps1, timestamps2) = self.timestamps.split_at(n);
        let (values1, values2) = self.values.split_at(n);
        (Self::new(timestamps1, values1), Self::new(timestamps2, values2))
    }

    pub fn split_at_timestamp(&self, ts: Timestamp) -> (Self, Self) {
        let idx = get_timestamp_index(self.timestamps, ts).unwrap_or_else(|| self.timestamps.len());
        self.split_at(idx)
    }

    pub fn take(&self, n: usize) -> Self {
        let (left, _) = self.split_at(n);
        left
    }

    pub fn truncate(&mut self, n: usize) {
        self.timestamps = &self.timestamps[..n];
        self.values = &self.values[..n];
    }
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }
    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }
    pub fn last_timestamp(&self) -> Timestamp {
        self.timestamps[self.timestamps.len() - 1]
    }
    pub fn first_timestamp(&self) -> Timestamp {
        self.timestamps[0]
    }
    pub fn iter(&self) -> SeriesSliceIter {
        SeriesSliceIter {
            slice: self,
            index: 0
        }
    }

    pub fn clear(&mut self) {
        self.timestamps = &[];
        self.values = &[];
    }

}

pub struct SeriesSliceIter<'a> {
    slice: &'a SeriesSlice<'a>,
    index: usize
}

impl<'a> Iterator for SeriesSliceIter<'a> {
    type Item = Sample;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.slice.len() {
            return None;
        }
        let item = Sample::new(self.slice.timestamps[self.index], self.slice.values[self.index]);
        self.index += 1;
        Some(item)
    }
}