use crate::common::types::Timestamp;
use crate::series::utils::get_timestamp_index;
use crate::series::Sample;

#[derive(Debug, Clone)]
pub struct SeriesSlice<'a> {
    pub timestamps: &'a [i64],
    pub values: &'a [f64],
}

impl<'a> SeriesSlice<'a> {
    pub fn new(timestamps: &'a [i64], values: &'a [f64]) -> Self {
        Self { timestamps, values }
    }

    pub fn split_at(&self, n: usize) -> (Self, Self) {
        let (timestamps1, timestamps2) = self.timestamps.split_at(n);
        let (values1, values2) = self.values.split_at(n);
        (Self::new(timestamps1, values1), Self::new(timestamps2, values2))
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
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