use crate::common::types::{Sample, Timestamp};
use crate::module::types::ValueFilter;

pub struct SeriesSliceIterator<'a> {
    timestamps: &'a [i64],
    values: &'a [f64],
    index: usize,
    ts_filter: Option<Vec<Timestamp>>,
    values_filter: Option<ValueFilter>
}

impl<'a> SeriesSliceIterator<'a> {
    pub fn new(timestamps: &'a [Timestamp], values: &'a [f64]) -> Self {
        Self {
            timestamps,
            values,
            index: 0,
            ts_filter: None,
            values_filter: None
        }
    }
}

impl<'a> Iterator for SeriesSliceIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.timestamps.len() {
            return None;
        }
        let ts = self.timestamps[self.index];
        let val = self.values[self.index];
        self.index += 1;
        Some(Sample { timestamp: ts, value: val })
    }
}