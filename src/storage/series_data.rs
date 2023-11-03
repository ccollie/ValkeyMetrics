use serde::{Deserialize, Serialize};
use crate::error::TsdbResult;
use crate::storage::{DuplicatePolicy, Sample};

// Time series measurements.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SeriesData {
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
}

impl SeriesData {
    pub fn new(n: usize) -> Self {
        Self {
            timestamps: Vec::with_capacity(n),
            values: Vec::with_capacity(n),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            timestamps: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
        }
    }

    pub fn new_with_data(timestamps: Vec<i64>, values: Vec<f64>) -> Self {
        Self { timestamps, values }
    }
    pub fn push(&mut self, ts: i64, value: f64, policy: Option<DuplicatePolicy>) -> TsdbResult<usize> {
        match self.timestamps.binary_search(&ts) {
            Ok(pos) => {
                let dp = policy.unwrap_or(DuplicatePolicy::KeepLast);
                self.values[pos] = dp.value_on_duplicate(ts, self.values[pos], value)?;
                Ok(0)
            }
            Err(idx) => {
                self.timestamps.insert(idx, ts);
                self.values.insert(idx, value);
                Ok(1)
            }
        }
    }

    pub fn push_sample(&mut self, sample: &Sample, policy: Option<DuplicatePolicy>) -> TsdbResult<usize>{
        self.push(sample.timestamp, sample.value, policy)
    }

    pub fn truncate(&mut self, len: usize) {
        self.timestamps.truncate(len);
        self.values.truncate(len);
    }

    pub fn clear(&mut self) {
        self.timestamps.clear();
        self.values.clear();
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }
    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }
    pub fn first_timestamp(&self) -> i64 {
        self.timestamps[0]
    }
    pub fn last_timestamp(&self) -> i64 {
        self.timestamps[self.timestamps.len() - 1]
    }
    pub fn iter(&self) -> SeriesDataIter {
        SeriesDataIter::new(self)
    }
}

pub(crate) struct SeriesDataIter<'a> {
    series: &'a SeriesData,
    idx: usize,
}
impl<'a> SeriesDataIter<'a> {
    pub fn new(series: &'a SeriesData) -> Self {
        Self { series, idx: 0 }
    }
}

impl Iterator for SeriesDataIter<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.series.timestamps.len() {
            return None;
        }
        let res = Some(Sample {
            timestamp: self.series.timestamps[self.idx],
            value: self.series.values[self.idx],
        });
        self.idx += 1;
        res
    }
}