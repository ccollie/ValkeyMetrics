use crate::ts::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PageMetadata {
    pub offset: usize,
    pub count: usize,
    pub t_min: i64,
    pub t_max: i64,
}

impl PageMetadata {
    pub fn size() -> usize {
        // offset
        std::mem::size_of::<usize>() +
            // count
            std::mem::size_of::<usize>() +
            // t_min
            std::mem::size_of::<i64>() +
            // t_max
            std::mem::size_of::<i64>()
    }

    pub fn overlaps(&self, start: Timestamp, end: Timestamp) -> bool {
        start <= self.t_max && end >= self.t_min
    }

    pub fn contains_timestamp(&self, ts: Timestamp) -> bool {
        ts >= self.t_min && ts <= self.t_max
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompressedData {
    pub data: Vec<u8>,
    pub pages: Vec<PageMetadata>,
    pub count: usize,
    pub last_value: f64,
}

impl CompressedData {
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn overlaps(&self, start: i64, end: i64) -> bool {
        if let Some((first, last)) = self.get_timestamp_range() {
            return start <= last && end >= first;
        }
        false
    }
    pub fn get_timestamp_range(&self) -> Option<(i64, i64)> {
        if self.is_empty() {
            return None;
        }
        let first = self.pages[0].t_min;
        let last = self.pages[self.pages.len() - 1].t_max;
        Some((first, last))
    }

    pub fn first_timestamp(&self) -> Timestamp {
        if self.is_empty() {
            return i64::MAX;
        }
        self.pages[0].t_min
    }
    pub fn last_timestamp(&self) -> Timestamp {
        if self.is_empty() {
            return i64::MAX;
        }
        self.pages[self.pages.len() - 1].t_max
    }
    pub fn last_value(&self) -> f64 {
        self.last_value
    }
    pub fn num_samples(&self) -> usize {
        self.count
    }
    pub fn size(&self) -> usize {
        // data
        self.data.len() +
            // pages
            self.pages.len() * PageMetadata::size() +
            // count
            std::mem::size_of::<usize>() +
            // max_points_per_page
            std::mem::size_of::<usize>() +
            // last_value
            std::mem::size_of::<f64>()
    }
}

impl Eq for CompressedData {}
