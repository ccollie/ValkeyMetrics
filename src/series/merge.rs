use crate::common::types::{Timestamp};
use crate::series::{DuplicatePolicy, Sample, SeriesSlice};
use ahash::AHashSet;
use binary_merge::{MergeOperation, MergeState};
use std::cmp::Ordering;

#[derive(Debug)]
struct MergeSource<'a> {
    data: SeriesSlice<'a>,
    index: usize,
}

impl<'a> MergeSource<'a> {
    pub fn new(timestamps: &'a [i64], values: &'a [f64]) -> Self {
        Self {
            data: SeriesSlice::new(timestamps, values),
            index: 0,
        }
    }
    pub fn len(&self) -> usize {
        self.data.len()
    }
    fn timestamp_slice(&self) -> &[i64] {
        &self.data.timestamps[self.index..]
    }
    fn next(&mut self) -> Option<Sample> {
        if self.index >= self.len() {
            return None;
        }
        let sample = Sample {
            timestamp: self.data.timestamps[self.index],
            value: self.data.values[self.index],
        };
        self.index += 1;
        Some(sample)
    }
    fn take(&mut self, n: usize) -> SeriesSlice {
        let end_index = self.index + n;

        let slice = SeriesSlice {
            timestamps: &self.data.timestamps[self.index..end_index],
            values: &self.data.values[self.index..end_index],
        };

        self.index += n;
        slice
    }
}

struct SeriesMergeState<'a> {
    a: MergeSource<'a>,
    b: MergeSource<'a>,
    dest_timestamps: &'a mut Vec<i64>,
    dest_values: &'a mut Vec<f64>,
    duplicates: &'a mut AHashSet<i64>,
}

impl<'a> SeriesMergeState<'a> {
    fn new(
        dest_timestamps: &'a mut Vec<i64>,
        dest_values: &'a mut Vec<f64>,
        left: MergeSource<'a>,
        right: MergeSource<'a>,
        duplicates: &'a mut AHashSet<i64>,
    ) -> Self {
        Self {
            dest_timestamps,
            dest_values,
            a: left,
            b: right,
            duplicates,
        }
    }

    fn take_from_a(&mut self, n: usize) {
        let slice = self.a.take(n);
        self.dest_timestamps.extend_from_slice(slice.timestamps);
        self.dest_values.extend_from_slice(slice.values);
    }

    fn take_from_b(&mut self, n: usize) {
        let slice = self.b.take(n);
        self.dest_timestamps.extend_from_slice(slice.timestamps);
        self.dest_values.extend_from_slice(slice.values);
    }

    fn add_sample(&mut self, sample: Sample) {
        self.dest_timestamps.push(sample.timestamp);
        self.dest_values.push(sample.value);
    }
}

impl<'a> MergeState for SeriesMergeState<'a> {
    type A = i64;
    type B = i64;
    fn a_slice(&self) -> &[Self::A] {
        self.a.timestamp_slice()
    }
    fn b_slice(&self) -> &[Self::B] {
        self.b.timestamp_slice()
    }
}

struct SeriesMerger {
    pub duplicate_policy: DuplicatePolicy,
}

impl SeriesMerger {
    pub fn new(duplicate_policy: DuplicatePolicy) -> Self {
        Self { duplicate_policy }
    }
}

impl<'a> MergeOperation<SeriesMergeState<'a>> for SeriesMerger {
    fn from_a(&self, m: &mut SeriesMergeState<'a>, n: usize) -> bool {
        m.take_from_a(n);
        true
    }

    fn from_b(&self, m: &mut SeriesMergeState<'a>, n: usize) -> bool {
        m.take_from_b(n);
        true
    }
    fn collision(&self, m: &mut SeriesMergeState<'a>) -> bool {
        let mut sample = m.a.next().unwrap();
        let old = m.b.next().unwrap();
        match self.duplicate_policy.value_on_duplicate(old.timestamp, old.value, sample.value) {
            Ok(value) => {
                sample.value = value;
                m.add_sample(sample);
            }
            Err(_) => {
                m.duplicates.insert(sample.timestamp);
            }
        }
        true
    }

    fn cmp(
        &self,
        a: &<SeriesMergeState<'a> as MergeState>::A,
        b: &<SeriesMergeState<'a> as MergeState>::B,
    ) -> Ordering {
        a.cmp(b)
    }
}

/// merges time series slices.
pub(crate) fn merge<'a>(
    dest_timestamps: &'a mut Vec<i64>,
    dest_values: &'a mut Vec<f64>,
    left: SeriesSlice<'a>,
    right: SeriesSlice<'a>,
    retention_deadline: i64,
    duplicate_policy: DuplicatePolicy,
    duplicates: &'a mut AHashSet<Timestamp>,
) -> usize {
    let mut left_source = MergeSource::new(left.timestamps, left.values);
    let mut right_source = MergeSource::new(right.timestamps, right.values);

    let mut rows_deleted = skip_samples_outside_retention(&mut left_source, retention_deadline);

    rows_deleted += skip_samples_outside_retention(&mut right_source, retention_deadline);

    let mut state = SeriesMergeState::new(
        dest_timestamps,
        dest_values,
        left_source,
        right_source,
        duplicates,
    );

    let merger = SeriesMerger::new(duplicate_policy);
    merger.merge(&mut state);
    rows_deleted
}

fn skip_samples_outside_retention(b: &mut MergeSource, retention_deadline: Timestamp) -> usize {
    if b.data.is_empty() {
        return 0;
    }
    if b.data.first_timestamp() >= retention_deadline {
        // Fast path - the block contains only samples with timestamps bigger than retention_deadline.
        return 0;
    }
    let timestamps = &b.data.timestamps[0..];
    let mut next_idx = b.index;
    let next_idx_orig = next_idx;
    for ts in timestamps.iter() {
        if *ts >= retention_deadline {
            break;
        }
        next_idx += 1;
    }
    let n = next_idx - next_idx_orig;
    if n > 0 {
        b.index = next_idx
    }
    n
}
