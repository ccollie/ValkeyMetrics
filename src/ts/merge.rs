use crate::common::types::{Sample, Timestamp};
use crate::ts::{handle_duplicate_sample, DuplicatePolicy, DuplicateStatus};
use ahash::AHashSet;
use std::cmp::Ordering;

// see: https://github.com/rklaehn/binary-merge/blob/master/src/lib.rs
// todo: implement binary_merge from above

pub(crate) struct DataBlock<'a> {
    pub timestamps: &'a mut Vec<i64>,
    pub values: &'a mut Vec<f64>,
    pub next_idx: usize,
}

impl<'a> DataBlock<'a> {
    pub fn new(timestamps: &'a mut Vec<i64>, values: &'a mut Vec<f64>) -> Self {
        Self {
            timestamps,
            values,
            next_idx: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn max_timestamp(&self) -> i64 {
        self.timestamps[self.timestamps.len() - 1]
    }

    pub fn min_timestamp(&self) -> i64 {
        self.timestamps[0]
    }

    pub fn iter(&self) -> DataBlockIterator {
        DataBlockIterator {
            timestamps: &self.timestamps,
            values: &self.values,
            idx: 0,
        }
    }

    pub fn iter_remaining(&self) -> DataBlockIterator {
        DataBlockIterator {
            timestamps: &self.timestamps[self.next_idx..],
            values: &self.values[self.next_idx..],
            idx: 0,
        }
    }
}

pub(crate) struct DataBlockIterator<'a> {
    timestamps: &'a [i64],
    values: &'a [f64],
    idx: usize,
}

impl<'a> DataBlockIterator<'a> {
    fn is_empty(&self) -> bool {
        self.idx >= self.timestamps.len()
    }

    fn remainders(&self) -> (&[i64], &[f64]) {
        (&self.timestamps[self.idx..], &self.values[self.idx..])
    }
}

impl<'a> Iterator for DataBlockIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.timestamps.len() {
            return None;
        }
        let ts = self.timestamps[self.idx];
        let v = self.values[self.idx];
        self.idx += 1;
        Some(Sample {
            timestamp: ts,
            value: v,
        })
    }
}

// merge_blocks merges ib1 and ib2 to ob.
pub(crate) fn merge_into<'a>(
    dest: &'a mut DataBlock,
    other: &'a mut DataBlock,
    retention_deadline: i64,
    duplicate_policy: DuplicatePolicy,
    duplicates: &mut AHashSet<Timestamp>,
) -> usize {
    let mut rows_deleted = skip_samples_outside_retention(dest, retention_deadline);

    if rows_deleted > 0 {
        dest.timestamps.drain(0..rows_deleted);
        dest.values.drain(0..rows_deleted);
    }

    rows_deleted += skip_samples_outside_retention(other, retention_deadline);

    if dest.max_timestamp() < other.min_timestamp() {
        // Fast path - dest values have smaller timestamps than ib2 values.
        append_slice(dest, other);
        return rows_deleted;
    }
    if other.max_timestamp() < dest.min_timestamp() {
        // Fast path - other values have smaller timestamps than ib1 values.
        // prepend
        dest.timestamps.splice(0..0, other.timestamps.iter().cloned());
        dest.values.splice(0..0, other.values.iter().cloned());
        return rows_deleted;
    }
    if dest.next_idx >= dest.len() {
        append_slice(dest, other);
        return rows_deleted;
    }
    if other.next_idx >= other.len() {
        return rows_deleted;
    }
    let mut a_iter = dest.iter_remaining();
    let mut b_iter = other.iter_remaining();

    let mut ts_result = Vec::with_capacity(dest.len() + other.len());
    let mut v_result = Vec::with_capacity(dest.len() + other.len());

    // do tape merge
    // todo: avoid allocation of ts_result and v_result by using indexes instead of iterators
    loop {
        if let Some(a) = a_iter.next() {
            if let Some(b) = b_iter.next() {
                // something left in both a and b
                match a.timestamp.cmp(&b.timestamp) {
                    Ordering::Less => {
                        ts_result.push(a.timestamp);
                        v_result.push(a.value);
                    }
                    Ordering::Equal => {
                        let mut new_sample = b;
                        let cr = handle_duplicate_sample(duplicate_policy, a, &mut new_sample);
                        if cr != DuplicateStatus::Ok {
                            duplicates.insert(new_sample.timestamp);
                        } else {
                            ts_result.push(new_sample.timestamp);
                            v_result.push(new_sample.value);
                        }
                    }
                    Ordering::Greater => {
                        ts_result.push(b.timestamp);
                        v_result.push(b.value);
                    }
                }
            } else {
                // b is empty, add the rest of a
                if !a_iter.is_empty() {
                    let (timestamps, values) = a_iter.remainders();
                    ts_result.extend_from_slice(timestamps);
                    v_result.extend_from_slice(values);
                }
                break;
            }
        } else {
            // a is empty, add the rest of b
            if !b_iter.is_empty() {
                let (timestamps, values) = b_iter.remainders();
                ts_result.extend_from_slice(timestamps);
                v_result.extend_from_slice(values);
            }
            break;
        }
    }

    dest.timestamps.extend(ts_result);
    dest.values.extend(v_result);

    rows_deleted
}

fn skip_samples_outside_retention(b: &mut DataBlock, retention_deadline: Timestamp) -> usize {
    if b.min_timestamp() >= retention_deadline {
        // Fast path - the block contains only samples with timestamps bigger than retention_deadline.
        return 0;
    }
    let timestamps = &b.timestamps[0..];
    let mut next_idx = b.next_idx;
    let next_idx_orig = next_idx;
    for ts in timestamps.iter() {
        if *ts >= retention_deadline {
            break;
        }
        next_idx += 1;
    }
    let n = next_idx - next_idx_orig;
    if n > 0 {
        b.next_idx = next_idx
    }
    n
}

#[inline]
fn append_slice(ob: &mut DataBlock, ib: &mut DataBlock) {
    ob.timestamps
        .extend_from_slice(&ib.timestamps[ib.next_idx..]);
    ob.values.extend_from_slice(&ib.values[ib.next_idx..]);
}
