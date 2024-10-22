use crate::common::types::{Sample, Timestamp};
use crate::series::DuplicatePolicy;
use std::collections::BTreeSet;


pub fn merge_samples<F,STATE>(
    left: impl Iterator<Item=Sample>,
    right: impl Iterator<Item=Sample>,
    dp_policy: DuplicatePolicy,
    mut f: F,
    state: &mut STATE
)
where F: FnMut(&mut STATE, Sample, bool) -> () {
    let mut left_iter = left.peekable();
    let mut right_iter = right.peekable();

    if left_iter.peek().is_none() {
        for sample in right_iter {
            f(state, sample, false);
        }
        return;
    } else if right_iter.peek().is_none() {
        for sample in left_iter {
            f(state, sample, false);
        }
        return;
    }

    loop {
        let merged = match (left_iter.peek(), right_iter.peek()) {
            (Some(&l), Some(&r)) => {
                if l.timestamp == r.timestamp {
                    let ts = l.timestamp;
                    if let Ok(val) = dp_policy.duplicate_value(ts, r.value, l.value) {
                        left_iter.next();
                        Some(Sample { timestamp: ts, value: val })
                    } else {
                        let sample = Sample { timestamp: ts, value: r.value };
                        f(state, sample, true);
                        left_iter.next()
                    }
                } else if l < r {
                    left_iter.next()
                } else {
                    right_iter.next()
                }
            }
            (Some(_), None) => left_iter.next(),
            (None, Some(_)) => right_iter.next(),
            (None, None) => None,
        };
        if let Some(merged) = merged {
            f(state, merged, false);
        } else {
            break;
        }
    }
}