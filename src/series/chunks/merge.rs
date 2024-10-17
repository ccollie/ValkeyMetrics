use crate::common::types::{Sample, Timestamp};
use crate::series::DuplicatePolicy;
use std::collections::BTreeSet;


pub fn merge_samples(
    dest: &mut Vec<Sample>,
    left: impl Iterator<Item=Sample>,
    right: impl Iterator<Item=Sample>,
    dp_policy: DuplicatePolicy,
    blocked: &mut BTreeSet<Timestamp>
) {
    let mut left_iter = left.peekable();
    let mut right_iter = right.peekable();

    if left_iter.peek().is_none() {
       dest.extend(right_iter);
        return;
    } else if right_iter.peek().is_none() {
        dest.extend(left_iter);
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
                        blocked.insert(ts);
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
            dest.push(merged);
        } else {
            break;
        }
    }
}