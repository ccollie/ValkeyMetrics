use std::iter::Peekable;
use crate::common::types::Sample;
use crate::iter::SampleIter;
use crate::series::DuplicatePolicy;

pub struct SampleMergeIterator<'a> {
    left: Peekable<SampleIter<'a>>,
    right: Peekable<SampleIter<'a>>,
    duplicate_policy: DuplicatePolicy
}

impl<'a> SampleMergeIterator<'a>
{
    fn new(left: SampleIter<'a>, right: SampleIter<'a>, duplicate_policy: DuplicatePolicy) -> Self {
        SampleMergeIterator {
            left: left.peekable(),
            right: right.peekable(),
            duplicate_policy
        }
    }

    fn next_internal(&mut self) -> Option<(Sample, bool)> {
        let mut blocked = false;

        let sample = match (self.left.peek(), self.right.peek()) {
            (Some(&left), Some(&right)) => {
                if left.timestamp == right.timestamp {
                    let ts = left.timestamp;
                    if let Ok(val) = self.duplicate_policy.duplicate_value(ts, right.value, left.value) {
                        self.left.next();
                        Some(Sample { timestamp: ts, value: val })
                    } else {
                        // block duplicate
                        blocked = true;
                        self.left.next()
                    }
                } else if left < right {
                    self.left.next()
                } else {
                    self.right.next()
                }
            }
            (Some(_), None) => self.left.next(),
            (None, Some(_)) => self.right.next(),
            (None, None) => None,
        };

        if let Some(sample) = sample {
            Some((sample, blocked))
        } else {
            None
        }
    }
}

impl<'a> Iterator for SampleMergeIterator<'a>
{
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            return if let Some((sample, blocked)) = self.next_internal() {
                if blocked {
                    continue;
                }
                Some(sample)
            } else {
                None
            }
        }
    }
}