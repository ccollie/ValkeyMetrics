use std::vec::IntoIter;
use crate::common::types::Sample;

pub struct VecSampleIterator {
    inner: IntoIter<Sample>,
}

impl VecSampleIterator {
    pub fn new(samples: Vec<Sample>) -> Self {
        let inner = samples.into_iter();  // slice iterator
        Self {
            inner,  // slice iterator
        }
    }
}

impl Iterator for VecSampleIterator {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}