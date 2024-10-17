use crate::common::types::Sample;

pub struct SampleSliceIter<'a> {
    inner: std::slice::Iter<'a, Sample>,
}

impl<'a> SampleSliceIter<'a> {
    pub fn new(samples: &'a [Sample]) -> Self {
        Self {
            inner: samples.iter()
        }
    }
}

impl<'a> Iterator for SampleSliceIter<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().copied()
    }
}