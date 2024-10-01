use crate::common::types::Sample;

pub struct SampleSliceIter<'a> {
    idx: usize,
    samples: &'a [Sample],
}

impl<'a> SampleSliceIter<'a> {
    pub fn new(samples: &'a [Sample]) -> Self {
        SampleSliceIter { idx: 0, samples }
    }
}

impl<'a> Iterator for SampleSliceIter<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.samples.len() {
            return None;
        }
        let sample = self.samples[self.idx];
        self.idx += 1;
        Some(sample)
    }
}