use crate::common::types::Sample;

pub struct VecSampleIterator {
    samples: Vec<Sample>,
    idx: usize,
}

impl VecSampleIterator {
    pub fn new(samples: Vec<Sample>) -> Self {
        Self { samples, idx: 0 }
    }
}

impl Iterator for VecSampleIterator {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.samples.len() {
            return None;
        }
        let result = self.samples[self.idx];
        self.idx += 1;
        Some(result)
    }
}