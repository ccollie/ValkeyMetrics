use crate::common::types::Sample;
use crate::iter::sample_slice_iterator::SampleSliceIter;
use crate::iter::vec_sample_iterator::VecSampleIterator;
use crate::series::time_series::SeriesSampleIterator;
use crate::series::ChunkSampleIterator;

pub enum SampleIter<'a> {
    Series(SeriesSampleIterator<'a>),
    Chunk(ChunkSampleIterator<'a>),
    Slice(SampleSliceIter<'a>),
    Vec(VecSampleIterator)
}

impl<'a> SampleIter<'a> {
    pub fn slice(slice: &'a [Sample]) -> Self {
        SampleIter::Slice(SampleSliceIter::new(slice))
    }
    pub fn series(iter: SeriesSampleIterator<'a>) -> Self {
        SampleIter::Series(iter)
    }
    pub fn chunk(iter: ChunkSampleIterator<'a>) -> Self {
        SampleIter::Chunk(iter)
    }
}


impl<'a> Iterator for SampleIter<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SampleIter::Series(series) => series.next(),
            SampleIter::Chunk(chunk) => chunk.next(),
            SampleIter::Slice(slice) => slice.next(),
            SampleIter::Vec(iter) => iter.next(),
        }
    }
}

impl<'a> From<SeriesSampleIterator<'a>> for SampleIter<'a> {
    fn from(value: SeriesSampleIterator<'a>) -> Self {
        Self::Series(value)
    }
}

impl<'a> From<ChunkSampleIterator<'a>> for SampleIter<'a> {
    fn from(value: ChunkSampleIterator<'a>) -> Self {
        Self::Chunk(value)
    }
}

impl<'a> From<VecSampleIterator> for SampleIter<'a> {
    fn from(value: VecSampleIterator) -> Self {
        Self::Vec(value)
    }
}

impl<'a> From<Vec<Sample>> for SampleIter<'a> {
    fn from(value: Vec<Sample>) -> Self {
        Self::Vec(VecSampleIterator::new(value))
    }
}