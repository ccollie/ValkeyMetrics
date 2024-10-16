use crate::common::types::Sample;
use crate::iter::sample_slice_iterator::SampleSliceIter;
use crate::iter::vec_sample_iterator::VecSampleIterator;
use crate::series::time_series::SeriesSampleIterator;
use crate::series::{ChunkSampleIterator, GorillaChunk, GorillaChunkIterator, PcoChunkIterator};

pub enum SampleIter<'a> {
    Series(SeriesSampleIterator<'a>),
    Chunk(ChunkSampleIterator<'a>),
    Slice(SampleSliceIter<'a>),
    Vec(VecSampleIterator),
    Gorilla(GorillaChunkIterator<'a>),
    Pco(PcoChunkIterator<'a>),
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
    pub fn vec(samples: Vec<Sample>) -> Self {
        SampleIter::Vec(VecSampleIterator::new(samples))
    }
    pub fn gorilla(iter: GorillaChunkIterator<'a>) -> Self {
        SampleIter::Gorilla(iter)
    }
    pub fn pco(iter: PcoChunkIterator<'a>) -> Self {
        SampleIter::Pco(iter)
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
            SampleIter::Gorilla(iter) => iter.next(),
            SampleIter::Pco(iter) => iter.next(),
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

impl<'a> From<GorillaChunkIterator<'a>> for SampleIter<'a> {
    fn from(value: GorillaChunkIterator<'a>) -> Self {
        Self::Gorilla(value)
    }
}

impl<'a> From<PcoChunkIterator<'a>> for SampleIter<'a> {
    fn from(value: PcoChunkIterator<'a>) -> Self {
        Self::Pco(value)
    }
}