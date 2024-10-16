use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::module::types::ValueFilter;
use crate::series::{DuplicatePolicy, Sample};
use ahash::AHashSet;
use get_size::GetSize;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use enum_dispatch::enum_dispatch;
use valkey_module::error::Error;
use valkey_module::RedisModuleIO;
use crate::series::chunks::timeseries_chunk::TimeSeriesChunk;

pub const MIN_CHUNK_SIZE: usize = 48;
pub const MAX_CHUNK_SIZE: usize = 1048576;

#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[derive(GetSize)]
#[non_exhaustive]
pub enum ChunkCompression {
    Uncompressed = 1,
    #[default]
    Gorilla = 2,
    Pco = 4,
}

impl ChunkCompression {
    pub fn name(&self) -> &'static str {
        match self {
            ChunkCompression::Uncompressed => "uncompressed",
            ChunkCompression::Gorilla => "gorilla",
            ChunkCompression::Pco => "pco",
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

impl Display for ChunkCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl TryFrom<u8> for ChunkCompression {
    type Error = TsdbError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ChunkCompression::Uncompressed),
            2 => Ok(ChunkCompression::Gorilla),
            4 => Ok(ChunkCompression::Pco),
            _ => Err(TsdbError::InvalidCompression(value.to_string())),
        }
    }
}

impl TryFrom<&str> for ChunkCompression {
    type Error = TsdbError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            s if s.eq_ignore_ascii_case("uncompressed") => Ok(ChunkCompression::Uncompressed),
            s if s.eq_ignore_ascii_case("gorilla") => Ok(ChunkCompression::Gorilla),
            s if s.eq_ignore_ascii_case("pco") => Ok(ChunkCompression::Pco),
            _ => Err(TsdbError::InvalidCompression(s.to_string())),
        }
    }
}

pub trait Chunk: Sized {
    fn first_timestamp(&self) -> Timestamp;
    fn last_timestamp(&self) -> Timestamp;
    fn num_samples(&self) -> usize;
    fn last_value(&self) -> f64;
    fn size(&self) -> usize;
    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize>;
    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()>;
    fn get_range(
        &self,
        start: Timestamp,
        end: Timestamp,
    ) -> TsdbResult<Vec<Sample>>;

    fn upsert_sample(
        &mut self,
        sample: Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize>;

    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: DuplicatePolicy,
        blocked: &mut AHashSet<Timestamp>
    ) -> TsdbResult<usize>;

    fn split(&mut self) -> TsdbResult<Self>;
    fn overlaps(&self, start_ts: i64, end_ts: i64) -> bool {
        self.first_timestamp() <= end_ts && self.last_timestamp() >= start_ts
    }
    fn rdb_save(&self, rdb: *mut RedisModuleIO);
    fn rdb_load(rdb: *mut RedisModuleIO, _encver: i32) -> Result<Self, Error>;
}

pub(crate) struct ChunkSampleIterator<'a> {
    inner: Box<dyn Iterator<Item = Sample> + 'a>,
    start: Timestamp,
    end: Timestamp,
    is_init: bool,
}

impl<'a> ChunkSampleIterator<'a> {
    pub fn new(chunk: &'a TimeSeriesChunk,
           start: Timestamp,
           end: Timestamp,
           value_filter: &'a Option<ValueFilter>,
           ts_filter: &'a Option<Vec<Timestamp>>,
    ) -> Self {
        let iter = if chunk.overlaps(start, end) {
            Self::get_inner_iter(chunk, start, end, ts_filter, value_filter)
        } else {
            // use an empty iterator
            Box::new(std::iter::empty::<Sample>())
        };

        Self {
            inner: Box::new(iter),
            start,
            end,
            is_init: false,
        }
    }

    fn get_inner_iter(chunk: &'a TimeSeriesChunk,
                      start: Timestamp,
                      end: Timestamp,
                      ts_filter: &'a Option<Vec<Timestamp>>,
                      value_filter: &'a Option<ValueFilter>) -> Box<dyn Iterator<Item = Sample> + 'a> {
        match (ts_filter, value_filter) {
            (Some(timestamps), Some(value_filter)) => {
                Box::new(chunk.samples_by_timestamps(timestamps)
                    .unwrap_or_else(|_e| {
                        // todo: properly handle error and log
                        vec![]
                    })
                    .into_iter()
                    .filter(move |sample| sample.value >= value_filter.min && sample.value <= value_filter.max)
                )
            }
            (Some(timestamps), None) => {
                Box::new(chunk.samples_by_timestamps(timestamps)
                    .unwrap_or_else(|_e| {
                        // todo: properly handle error and log
                        vec![]
                    }).into_iter()
                )
            }
            (None, Some(filter)) => {
                Box::new(
                    chunk.range_iter(start, end)
                        .filter(|sample| sample.value >= filter.min && sample.value <= filter.max)
                )
            }
            _ => {
                Box::new(chunk.range_iter(start, end))
            }
        }
    }
}

// todo: implement next_chunk
impl<'a> Iterator for ChunkSampleIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_init {
            self.is_init = true;
            for sample in self.inner.by_ref() {
                if sample.timestamp > self.end {
                    return None;
                }
                if sample.timestamp < self.start {
                    continue;
                }
                return Some(sample);
            }
        }
        if let Some(sample) = self.inner.by_ref().next() {
            if sample.timestamp > self.end {
                return None;
            }
            return Some(sample);
        }
        None
    }
}

pub(crate) fn validate_chunk_size(chunk_size_bytes: usize) -> TsdbResult<()> {
    fn get_error_result() -> TsdbResult<()> {
        let msg = format!("TSDB: CHUNK_SIZE value must be a multiple of 2 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]");
        Err(TsdbError::InvalidConfiguration(msg))
    }

    if chunk_size_bytes < MIN_CHUNK_SIZE {
        return get_error_result();
    }

    if chunk_size_bytes > MAX_CHUNK_SIZE {
        return get_error_result();
    }

    if chunk_size_bytes % 2 != 0 {
        return get_error_result();
    }

    Ok(())
}

pub(crate) fn merge_by_capacity(
    dest: &mut TimeSeriesChunk,
    src: &mut TimeSeriesChunk,
    min_timestamp: Timestamp,
    duplicate_policy: DuplicatePolicy,
) -> TsdbResult<Option<usize>> {
    if src.is_empty() {
        return Ok(None);
    }

    // check if previous block has capacity, and if so merge into it
    let count = src.num_samples();
    let remaining_capacity = dest.estimate_remaining_sample_capacity();
    // if there is enough capacity in the previous block, merge the last block into it
    if remaining_capacity >= count {
        // copy all from last_chunk
        let res = dest.merge(src, min_timestamp, duplicate_policy)?;
        // reuse last block
        src.clear();
        return Ok(Some(res));
    } else if remaining_capacity > count / 4 {
        // do a partial merge
        let samples = src.get_range(src.first_timestamp(), src.last_timestamp())?;
        let (left, right) = samples.split_at(remaining_capacity);
        let mut duplicates = AHashSet::new();
        let res = dest.merge_samples(
            left,
            duplicate_policy,
            &mut duplicates,
        )?;
        src.clear();
        src.merge_samples(right, duplicate_policy, &mut duplicates)?;
        return Ok(Some(res));
    }
    Ok(None)
}


#[cfg(test)]
mod tests {
}