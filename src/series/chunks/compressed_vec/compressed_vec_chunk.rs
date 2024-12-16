use std::fmt::Debug;
use crate::common::binary_search::get_index_bounds;
use crate::common::types::Sample;
use crate::error::{TsdbError, TsdbResult};
use crate::iterators::SampleIter;
use crate::series::merge::merge_samples;
use crate::series::{Chunk, DuplicatePolicy};
use compressed_vec::vector::{VectorItemIter, VectorStats};
use compressed_vec::{VectorF32XorAppender, VectorU64Appender};
use metricsql_runtime::prelude::Timestamp;
use std::iter::{Map, Zip};
use std::sync::LazyLock;
use get_size::GetSize;
use regex::Regex;

#[derive(Clone)]
pub struct CompressedVecChunk {
    pub(super) values: VectorF32XorAppender,
    pub(super) timestamps: VectorU64Appender,
    pub(super) init_size: usize,
    pub max_size_bytes: usize,
    pub start_ts: i64,
    pub end_ts: i64,
    pub last_value: f64,
}

impl Debug for CompressedVecChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressedVecChunk")
            .field("samples", &self.len())
            .field("max_size_bytes", &self.max_size_bytes)
            .field("start_ts", &self.start_ts)
            .field("end_ts", &self.end_ts)
            .field("last_value", &self.last_value)
            .finish()
    }
}
impl GetSize for CompressedVecChunk {
    fn get_size(&self) -> usize {
        let value_reader = self.values.reader();
        let timestamp_reader = self.timestamps.reader();
        let value_stats = VectorStats::new(&value_reader);
        let timestamp_stats = VectorStats::new(&timestamp_reader);
        let (val_bytes, val_bytes_per_elem) = parse_stats(&value_stats)
            .unwrap_or((0, 0.0));
        let (ts_bytes, ts_bytes_per_elem) = parse_stats(&timestamp_stats)
            .unwrap_or((0, 0.0));

        let struct_size = size_of::<Self>();
        struct_size + ts_bytes + val_bytes
    }
}

impl CompressedVecChunk {
    pub fn new(
        max_size_bytes: usize,
    ) -> Self {
        // calculate the size of the chunk
        let init_size = Self::calc_init_size(max_size_bytes);
        let (values, timestamps) = alloc_vectors(init_size).unwrap();
        Self {
            init_size,
            max_size_bytes,
            values, 
            timestamps, 
            start_ts: 0,
            end_ts: 0,
            last_value: f64::NAN,
        }
    }

    pub fn clear(&mut self) {
        // todo: remove unwrap and return a Result
        let (new_values, new_timestamps) = alloc_vectors(self.init_size).unwrap();
        self.values = new_values;
        self.timestamps = new_timestamps;
        self.start_ts = 0;
        self.end_ts = 0;
        self.last_value = f64::NAN;
    }

    pub fn iter(&self) -> InnerIterator {
        let values = self.values.reader();
        let timestamps = self.timestamps.reader();
        timestamps.iterate().zip(values.iterate())
            .map(|(ts, val)| Sample {timestamp: *ts, value: *val })
    }
    
    fn append_internal(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.start_ts <= 0 {
            self.start_ts = sample.timestamp;
        }
        self.end_ts = sample.timestamp;
        self.last_value = sample.value;
        append_internal(&mut self.values, &mut self.timestamps, sample)
    }

    pub(super) fn calc_init_size(max_size: usize) -> usize {
        // calculate the size of the chunk
        if max_size < 512 {
            512
        } else {
            max_size.min(1024)
        }
    }
}


fn parse_stats(stats: &VectorStats) -> Option<(usize, f32)> {
    let str = stats.summary_string();
    parse_stats_summary_string(&str)
}

const STATS_REGEX: LazyLock<Regex> = LazyLock::new(||
    Regex::new(r"#bytes=(\d+)\s+#elems=\d+\s+bytes-per-elem=([\d.]+)").unwrap()
);

pub fn parse_stats_summary_string(summary: &str) -> Option<(usize, f32)> {
    if let Some(captures) = STATS_REGEX.captures(summary) {
        let num_bytes = captures.get(1)?.as_str().parse::<usize>().ok()?;
        let bytes_per_elem = captures.get(2)?.as_str().parse::<f32>().ok()?;
        Some((num_bytes, bytes_per_elem))
    } else {
        None
    }
}

// type to avoid writing the whole type signature or having to box the iterator
pub type InnerIterator<'a> = Map<Zip<VectorItemIter<'a, u64>, VectorItemIter<'a, f32>>, fn((u64, f32)) -> Sample>;

pub struct CompressedVecChunkIterator<'a> {
    inner: InnerIterator<'a>,
    index: usize,
}

impl CompressedVecChunkIterator<'_> {
    pub fn new<'a>(chunk: &'a CompressedVecChunk) -> Self {
        let values = chunk.values.reader();
        let timestamps = chunk.timestamps.reader();
        let inner: InnerIterator<'a> = timestamps.iterate().zip(values.iterate())
            .map(|(ts, val)| Sample {timestamp: *ts, value: *val });
        Self {
            inner,
            index: 0,
        }
    }
}

impl PartialEq for CompressedVecChunk {
    fn eq(&self, other: &Self) -> bool {
        let eq = self.start_ts == other.start_ts
            && self.end_ts == other.end_ts
            && self.last_value == other.last_value
            && self.values.num_elements() == other.values.num_elements()
            && self.timestamps.num_elements() == other.timestamps.num_elements();

        if !eq {
            return false;
        }
        // compare the values
        let self_values = self.values.reader();
        let other_values = other.values.reader();
        if !self_values.iterate().zip(other_values.iterate())
            .all(|(a, b)| a == b) {
            return false;
        }
        let self_timestamps = self.timestamps.reader();
        let other_timestamps = other.timestamps.reader();

        self_timestamps.iterate().zip(other_timestamps.iterate())
            .all(|(a, b)| a == b)
    }
}

// Implementing the Chunk trait
impl Chunk for CompressedVecChunk {
    fn first_timestamp(&self) -> i64 {
        self.start_ts
    }

    fn last_timestamp(&self) -> i64 {
        self.end_ts
    }

    fn len(&self) -> usize {
        self.values.num_elements()
    }

    fn last_value(&self) -> f64 {
        self.last_value
    }

    fn size(&self) -> usize {
        self.get_size()
    }

    fn max_size(&self) -> usize {
        self.max_size_bytes
    }

    fn remove_range(&mut self, start_ts: i64, end_ts: i64) -> TsdbResult<usize> {
        if self.is_empty() {
            return Ok(0);
        }

        if self.is_range_covering_full_period(start_ts, end_ts) {
            self.clear();
            return Ok(0);
        }

        let old_sample_count = self.len();
        
        let (mut new_values, mut new_timestamps) = alloc_vectors(self.init_size)?;
        let mut last_ts = self.end_ts;
        let mut first_ts = -1;
        let mut last_value = f64::NAN;
        
        let mut iter = self.iter();

        // skip previous samples
        for sample in iter.by_ref() {
            if sample.timestamp < start_ts {
                continue;
            }
            if sample >= end_ts {
                break;
            }
            append_internal(&mut new_values, &mut new_timestamps, &sample)?;
            if first_ts < 0 {
                first_ts = sample.timestamp;
            }
            last_ts = sample.timestamp;
            last_value = sample.value;
        }

        self.values = new_values;
        self.timestamps = new_timestamps;
        
        self.start_ts = first_ts.min(0);
        self.end_ts = last_ts;
        self.last_value = last_value;

        Ok(old_sample_count - self.len())
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_empty() {
            self.start_ts = sample.timestamp;
        }
        self.end_ts = sample.timestamp;
        self.last_value = sample.value;
        append_internal(&mut self.values, &mut self.timestamps, sample)
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() {
            return Ok(vec![]);
        }
        
        if start < self.start_ts && end > self.end_ts {
            // todo: use VectorSinks instead
            return Ok(self.iter().collect());
        }
        // todo: use filters instead
        let iter = self.iter().skip_while(|s| s.timestamp < start);
        let samples = iter.filter(|s| s.timestamp <= end).collect();
        Ok(samples)
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;

        if self.is_empty() {
            self.add_sample(&sample)?;
            return Ok(1)
        }

        let count = self.len();
        let (mut values, mut timestamps) = alloc_vectors(self.len())?;
        let mut iter = self.iter();

        let mut current = Sample::default();
        
        // skip previous samples
        for sample in iter.by_ref().filter(|s| s.timestamp < ts) {
            append_internal(&mut values, &mut timestamps, &sample)?;
        }

        if let Some(sample) = iter.next() {
            current = sample;
            if current.timestamp == ts {
                duplicate_found = true;
                current.value = dp_policy.duplicate_value(ts, current.value, sample.value)?;
                append_internal(&mut values, &mut timestamps, &current)?;
                //tail.next();
            } else {
                append_internal(&mut values, &mut timestamps, &sample)?;
            }

            for current in iter {
                append_internal(&mut values, &mut timestamps, &current)?;
            }
        }
        
        self.values = values;
        self.timestamps = timestamps;
        self.start_ts = self.start_ts.min(ts); 
        if ts >= self.end_ts {
            self.end_ts = ts;
            self.last_value = sample.value;
        }
        
        let size = if duplicate_found { count } else { count + 1 };
        Ok(size)
    }

    fn merge_samples(&mut self, samples: &[Sample], dp_policy: Option<DuplicatePolicy>) -> TsdbResult<usize> {
        let policy = dp_policy.unwrap_or(DuplicatePolicy::KeepLast);

        if samples.len() == 1 {
            let first = samples[0];
            if self.is_empty() {
                self.add_sample(&first)?;
                return Ok(1);
            }
            return self.upsert_sample(first, policy);
        } else if self.is_empty() {
            self.set_data(samples).map(|_| self.len())
        }

        struct MergeState {
            values: VectorF32XorAppender,
            timestamps: VectorU64Appender,
            count: usize,
        }

        let (values, timestamps) = alloc_vectors(self.len() + samples.len())?;
        let mut merge_state = MergeState {
            values,
            timestamps,
            count: 0,
        };

        let left = SampleIter::Slice(samples.iter());
        let right = self.iter();

        let mut last_ts = 0;
        let mut last_value = self.last_value;
        merge_samples(left, right, dp_policy, &mut merge_state, |state, sample, is_duplicate| {
            if !is_duplicate {
                state.count += 1;
                append_internal(&mut state.values, &mut state.timestamps, &sample)?;
            }
            Ok(())
        })?;

        if merge_state.count == 0 {
            return Ok(0);
        }
        
        self.values = merge_state.values;
        self.timestamps = merge_state.timestamps;
        Ok(merge_state.count)
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let (mut left_chunk, mut right_chunk) = (Self::new(self.max_size_bytes), Self::new(self.max_size_bytes));

        if self.is_empty() {
            return Ok(self.clone());
        }

        let mid = self.len() / 2;
        let mut iter = self.iter();
        for (i, sample) in iter.by_ref().enumerate() {
            if i < mid {
                left_chunk.append_internal(&sample)?;
            } else {
                break;
            }
        }

        for sample in iter {
            right_chunk.append_internal(&sample)?;
        }
        
        self.start_ts = left_chunk.start_ts;
        self.end_ts = left_chunk.end_ts;
        self.last_value = left_chunk.last_value;
        self.values = left_chunk.values;
        self.timestamps = left_chunk.timestamps;

        Ok(right_chunk)
    }
}


fn append_internal(values: &mut VectorF32XorAppender, timestamps: &mut VectorU64Appender, sample: &Sample) -> TsdbResult<()> {
    // todo: check for overflow
    values.append(sample.value as f32)
        .map_err(|e| TsdbError::from(e))?;
    // todo: check for overflow
    timestamps.append(sample.timestamp as u64)
        .map_err(|e| TsdbError::from(e))?;
    Ok(())
}

fn alloc_vectors(size: usize) -> TsdbResult<(VectorF32XorAppender, VectorU64Appender)> {
    let values = VectorF32XorAppender::try_new(size)
        .map_err(|e| TsdbError::from(e))?;
    let timestamps = VectorU64Appender::try_new(size)
        .map_err(|e| TsdbError::from(e))?;
    Ok((values, timestamps))
}

fn get_timestamp_index_bounds(timestamps: &[u64], start_ts: Timestamp, end_ts: Timestamp) -> Option<(usize, usize)> {
    let start = start_ts as u64;
    let end = end_ts as u64;
    get_index_bounds(timestamps, &start, &end)
}