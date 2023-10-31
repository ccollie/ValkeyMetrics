use super::{Chunk, ChunkCompression, Label, merge_by_capacity, Sample, TimeSeriesChunk};
use crate::common::types::{PooledTimestampVec, PooledValuesVec, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::ts::constants::{DEFAULT_CHUNK_SIZE_BYTES, SPLIT_FACTOR};
use crate::ts::uncompressed_chunk::UncompressedChunk;
use crate::ts::DuplicatePolicy;
use ahash::{AHashMap, AHashSet};
use metricsql_common::pool::{get_pooled_vec_f64, get_pooled_vec_i64};
use serde::{Deserialize, Serialize};
use std::collections::BinaryHeap;
use std::time::Duration;
use get_size::GetSize;

pub type Labels = AHashMap<String, String>;

/// Represents a time series. The time series consists of time series blocks, each containing BLOCK_SIZE_FOR_TIME_SERIES
/// data points. All but the last block are compressed.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[derive(GetSize)]
pub struct TimeSeries {
    pub(crate) id: u64,

    pub metric_name: String,

    pub labels: Vec<Label>,
    pub retention: Duration,
    pub dedupe_interval: Option<Duration>,
    pub duplicate_policy: Option<DuplicatePolicy>,
    pub chunk_compression: ChunkCompression,
    pub chunk_size_bytes: usize,
    pub chunks: Vec<TimeSeriesChunk>,

    pub total_samples: usize,
    pub first_timestamp: Timestamp,
    pub last_timestamp: Timestamp,
    pub last_value: f64,
}

impl TimeSeries {
    /// Create a new empty time series.
    pub fn new() -> Self {
        TimeSeries {
            id: 0,
            metric_name: "".to_string(),
            labels: Default::default(),
            retention: Default::default(),
            duplicate_policy: None,
            chunk_compression: Default::default(),
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            dedupe_interval: Default::default(),
            chunks: vec![],
            total_samples: 0,
            first_timestamp: 0,
            last_timestamp: 0,
            last_value: f64::NAN,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.total_samples == 0
    }

    pub fn get_label_value(&self, name: &str) -> Option<&String> {
        if let Some(label) = self.get_label(name) {
            return Some(&label.value);
        }
        None
    }

    pub fn get_label(&self, name: &str) -> Option<&Label> {
        self.labels.iter().find(|label| label.name == name)
    }

    pub fn add(
        &mut self,
        ts: Timestamp,
        value: f64,
        dp_override: Option<DuplicatePolicy>,
    ) -> TsdbResult<()> {
        if self.is_older_than_retention(ts) {
            return Err(TsdbError::SampleTooOld);
        }

        let last_ts = self.last_timestamp;
        if !self.is_empty() {
            if let Some(dedup_interval) = self.dedupe_interval {
                let millis = dedup_interval.as_millis() as i64;
                if millis > 0 && (ts - last_ts) < millis {
                    // todo: use policy to derive a value to insert
                    let msg = "New sample encountered in less than dedupe interval";
                    return Err(TsdbError::DuplicateSample(msg.to_string()));
                }
            }

            if ts <= last_ts {
                let _ = self.upsert_sample(ts, value, dp_override);
                return Ok(());
            }
        }

        self.add_sample(ts, value)
    }

    pub(super) fn add_sample(&mut self, time: Timestamp, value: f64) -> TsdbResult<()> {
        let sample = Sample {
            timestamp: time,
            value,
        };
        if self.is_empty() {
            let mut new_chunk = TimeSeriesChunk::Uncompressed(UncompressedChunk::with_max_size(
                self.chunk_size_bytes,
            ));
            new_chunk.add_sample(&sample)?;

            self.first_timestamp = time;
            self.chunks.push(new_chunk);
        } else {
            // append sample. False means we've overflowed capacity
            if !self.append(&sample)? {
                self.add_chunk_with_sample(&sample)?;
            }
        }

        self.last_value = value;
        self.last_timestamp = time;
        self.total_samples += 1;
        Ok(())
    }

    #[inline]
    fn append(&mut self, sample: &Sample) -> TsdbResult<bool> {
        let chunk = self.get_last_chunk();
        match chunk.add_sample(sample) {
            Err(TsdbError::CapacityFull(_)) => Ok(false),
            Err(e) => return Err(e),
            _ => Ok(true),
        }
    }

    /// Add a new chunk and compact the current chunk if necessary.
    fn add_chunk_with_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        let chunks_len = self.chunks.len();

        // The last block is full. So, compress it and append it time_series_block_compressed.
        let chunk_size = self.chunk_size_bytes;
        let compression = self.chunk_compression;
        let min_timestamp = self.get_min_timestamp();
        let duplicate_policy = self.duplicate_policy.unwrap_or(DuplicatePolicy::KeepLast);

        // arrrgh! rust treats vecs as a single unit wrt borrowing, but the following iterator trick
        // seems to work
        let mut iter = self.chunks.iter_mut().rev();
        let last_chunk = iter.next().unwrap();

        // check if previous block has capacity, and if so merge into it
        if let Some(prev_chunk) = iter.next() {
            if let Some(deleted_count) = merge_by_capacity(
                prev_chunk,
                last_chunk,
                min_timestamp,
                duplicate_policy,
            )? {
                self.total_samples -= deleted_count;
                last_chunk.add_sample(sample)?;
                return Ok(());
            }
        }

        // if the last chunk is uncompressed, create a new compressed block and move samples to it
        if let TimeSeriesChunk::Uncompressed(uncompressed_chunk) = last_chunk {
            let new_chunk = TimeSeriesChunk::new(
                compression,
                chunk_size,
                &uncompressed_chunk.timestamps,
                &uncompressed_chunk.values,
            )?;

            // reuse last chunk
            last_chunk.clear();

            // insert new chunk before last block
            self.chunks.insert(chunks_len - 1, new_chunk);
            // res
        } else {
            let mut new_chunk = TimeSeriesChunk::Uncompressed(UncompressedChunk::with_max_size(
                self.chunk_size_bytes,
            ));
            new_chunk.add_sample(sample)?;
            self.chunks.push(new_chunk);

            return Ok(());
        }

        return Ok(());
    }

    fn append_uncompressed_chunk(&mut self) {
        let new_chunk =
            TimeSeriesChunk::Uncompressed(UncompressedChunk::with_max_size(self.chunk_size_bytes));
        self.chunks.push(new_chunk);
    }

    #[inline]
    fn get_last_chunk(&mut self) -> &mut TimeSeriesChunk {
        if self.chunks.is_empty() {
            self.append_uncompressed_chunk();
        }
        self.chunks.last_mut().unwrap()
    }

    fn get_first_chunk(&mut self) -> &mut TimeSeriesChunk {
        if self.chunks.is_empty() {
            self.append_uncompressed_chunk();
        }
        self.chunks.first_mut().unwrap()
    }

    pub(crate) fn upsert_sample(
        &mut self,
        timestamp: Timestamp,
        value: f64,
        dp_override: Option<DuplicatePolicy>,
    ) -> TsdbResult<usize> {
        let dp_policy = dp_override.unwrap_or(
            self.duplicate_policy.unwrap_or_default(), //.unwrap_or(/* TSGlobalConfig.duplicatePolicy),
        );

        let (size, new_chunk) = {
            let max_size = self.chunk_size_bytes;
            let (pos, _) = get_chunk_index(&self.chunks, timestamp);
            let chunk = self.chunks.get_mut(pos).unwrap();
            Self::handle_upsert(chunk, timestamp, value, max_size, dp_policy)?
        };

        if let Some(new_chunk) = new_chunk {
            // todo: how to avoid this ? Since chunks are currently stored inline this can cause a lot of
            // moves
            let ts = new_chunk.first_timestamp();
            let insert_at = self.chunks.binary_search_by(|chunk| {
                chunk.first_timestamp().cmp(&ts)
            }).unwrap_or_else(|i| i);
            self.chunks.insert(insert_at, new_chunk);
        }

        self.total_samples += size;
        if timestamp == self.last_timestamp {
            self.last_value = value;
        }

        Ok(size)
    }

    fn handle_upsert(
        chunk: &mut TimeSeriesChunk,
        timestamp: Timestamp,
        value: f64,
        max_size: usize,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<(usize, Option<TimeSeriesChunk>)> {
        let mut sample = Sample { timestamp, value };
        if chunk.size() as f64 > max_size as f64 * SPLIT_FACTOR {
            let mut new_chunk = chunk.split()?;
            let size = new_chunk.upsert_sample(&mut sample, dp_policy)?;
            Ok((size, Some(new_chunk)))
        } else {
            let size = chunk.upsert_sample(&mut sample, dp_policy)?;
            Ok((size, None))
        }
    }

    /// Get the time series between give start and end time (both inclusive).
    pub fn get_range(&self, start_time: Timestamp, end_time: Timestamp) -> TsdbResult<Vec<Sample>> {
        let mut result: BinaryHeap<Sample> = BinaryHeap::new();
        let mut timestamps = Vec::with_capacity(64);
        let mut values = Vec::with_capacity(64);

        self.select_raw(start_time, end_time, &mut timestamps, &mut values)?;
        for (ts, value) in timestamps.iter().zip(values.iter()) {
            result.push(Sample::new(*ts, *value));
        }

        Ok(result.into_sorted_vec())
    }

    pub fn select_raw(
        &self,
        start_time: Timestamp,
        end_time: Timestamp,
        timestamps: &mut Vec<Timestamp>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        if self.is_empty() {
            return Ok(());
        }
        let (index, _) = get_chunk_index(&self.chunks, start_time);
        let chunks = &self.chunks[index..];
        // Get overlapping data points from the compressed blocks.
        for chunk in chunks.iter() {
            let first = chunk.first_timestamp();
            if first > end_time {
                break;
            }
            chunk.get_range(first, end_time, timestamps, values)?;
        }

        Ok(())
    }

    pub fn iter<'a>(
        &'a self,
        start: Timestamp,
        end: Timestamp,
    ) -> impl Iterator<Item = Sample> + 'a {
        SampleIterator::new(self, start, end)
    }

    pub fn overlaps(&self, start_ts: Timestamp, end_ts: Timestamp) -> bool {
        self.last_timestamp >= start_ts && self.first_timestamp <= end_ts
    }

    pub fn is_older_than_retention(&self, timestamp: Timestamp) -> bool {
        let min_ts = self.get_min_timestamp();
        if timestamp < min_ts {
            return true;
        }
        false
    }

    pub fn trim(&mut self) -> TsdbResult<()> {
        if self.retention.is_zero() {
            return Ok(());
        }

        let min_timestamp = self.get_min_timestamp();
        let mut count = 0;
        let mut deleted_count = 0;

        for chunk in self
            .chunks
            .iter()
            .take_while(|&block| block.first_timestamp() < min_timestamp)
        {
            count += 1;
            deleted_count += chunk.num_samples();
        }

        if count > 0 {
            let _ = self.chunks.drain(0..count);
        }

        self.total_samples -= deleted_count;

        Ok(())
    }

    pub fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        let mut deleted_samples = 0;
        // todo: tinyvec
        let mut indexes_to_delete = Vec::new();

        let (index, _) = get_chunk_index(&self.chunks, start_ts);
        let chunks = &mut self.chunks[index..];

        // Todo: although many chunks may be deleted, only a max of 2 will be modified, so
        // we can try to merge it with the next chunk

        for (idx, chunk) in chunks.iter_mut().enumerate() {
            let chunk_first_ts = chunk.first_timestamp();

            // We deleted the latest samples, no more chunks/samples to delete or cur chunk start_ts is
            // larger than end_ts
            if chunk.is_empty() || chunk_first_ts > end_ts {
                // Having empty chunk means the series is empty
                break;
            }

            let is_only_chunk =
                (chunk.num_samples() + deleted_samples) == self.total_samples;

            // Should we delete the entire chunk?
            // We assume at least one allocated chunk in the series
            if chunk.is_contained_by_range(start_ts, end_ts) && (!is_only_chunk) {
                deleted_samples += chunk.num_samples();
                indexes_to_delete.push(index + idx);
            } else {
                deleted_samples += chunk.remove_range(start_ts, end_ts)?;
            }
        }

        self.total_samples -= deleted_samples;

        for idx in indexes_to_delete.iter().rev() {
            let _ = self.chunks.remove(*idx);
        }

        // Check if last timestamp deleted
        if end_ts >= self.last_timestamp && start_ts <= self.last_timestamp {
            match self.chunks.iter().last() {
                Some(chunk) => {
                    self.last_timestamp = chunk.last_timestamp();
                    self.last_value = chunk.last_value();
                }
                None => {
                    self.last_timestamp = 0;
                    self.last_value = f64::NAN;
                }
            }
        }

        Ok(deleted_samples)
    }

    pub fn data_size(&self) -> usize {
        self.chunks.iter().map(|x| x.size()).sum()
    }

    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() +
            self.get_heap_size()
    }

    fn get_min_timestamp(&self) -> Timestamp {
        if self.retention.is_zero() {
            return 0;
        }
        let retention_millis = self.retention.as_millis() as i64;
        (self.last_timestamp - retention_millis).min(0)
    }
}

impl Default for TimeSeries {
    fn default() -> Self {
        Self::new()
    }
}

/// Return the index of the chunk in which the timestamp belongs. Assumes !chunks.is_empty()
fn get_chunk_index(chunks: &Vec<TimeSeriesChunk>, timestamp: Timestamp) -> (usize, bool) {
    let len = chunks.len();
    let slice = chunks.as_slice();
    let first = slice[0].first_timestamp();
    let last = slice[len - 1].last_timestamp();
    if timestamp <= first {
        return (0, false);
    }
    if timestamp >= last {
        return (slice.len() - 1, false);
    }
    return match chunks.binary_search_by(|probe| {
        if timestamp < probe.first_timestamp() {
            std::cmp::Ordering::Greater
        } else if timestamp > probe.last_timestamp() {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Equal
        }
    }) {
        Ok(pos) => (pos, true),
        Err(pos) => (pos, false),
    };
}

pub struct SampleIterator<'a> {
    series: &'a TimeSeries,
    timestamps: PooledTimestampVec,
    values: PooledValuesVec,
    chunk_index: usize,
    sample_index: usize,
    start: Timestamp,
    end: Timestamp,
    err: bool,
    first_iter: bool,
    filter: Option<Box<dyn Fn(&Sample) -> bool>>,
}

impl<'a> SampleIterator<'a> {
    fn new(series: &'a TimeSeries, start: Timestamp, end: Timestamp) -> Self {
        let (chunk_index, _) = get_chunk_index(&series.chunks, start);
        // estimate buffer size
        let size = if chunk_index < series.chunks.len() {
            series.chunks[chunk_index..]
                .iter()
                .take_while(|chunk| chunk.last_timestamp() >= end)
                .map(|chunk| chunk.num_samples())
                .min()
                .unwrap_or(4)
        } else {
            4
        };

        let timestamps = get_pooled_vec_i64(size);
        let values = get_pooled_vec_f64(size);

        Self {
            series,
            timestamps,
            values,
            chunk_index,
            sample_index: 0,
            start,
            end,
            err: false,
            first_iter: false,
            filter: None,
        }
    }

    fn with_filter<F>(series: &'a TimeSeries, start: Timestamp, end: Timestamp, filter: F) -> Self
    where
        F: Fn(&Sample) -> bool + 'static,
    {
        let mut iter = Self::new(series, start, end);
        iter.filter = Some(Box::new(filter));
        iter
    }

    fn next_chunk(&mut self) -> bool {
        if self.chunk_index >= self.series.chunks.len() {
            return false;
        }
        let chunk = &self.series.chunks[self.chunk_index];
        if chunk.first_timestamp() > self.end {
            return false;
        }
        self.chunk_index += 1;
        self.timestamps.clear();
        self.values.clear();
        if let Err(_e) =
            chunk.get_range(self.start, self.end, &mut self.timestamps, &mut self.values)
        {
            self.err = true;
            return false;
        }
        self.sample_index = if self.first_iter {
            match self.timestamps.binary_search(&self.start) {
                Ok(idx) => idx,
                Err(idx) => idx,
            }
        } else {
            0
        };
        if let Some(filter) = &self.filter {
            let timestamps = &self.timestamps[self.sample_index..];
            let values = &self.values[self.sample_index..];

            // trim the timestamps by the filter function while retaining a list of the
            // indices that were removed
            let mut indices_to_remove = AHashSet::new();
            for (idx, (ts, v)) in timestamps.iter().zip(values.iter()).enumerate() {
                let sample = Sample::new(*ts, *v);
                if !filter(&sample) {
                    indices_to_remove.insert(idx);
                }
            }

            let mut idx = 0;
            self.timestamps.retain(|_| {
                let keep = indices_to_remove.contains(&idx);
                idx += 1;
                keep
            });

            idx = 0;
            self.values.retain(|_| {
                let keep = indices_to_remove.contains(&idx);
                idx += 1;
                keep
            });

            if self.timestamps.is_empty() {
                // loop again
                return self.next_chunk();
            }
        }

        self.start = chunk.last_timestamp();
        self.first_iter = false;
        true
    }
}

// todo: implement next_chunk
impl<'a> Iterator for SampleIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.sample_index >= self.timestamps.len() || self.first_iter {
            if !self.next_chunk() {
                return None;
            }
        }
        let timestamp = self.timestamps[self.sample_index];
        let value = self.values[self.sample_index];
        self.sample_index += 1;
        Some(Sample::new(timestamp, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ts::constants::BLOCK_SIZE_FOR_TIME_SERIES;

    #[test]
    fn test_one_entry() {
        let mut ts = TimeSeries::new();
        ts.add(100, 200.0, None).unwrap();

        assert_eq!(ts.get_last_chunk().num_samples(), 1);
        let last_block = ts.get_last_chunk();
        let samples = last_block.get_samples(0, 1000).unwrap();

        let data_point = samples.get(0).unwrap();
        assert_eq!(data_point.get_time(), 100);
        assert_eq!(data_point.get_value(), 200.0);
        assert_eq!(ts.total_samples, 1);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp, 100);
    }

    #[test]
    fn test_block_size_entries() {
        let mut ts = TimeSeries::new();
        for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
            ts.add(i as i64, i as f64, None).unwrap();
        }

        // All the entries will go to 'last', as we have pushed exactly BLOCK_SIZE_FOR_TIME_SERIES entries.
        assert_eq!(ts.chunks.len(), 2);
        assert_eq!(
            ts.get_last_chunk().num_samples(),
            BLOCK_SIZE_FOR_TIME_SERIES
        );

        for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
            let last_block = ts.get_last_chunk();
            let samples = last_block.get_samples(0, 7000).unwrap();
            let data_point = samples.get(i).unwrap();
            assert_eq!(data_point.timestamp, i as i64);
            assert_eq!(data_point.value, i as f64);
        }
    }
}
