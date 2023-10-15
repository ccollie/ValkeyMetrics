use crate::error::{TsdbError, TsdbResult};
use crate::ts::chunks::uncompressed::UncompressedChunk;
use crate::ts::chunks::{Compression, TimeSeriesChunk, TimesSeriesBlock};
use crate::ts::constants::{BLOCK_SIZE_FOR_TIME_SERIES, DEFAULT_CHUNK_SIZE_BYTES, SPLIT_FACTOR};
use crate::ts::{DuplicatePolicy, Sample, Timestamp};
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use std::collections::BinaryHeap;
use std::time::Duration;

pub type Labels = AHashMap<String, String>;

/// Represents a time series. The time series consists of time series blocks, each containing BLOCK_SIZE_FOR_TIME_SERIES
/// data points. All but the last block are compressed.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct TimeSeries {
    pub(crate) id: u64,

    pub metric_name: String,

    pub labels: Labels,

    pub(crate) retention: Duration,

    pub(crate) dedupe_interval: Option<Duration>,
    pub(crate) duplicate_policy: Option<DuplicatePolicy>,

    pub(crate) chunk_compression: Compression,
    pub(crate) chunk_size_bytes: usize,
    pub(crate) chunks: Vec<TimeSeriesChunk>,

    /// We only compress blocks that have 128 integers. The last block which
    /// may have <=127 items is stored in uncompressed form.
    pub last_chunk: TimeSeriesChunk,

    pub total_samples: u64,

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
            last_chunk: TimeSeriesChunk::Uncompressed(UncompressedChunk::default()),
            total_samples: 0,
            first_timestamp: 0,
            last_timestamp: 0,
            last_value: f64::NAN,
        }
    }

    /// Append the given time and value to the time series.
    pub fn append(&mut self, time: Timestamp, value: f64) -> TsdbResult<()> {
        if self.last_chunk.is_empty() {
            // First insertion in this time series.
            self.first_timestamp = time;
            self.last_timestamp = time;
            self.last_value = value;
        }

        // Try to append the time+value to the last block.
        let ret_val = self.last_chunk.add_sample(&Sample {
            timestamp: time,
            value,
        });

        if ret_val.is_err()
            && ret_val.err().unwrap() == TsdbError::CapacityFull(BLOCK_SIZE_FOR_TIME_SERIES)
        {
            // The last block is full. So, compress it and append it time_series_block_compressed.
            // todo: see if previous block is not full, and if not simply append to it
            match self.last_chunk {
                TimeSeriesChunk::Uncompressed(ref mut uncompressed_chunk) => {
                    let tsbc = TimeSeriesChunk::new(
                        self.chunk_compression,
                        &uncompressed_chunk.timestamps,
                        &uncompressed_chunk.values,
                    )?;
                    self.chunks.push(tsbc);
                }
                _ => unreachable!("last block should be uncompressed"),
            }

            // Create a new last block and append the time+value to it.
            self.last_chunk = TimeSeriesChunk::Uncompressed(UncompressedChunk::default());
            self.last_chunk.add_sample(&Sample {
                timestamp: time,
                value,
            })?;

            // We created a new block and pushed initial value - so set is_initial to true.
        }

        if time < self.first_timestamp {
            self.first_timestamp = time;
        }

        if time > self.last_timestamp {
            self.last_timestamp = time;
            self.last_value = value;
        }
        Ok(())
    }

    pub fn upsert_sample(
        &mut self,
        timestamp: Timestamp,
        value: f64,
        dp_override: Option<DuplicatePolicy>,
    ) -> TsdbResult<usize> {
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

        let dp_policy = dp_override.unwrap_or(
            self.duplicate_policy.unwrap_or_default(), //.unwrap_or(/* TSGlobalConfig.duplicatePolicy),
        );

        let first_ts = self.first_timestamp;
        let max_size = self.chunk_size_bytes;
        let (size, new_chunk) = if timestamp < first_ts && self.chunks.len() > 1 {
            // Upsert in an older chunk
            let chunk = self
                .chunks
                .iter_mut()
                .rev()
                .find(|chunk| timestamp >= chunk.first_timestamp());

            if let Some(chunk) = chunk {
                handle_upsert(chunk, timestamp, value, max_size, dp_policy)?
            } else {
                handle_upsert(&mut self.last_chunk, timestamp, value, max_size, dp_policy)?
            }
        } else {
            handle_upsert(&mut self.last_chunk, timestamp, value, max_size, dp_policy)?
        };

        if let Some(new_chunk) = new_chunk {
            self.chunks.push(new_chunk);
            self.chunks.sort_by_key(|chunk| chunk.first_timestamp());
        }

        self.total_samples += size as u64;
        if timestamp == self.last_timestamp {
            self.last_value = value;
        }

        Ok(size)
    }

    /// Get the time series between give start and end time (both inclusive).
    pub fn get_range(&self, start_time: Timestamp, end_time: Timestamp) -> TsdbResult<Vec<Sample>> {
        // While each TimeSeriesBlock as well as TimeSeriesBlockCompressed has data points sorted by time, in a
        // multithreaded environment, they might not be sorted across blocks. Hence, we collect all the samples in a heap,
        // and return a vector created from the heap, so that the return value is a sorted vector of data points.

        let mut result: BinaryHeap<Sample> = BinaryHeap::new();
        let mut timestamps = Vec::with_capacity(64);
        let mut values = Vec::with_capacity(64);

        self.get_range_raw(start_time, end_time, &mut timestamps, &mut values)?;
        for (ts, value) in timestamps.iter().zip(values.iter()) {
            result.push(Sample::new(*ts, *value));
        }

        Ok(result.into_sorted_vec())
    }

    pub fn get_range_raw(
        &self,
        start_time: Timestamp,
        end_time: Timestamp,
        timestamps: &mut Vec<Timestamp>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        let mut _state: bool = false;

        // Get overlapping data points from the compressed blocks.
        for chunk in self.chunks.iter() {
            if chunk.overlaps(start_time, end_time) {
                chunk.iterate_range(
                    &mut _state,
                    start_time,
                    end_time,
                    |_, times, vals, done| {
                        timestamps.extend_from_slice(times);
                        values.extend_from_slice(vals);
                        Ok(!done)
                    },
                )?;
            }
        }

        if self.last_chunk.overlaps(start_time, end_time) {
            self.last_chunk.iterate_range(
                &mut _state,
                start_time,
                end_time,
                |_, times, vals, done| {
                    timestamps.extend_from_slice(times);
                    values.extend_from_slice(vals);
                    Ok(!done)
                },
            )?;
        }
        Ok(())
    }

    #[cfg(test)]
    /// Get the last block, wrapped in RwLock.
    pub fn get_last_block(&self) -> &TimeSeriesChunk {
        &self.last_chunk
    }
    pub fn overlaps(&self, start_ts: Timestamp, end_ts: Timestamp) -> bool {
        self.last_timestamp >= start_ts && self.first_timestamp <= end_ts
    }

    pub fn trim(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<()> {
        if self.retention.as_millis() == 0 {
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

        // todo: trim partial compressed chunks
        if self.last_chunk.first_timestamp() < min_timestamp {
            let _ = self.last_chunk.remove_range(start_ts, end_ts);
        }

        let end_start = end_ts + 1; // ugly. use proper units/
                                    // trim last block
        deleted_count += self.last_chunk.remove_range(end_start, i64::MAX)?;

        self.total_samples -= deleted_count as u64;

        Ok(())
    }

    pub fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        let mut deleted_samples = 0;
        // todo: tinyvec
        let mut indexes_to_delete = Vec::new();

        for (idx, chunk) in self.chunks.iter_mut().enumerate() {
            let chunk_first_ts = chunk.first_timestamp();
            let chunk_last_ts = chunk.last_timestamp();

            // We deleted the latest samples, no more chunks/samples to delete or cur chunk start_ts is
            // larger than end_ts
            if chunk.is_empty() || chunk_first_ts > end_ts {
                // Having empty chunk means the series is empty
                break;
            }

            if chunk_last_ts < start_ts {
                continue;
            }

            let is_only_chunk =
                (chunk.num_samples() + deleted_samples) as u64 == self.total_samples;

            // Should we delete the entire chunk?
            let ts_del_condition =
                (chunk_first_ts >= start_ts && chunk_last_ts <= end_ts) && (!is_only_chunk); // We assume at least one allocated chunk in the series

            if !ts_del_condition {
                deleted_samples += chunk.remove_range(start_ts, end_ts)?;
                continue;
            }

            deleted_samples += chunk.num_samples();
            indexes_to_delete.push(idx);
        }

        self.total_samples -= deleted_samples as u64;

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

    pub fn size(&self) -> usize {
        let mut size = 0;
        for chunk in self.chunks.iter() {
            size += chunk.size();
        }
        size += self.last_chunk.size();
        size
    }

    fn get_min_timestamp(&self) -> Timestamp {
        let retention_millis = self.retention.as_millis() as i64;
        if self.last_timestamp > retention_millis {
            self.last_timestamp - retention_millis
        } else {
            0
        }
    }
}

impl Default for TimeSeries {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ts::constants::BLOCK_SIZE_FOR_TIME_SERIES;

    #[test]
    fn test_one_entry() {
        let mut ts = TimeSeries::new();
        ts.append(100, 200.0).unwrap();

        assert_eq!(ts.last_chunk.read().unwrap().len(), 1);
        let last_block_lock = ts.last_chunk.read().unwrap();
        let time_series_data_points = &*last_block_lock
            .get_time_series_data_points()
            .read()
            .unwrap();
        let data_point = time_series_data_points.get(0).unwrap();
        assert_eq!(data_point.get_time(), 100);
        assert_eq!(data_point.get_value(), 200.0);
    }

    #[test]
    fn test_block_size_entries() {
        let mut ts = TimeSeries::new();
        for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
            ts.append(i as i64, i as f64).unwrap();
        }

        // All the entries will go to 'last', as we have pushed exactly BLOCK_SIZE_FOR_TIME_SERIES entries.
        assert_eq!(ts.compressed_blocks.read().unwrap().len(), 0);
        assert_eq!(
            ts.last_chunk.read().unwrap().len(),
            BLOCK_SIZE_FOR_TIME_SERIES
        );

        for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
            let last_block_lock = ts.last_chunk.read().unwrap();
            let time_series_data_points = &*last_block_lock
                .get_time_series_data_points()
                .read()
                .unwrap();
            let data_point = time_series_data_points.get(i).unwrap();
            assert_eq!(data_point.get_time(), i as u64);
            assert_eq!(data_point.get_value(), i as f64);
        }
    }
}
