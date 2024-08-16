use get_size::GetSize;
use metricsql_common::pool::{get_pooled_buffer, get_pooled_vec_f64, get_pooled_vec_i64};
use metricsql_runtime::{RuntimeError, Timestamp};
use pco::errors::PcoError;
use rand_distr::num_traits::Zero;

use crate::error::{TsdbError, TsdbResult};
use crate::storage::{Chunk, DuplicatePolicy, Sample, SeriesSlice};
use super::serialization::{
    compress_data,
    CompressionOptions,
    find_data_page,
    read_data_segment,
    read_date_range,
    read_timestamp,
    read_usize,
    write_data_segment
};
use crate::storage::utils::trim_to_date_range;

// todo: move elsewhere
const DEFAULT_DATA_PAGE_SIZE: usize = 2048;

#[derive(Debug, Default)]
#[derive(GetSize)]
pub struct CompressedSegment {
    pub buf: Vec<u8>,
    pub min_time: Timestamp,
    pub max_time: Timestamp,
    pub max_size: usize,
    pub page_size: Option<usize>,
    pub options: CompressionOptions,
    /// number of compressed samples
    pub count: usize,
}

impl CompressedSegment {
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn new(max_size: usize, options: Option<CompressionOptions>) -> Self {
        CompressedSegment {
            buf: Vec::with_capacity(max_size),
            min_time: 0,
            max_time: 0,
            max_size,
            page_size: None,
            options: options.unwrap_or_default(),
            count: 0,
        }
    }

    pub fn upsert_sample(
        &mut self,
        sample: &mut Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;

        if self.buf.is_empty() {
            return Ok(0)
        }

        let mut compressed = self.buf.as_slice();
        let meta = find_data_page(&mut compressed, ts)?;

        let mut compressed = &meta.start[0..];

        // size of data segment (timestamps and values)
        if meta.data_size.is_zero() {
            return Ok(0);
        }

        if meta.count == 0 {
            return Ok(0);
        }

        // accumulate all the samples in a new chunk and then swap it with the old one
        let mut timestamps = get_pooled_vec_i64(meta.count);
        let mut values = get_pooled_vec_f64(meta.count);

        read_data_segment(&mut compressed, &mut timestamps, &mut values)?;

        let mut size = timestamps.len();
        match timestamps.binary_search(&ts) {
            Ok(pos) => {
                duplicate_found = true;
                values[pos] = dp_policy.value_on_duplicate(ts, values[pos], sample.value)?;
            }
            Err(idx) => {
                timestamps.insert(idx, ts);
                values.insert(idx, sample.value);
                size += 1;
            }
        };

        let ofs = meta.offset;
        let block_size = compressed.as_ptr() as usize -  ofs;
        let mut binding = get_pooled_buffer(block_size + 16);
        let out_buf = binding.as_mut();
        write_data_segment(out_buf, &timestamps, &values, &self.options)?;

        self.buf.splice(ofs..ofs + block_size, out_buf.iter().cloned());

        Ok(size)
    }

    pub fn get_range(
        &self,
        start_ts: i64,
        end_ts: i64,
        timestamps: &mut Vec<i64>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        if self.buf.is_empty() {
            return Ok(());
        }

        // if the range is outside the block range, we can skip the entire block
        if end_ts < self.min_time || start_ts > self.max_time {
            return Ok(());
        }

        // if the range is within the block range, we can return the entire block
        if start_ts >= self.min_time && end_ts <= self.max_time {
            return self.get_all(timestamps, values);
        }

        fn trim_end(end_ts: i64, ts: &mut Vec<i64>, values: &mut Vec<f64>) {
            let end_index = ts.binary_search(&end_ts).unwrap_or_else(|x| x);
            ts.truncate(end_index);
            values.truncate(end_index);
        }

        let mut compressed = &self.buf[..];

        while !compressed.is_empty() {

            let mut ts = read_timestamp(&mut compressed)?;

            if ts > end_ts {
                break;
            }

            ts = read_timestamp(&mut compressed)?;

            // size of data segment (timestamps and values)
            let data_size= read_usize(&mut compressed, "data segment length")?;

            if ts < start_ts {
                // we can skip this page
                compressed = &compressed[data_size..];
            } else {
                let sample_count = read_usize(&mut compressed, "data length")?;

                if sample_count == 0 {
                    break;
                }

                let len = timestamps.len();
                let start_index = len;
                let new_len = len + sample_count;

                timestamps.resize(new_len, 0);
                values.resize(new_len, 0.0);

                let count = read_data_segment(&mut compressed, timestamps, values)?;
                if count != sample_count {
                    return Err(TsdbError::CannotDeserialize("incomplete data page".to_string()));
                }

                if count.is_zero() {
                    continue;
                }

                let stamps = &timestamps[start_index..];
                let first_ts = stamps[0];
                if first_ts > end_ts {
                    // new chunk is outside the range, so drain it
                    timestamps.truncate(start_index);
                    values.truncate(start_index);
                    break;
                }

                let last_ts = stamps[count - 1];

                if first_ts < start_ts {
                    // trim the start
                    let start_ofs = timestamps.binary_search(&start_ts).unwrap_or_else(|pos| pos);
                    timestamps.drain(start_index..start_ofs);
                    values.drain(start_index..start_ofs);
                }

                if last_ts > end_ts {
                    trim_end(end_ts, timestamps, values);
                    break;
                }
            }
        }

        Ok(())
    }

    fn compress(&mut self, timestamps: &[i64], values: &[f64]) -> TsdbResult<()> {
        let mut buf = Vec::with_capacity(1024);
        write_data_segment(&mut buf, timestamps, values, &self.options)?;
        self.buf.extend_from_slice(&buf);
        self.count += timestamps.len();
        self.min_time = timestamps[0];
        self.max_time = timestamps[timestamps.len() - 1];
        Ok(())
    }

    pub fn get_all(
        &self,
        timestamps: &mut Vec<i64>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        let compressed = &mut &self.buf[..];
        while !compressed.is_empty() {
            let _ = read_date_range(compressed)?;
            let _ = read_data_segment(compressed, timestamps, values)?;
        }

        Ok(())
    }

    pub fn process_range<F, State, R>(
        &self,
        start: Timestamp,
        end: Timestamp,
        state: &mut State,
        mut f: F,
    ) -> TsdbResult<R>
    where
        F: FnMut(&mut State, &[i64], &[f64]) -> TsdbResult<R>,
    {
        if self.is_empty() {
            let mut timestamps = vec![];
            let mut values = vec![];
            return f(state, &mut timestamps, &mut values);
        }

        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values = get_pooled_vec_f64(self.count);

        self.get_range(start, end, &mut timestamps, &mut values)?;
        f(state, &timestamps, &values)
    }

    pub fn remove_range(
        &mut self,
        start_ts: i64,
        end_ts: i64,
        start_offset: Option<usize>,
    ) -> TsdbResult<usize> {

        if self.buf.is_empty() {
            return Ok(0);
        }

        let ofs = start_offset.unwrap_or(0);
        let mut cursor = &self.buf[ofs..];

        let mut first_ts: Option<Timestamp> = Some(self.min_time);
        let mut last_ts: Timestamp = self.max_time;
        let mut prev_ts: Timestamp = self.max_time;

        // decompression scratch buffers to minimize allocations
        // todo: tinyvec or pooled vecs
        let mut page_t: Vec<i64> = Vec::new();
        let mut page_v: Vec<f64> = Vec::new();

        let mut delete_start_offset: Option<usize> = None;
        let mut delete_count = 0;

        let mut bytes_shifted = 0;

        while !cursor.is_empty() {

            let start_offset = cursor.as_ptr() as usize - self.buf.as_ptr() as usize;

            let segment_start_ts = read_timestamp(&mut cursor)?;

            if segment_start_ts > end_ts {
                break;
            }

            let segment_end_ts = read_timestamp(&mut cursor)?;

            // size of data segment (timestamps and values)
            let data_size= read_usize(&mut cursor, "data segment length")?;

            if segment_start_ts < start_ts {
                // we can skip this data
                cursor = &cursor[data_size..];
            } else {

                if delete_start_offset.is_none() {
                    delete_start_offset = Some(start_offset);
                }

                if first_ts.is_none() {
                    first_ts = Some(segment_start_ts);
                }

                let sample_count = read_usize(&mut cursor, "sample count")?;

                if sample_count == 0 {
                    prev_ts = segment_end_ts;
                    continue;
                }

                let is_last_chunk_deleted = cursor.is_empty();

                // if we are completely within the range, we can skip this page
                if segment_start_ts >= start_ts && segment_end_ts <= end_ts {
                    let saved_size = self.buf.len();
                    let _ = self.buf.drain(start_offset..start_offset + data_size);
                    cursor = &self.buf[start_offset..];
                    delete_count += sample_count;
                    bytes_shifted += self.buf.len() - saved_size;
                    last_ts = prev_ts;
                    first_ts = None; // signal that we need to update the first timestamp
                    // todo: what to do with last_ts
                    continue;
                }

                page_v.resize(sample_count, 0.0);
                page_t.resize(sample_count, 0);

                // we need to filter and append this data
                let count = read_data_segment(&mut cursor, &mut page_t, &mut page_v)?;
                if page_t.is_empty() {
                    // todo; different error
                    return Err(TsdbError::CannotDeserialize("incomplete data page".to_string()));
                }
                if count != sample_count {
                    return Err(TsdbError::CannotDeserialize("incomplete data page".to_string()));
                }

                let start = page_t[0];
                let end = page_t[count - 1];

                if start > end_ts {
                    break;
                }

                if let Some((ts_slice, value_slice)) = trim_to_date_range(&page_t, &page_v, start_ts, end_ts) {
                    let old_size = self.buf.len();
                    let mut buf = Vec::with_capacity(data_size);
                    let bytes_written = write_data_segment(&mut buf, ts_slice, value_slice, &self.options)?;

                    self.buf.splice(start_offset..start_offset + data_size, buf.into_iter());
                    cursor = &self.buf[start_offset + bytes_written..];

                    let count = page_t.len() - ts_slice.len();
                    delete_count += count;
                    bytes_shifted += self.buf.len() - old_size;
                    last_ts = ts_slice[ts_slice.len() - 1];
                } else {

                }

                if end >= end_ts {
                    break;
                }

                prev_ts = segment_end_ts;
            }
        }

        // Update last timestamp if needed
        if end_ts >= self.max_time && start_ts <= self.max_time {
            self.max_time = last_ts;
        }
        // todo: update max and min timestamps

        Ok(delete_count)
    }

    pub fn split(&mut self, size_hint: Option<usize>) -> TsdbResult<Self>
    {
        let buf_len = self.buf.len();
        let mid = buf_len / 2;

        let mut split_at: usize = 0;

        let mut cursor = &self.buf[..];
        let mut last_segment_offset: usize = 0;

        while !cursor.is_empty() {
            let ofs = cursor.as_ptr() as usize - self.buf.as_ptr() as usize;
            last_segment_offset = ofs;
            if let Some((_, _)) = read_date_range(&mut cursor)? {
                if ofs >= mid {
                    split_at = ofs;
                    break;
                }
                let data_size = read_usize(&mut cursor, "data segment length")?;
                cursor = &cursor[data_size..];
            } else {
                break;
            }
        }

        // check if it's imbalanced. As an example, consider 2 pages where the first page has 1 sample and
        // the second page has 1000 samples
        let second_half = self.buf.len() - split_at;
        let delta = (second_half as i64 - split_at as i64).abs();
        // if the difference in size is greater than a fifth of the buffer size, we consider it imbalanced
        if delta > buf_len as i64 / 5 {
            // do it the hard way. Uncompress the entire buffer and split

            let count = size_hint.unwrap_or(DEFAULT_DATA_PAGE_SIZE);
            let mut timestamp_buf = get_pooled_vec_i64(count);
            let mut value_buf = get_pooled_vec_f64(count);

            let mut ts_vec = &mut timestamp_buf;
            let mut value_vec = &mut value_buf;
            self.get_all(ts_vec, value_vec)?;

            let slice = SeriesSlice::new(ts_vec, value_vec);
            let (left, right) = slice.split_at(mid);

            let mut left_buf = Vec::new();
            let mut right_buf = Vec::new();

            // todo: rayon.join
            compress_data(&mut left_buf, left.timestamps, left.values, self.page_size, &self.options)?;
            compress_data(&mut right_buf, right.timestamps, right.values, self.page_size, &self.options)?;

            self.max_time = left.last_timestamp();
            self.count = left.len();
            self.buf = left_buf;

            let right_result = Self {
                buf: right_buf,
                min_time: right.first_timestamp(),
                max_time: right.last_timestamp(),
                max_size: self.max_size,
                page_size: self.page_size,
                options: self.options.clone(),
                count: right.len(),
            };

            return Ok(right_result);
        }

        // do a simple split

        if split_at > 0 {
            let right = self.buf.split_off(split_at);

            // todo: rayon ?

            let mut right_result = Self::default();
            right_result.max_size = self.max_size;
            right_result.buf = right;
            right_result.min_time = self.min_time;
            right_result.page_size = self.page_size.clone();
            right_result.options = self.options.clone();

            return Ok(right_result);
        }

        Err(TsdbError::EmptyTimeSeriesBlock())
    }
}

impl Chunk for CompressedSegment {
    fn first_timestamp(&self) -> Timestamp {
        self.min_time
    }

    fn last_timestamp(&self) -> Timestamp {
        self.max_time
    }

    fn num_samples(&self) -> usize {
        self.count
    }

    fn last_value(&self) -> f64 {
        todo!()
    }

    fn size(&self) -> usize {
        self.get_size()
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
       self.remove_range(start_ts, end_ts, None)
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        // add to the last page
        // if self.is_full() {
        //     return Err(TsdbError::CapacityFull(self.max_size));
        // }
        if self.is_empty() {
            let timestamps = vec![sample.timestamp];
            let values = vec![sample.value];
            return self.compress(&timestamps, &values);
        }
        let mut compressed = self.buf.as_slice();
        let meta = find_data_page(&mut compressed, sample.timestamp)?;
        let count = meta.count.min(4);
        let mut timestamps = get_pooled_vec_i64(count);
        let mut values = get_pooled_vec_f64(count);
        let mut compressed = &meta.start[0..];
        read_data_segment(&mut compressed, &mut timestamps, &mut values)?;
        timestamps.push(sample.timestamp);
        values.push(sample.value);
        self.compress(&timestamps, &values)
    }

    fn get_range(&self, start: Timestamp, end: Timestamp, timestamps: &mut Vec<i64>, values: &mut Vec<f64>) -> TsdbResult<()> {
        self.get_range(start, end, timestamps, values)
    }

    fn upsert_sample(&mut self, sample: &mut Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        self.upsert_sample(sample, dp_policy)
    }

    fn split(&mut self) -> TsdbResult<Self> {
        self.split(None)
    }
}

fn map_err(e: PcoError) -> RuntimeError {
    RuntimeError::SerializationError(e.to_string())
}

#[cfg(test)]
mod tests {
    // Single series
}
