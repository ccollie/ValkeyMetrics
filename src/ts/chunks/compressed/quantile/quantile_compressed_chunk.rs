use super::compress::compress_series;
use super::DecompressorContext;
use crate::ts::chunks::uncompressed::UncompressedChunk;
use crate::ts::chunks::{ChunkRangeVisitor, ChunkRangeVisitorContinuation, DataPage, TimesSeriesBlock};
use crate::ts::utils::trim_data;
use crate::ts::{handle_duplicate_sample, DuplicatePolicy, DuplicateStatus, Sample, Timestamp};
use serde::{Deserialize, Serialize};
use crate::error::{TsdbError, TsdbResult};
use crate::ts::chunks::compressed::common::{CompressedData, PageMetadata};
use crate::ts::chunks::compressed::quantile::compress::DATA_PAGE_SIZE;


#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuantileCompressedChunk {
    pub compressed: CompressedData
}

impl QuantileCompressedChunk {
    pub fn new(timestamps: &[i64], values: &[f64]) -> TsdbResult<Self> {
        // todo: calculate init buffer size
        // todo: ensure timestamps.len() == values.len()
        let chunk = create_chunk(timestamps, values)?;
        Ok(QuantileCompressedChunk {
            compressed: chunk,
        })
    }

    pub fn len(&self) -> usize {
        self.compressed.count
    }
    pub fn is_empty(&self) -> bool {
        self.compressed.count == 0
    }
    pub fn overlaps(&self, start: i64, end: i64) -> bool {
        self.compressed.overlaps(start, end)
    }
    pub fn get_timestamp_range(&self) -> Option<(i64, i64)> {
        self.compressed.get_timestamp_range()
    }

    pub fn buf(&self) -> &[u8] {
        &self.compressed.data
    }

    pub fn pages(&self) -> &[PageMetadata] {
        &self.compressed.pages
    }

    /// Get the data points in the specified range (both range_start_time and range_end_time inclusive).
    pub fn get_samples_in_range(&self, start_time: Timestamp, end_time: Timestamp) -> Vec<Sample> {
        let mut result = Vec::new();

        self.iterate_range(start_time, end_time, &mut result, |result, timestamps, values, _| {
            for (time, val) in timestamps.iter().zip(values.iter()) {
                result.push(Sample::new(*time, *val));
            }
            Ok(false)
        }).unwrap(); // Safety: `iterate_range` returns the result of the closure, which is always `Ok(false)`.

        result
    }

    pub fn get_page_indices(&self, start: Timestamp, end: Timestamp) -> (usize, usize) {
        let pages = self.pages();

        let start_idx =  match pages.binary_search_by(|x| x.t_min.cmp(&start)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        let end_idx = pages
            .iter()
            .rev()
            .position(|page| page.t_max <= end)
            .unwrap_or(0);

        (start_idx, end_idx)
    }

    pub(crate) fn iterate_range<F, State>(&self, start: Timestamp, end: Timestamp, state: &mut State, mut f: F) -> TsdbResult<()>
    where F: FnMut(&mut State, &[i64], &[f64], bool) -> TsdbResult<bool> {
        let pages = self.pages();

        let (start_idx, end_idx) = self.get_page_indices(start, end);

        let done = start_idx >= pages.len() || end_idx >= pages.len();
        if !done {
            // todo(perf): use pool
            let mut timestamps = Vec::with_capacity(DATA_PAGE_SIZE);
            let mut values = Vec::with_capacity(DATA_PAGE_SIZE);

            let buf = self.buf();
            let mut decompressed = DecompressorContext::new(buf)?;
            for page_idx in start_idx..=end_idx {
                let page = &pages[page_idx];
                let _ = decompressed.get_page_data(page.offset, &mut timestamps, &mut values)?;
                if !page.overlaps(start, end) {
                    trim_data(&mut timestamps, &mut values, start, end);
                }
                if f(state, &timestamps, &values, page_idx == end_idx)? {
                    break;
                }
            }
        } else {
            let mut timestamps = vec![];
            let mut values = vec![];
            f(state, &mut timestamps, &mut values, true)?;
        }
        Ok(())
    }

    pub fn visit_range<V: ChunkRangeVisitor>(&self, start: Timestamp, end: Timestamp, visitor: &mut V) -> TsdbResult<()> {

        if let Ok(v) = visitor.pre_visit(self) {
            if v == ChunkRangeVisitorContinuation::Stop {
                return Ok(());
            }
        }

        self.iterate_range(start, end, visitor, |visitor, timestamps, values, finished| {
            let page = DataPage::new(timestamps, values);
            visitor.visit(&page, finished)
                .and_then(|v| Ok(v == ChunkRangeVisitorContinuation::Stop))
        })?;

        let _ = visitor.post_visit(self);
        Ok(())
    }

    pub fn iter(
        &self,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> impl Iterator<Item=Sample> {
        let values = self.get_samples_in_range(start_ts, end_ts);
        values.into_iter()
    }
}

impl TimesSeriesBlock for QuantileCompressedChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.compressed.first_timestamp()
    }
    fn last_timestamp(&self) -> Timestamp {
        self.compressed.last_timestamp()
    }
    fn last_value(&self) -> f64 {
        self.compressed.last_value()
    }
    fn num_samples(&self) -> usize {
        self.compressed.count
    }
    fn size(&self) -> usize {
        self.compressed.size()
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        // todo: better capacity calculation.
        // Idea:: iterated over pages and calculate total size
        // / avg(t-max - tmin)

        struct State {
            timestamps: Vec<i64>,
            values: Vec<f64>
        }
        let mut data: State = State {
            timestamps: Vec::with_capacity(128),
            values: Vec::with_capacity(128)
        };

        self.iterate_range(start_ts, end_ts, &mut data, |data, times, values, _| {
            data.timestamps.extend_from_slice(times);
            data.values.extend_from_slice(values);
            Ok(false)
        })?;

        let deleted_count = self.num_samples() - data.values.len();
        if deleted_count == 0 {
            return Ok(0);
        }

        // todo: calculate initial capacity
        let compressed = create_chunk( &data.timestamps, &data.values)?;
        self.compressed = compressed;

        Ok(deleted_count)
    }

    fn upsert_sample(
        &mut self,
        sample: &mut Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let num_samples = self.num_samples();

        let mut duplicate_found = false;

        // this compressed method does not do streaming compressed, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old one
        struct State {
            duplicate_found: bool,
            timestamps: Vec<Timestamp>,
            values: Vec<f64>
        }

        let mut state = State {
            duplicate_found: false,
            timestamps: Vec::with_capacity(num_samples + 1),
            values: Vec::with_capacity(num_samples + 1)
        };

        self.iterate_range(0, i64::MAX, &mut state, |
            state, timestamps, values, _
        | {
            let first = timestamps[0];
            let last = timestamps[timestamps.len() - 1];
            if ts >= first && ts <= last {
                let dupe_pos = timestamps.iter().position(|&t| t == ts);
                if let Some(pos) = dupe_pos {
                    let old_sample = Sample {
                        timestamp: timestamps[pos],
                        value: values[pos],
                    };
                    let cr = handle_duplicate_sample(dp_policy, old_sample, sample);
                    if cr != DuplicateStatus::Ok {
                        return Err(TsdbError::DuplicateSample(sample.timestamp.to_string()));
                    }
                    duplicate_found = true;
                    // value changed. write it to the accumulator
                    state.values.extend_from_slice(&values[0..pos]);
                    state.values.push(sample.value);
                    state.values.extend_from_slice(&values[pos + 1..]);
                    state.timestamps.extend(timestamps);
                    return Ok(false);
                }
            }

            state.timestamps.extend(timestamps);
            state.values.extend(values);

            Ok(false)
        }).unwrap(); // Safety: `iterate_range` returns the result of the closure, which is always `Ok(false)`.

        let mut size = state.timestamps.len();
        if duplicate_found {
            size -= 1;
        }
        // todo: specify config options
        self.compressed = create_chunk(&state.timestamps, &state.values)?;

        Ok(size)
    }

    fn split(&mut self) -> TsdbResult<Self> {
        let mid = self.num_samples() / 2;

        // this compression method does not do streaming compression, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old one
        struct State {
            left: Option<CompressedData>,
            timestamps: Vec<Timestamp>,
            values: Vec<f64>
        }

        let mut state = State {
            left: None,
            timestamps: Vec::with_capacity(mid),
            values: Vec::with_capacity(mid)
        };

        self.iterate_range(0, i64::MAX, &mut state, |state, timestamps, values, _| {
            // slight optimization. if the first iteration gets us at least half of the data points,
            // we can skip one round of allocations
            if state.left.is_none() && state.timestamps.len() >= mid {
                let remaining = state.timestamps.len() - mid;
                let (left_ts, _) = state.timestamps.split_at(mid);
                let (left_values, _) = state.values.split_at(mid);
                let compressed = create_chunk(left_ts, left_values)?;
                state.left = Some(compressed);
                state.values.rotate_left(mid);
                state.values.truncate(remaining);
                state.timestamps.rotate_left(mid);
                state.timestamps.truncate(remaining);
            } else {
                state.timestamps.extend(timestamps);
                state.values.extend(values);
            }
            Ok(false)
        })?;

        if let Some(left) = state.left {
           self.compressed = left;
        } else {
            // todo: more specific error
            return Err(TsdbError::EncodingError("Error splitting chunk".to_string()))
        }

        // todo: specify config options
        let chunk = create_chunk(&state.timestamps, &state.values)?;
        Ok(
            QuantileCompressedChunk {
                compressed: chunk,
            }
        )
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        todo!()
    }
}

impl Eq for QuantileCompressedChunk {}

impl TryFrom<&UncompressedChunk> for QuantileCompressedChunk {
    type Error = TsdbError;

    /// Compress the given time series block.
    fn try_from(time_series_block: &UncompressedChunk) -> Result<Self, Self::Error> {
        if time_series_block.is_empty() {
            return Err(TsdbError::EmptyTimeSeriesBlock());
        }

        let chunk = QuantileCompressedChunk::new(
            &time_series_block.timestamps[0..],
            &time_series_block.values,
        )?;

        Ok(chunk)
    }
}

fn estimate_compressed_size(count: usize) -> usize {
    (count * 16) / 6
}

fn create_chunk(
    timestamps: &[i64],
    values: &[f64],
) -> TsdbResult<CompressedData> {
    let to_reserve: usize = estimate_compressed_size(timestamps.len());
    let mut data = Vec::with_capacity(to_reserve);
    let pages = compress_series(&mut data, timestamps, values)?;
    data.shrink_to_fit();
    let last_value = values[values.len() - 1];
    Ok(CompressedData {
        data,
        pages,
        count: timestamps.len(),
        last_value,
    })
}