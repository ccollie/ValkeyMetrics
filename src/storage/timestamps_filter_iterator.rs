use super::time_series::TimeSeries;
use crate::common::types::{PooledTimestampVec, PooledValuesVec, Timestamp};
use crate::storage::{Chunk, Sample};
use metricsql_common::pool::{get_pooled_vec_f64, get_pooled_vec_i64};

pub struct TimestampsFilterIterator<'a> {
    series: &'a TimeSeries,
    chunk_index: usize,
    ts_index: usize,
    by_ts_args: &'a [Timestamp],
    timestamps: PooledTimestampVec,
    values: PooledValuesVec,
    start: Timestamp,
    end: Timestamp,
    err: bool,
    loaded: bool,
}

impl<'a> TimestampsFilterIterator<'a> {
    pub(crate) fn new(series: &'a TimeSeries,
                      timestamp_filters: &'a [Timestamp]
    ) -> Self {
        let start = timestamp_filters[0];
        let end = timestamp_filters[timestamp_filters.len() - 1];
        let size = 16; // ??

        let timestamps = get_pooled_vec_i64(size);
        let values = get_pooled_vec_f64(size);

        Self {
            series,
            timestamps,
            values,
            chunk_index: 0,
            start,
            end,
            by_ts_args: timestamp_filters,
            err: false,
            ts_index: 0,
            loaded : false
        }
    }

}

impl<'a> Iterator for TimestampsFilterIterator<'a> {
    type Item = Sample;
    fn next(&mut self) -> Option<Self::Item> {
        let chunks = &self.series.chunks;

        let mut should_load = false;
        while self.ts_index < self.by_ts_args.len() && self.chunk_index < self.series.chunks.len() {
            let chunk = &chunks[self.chunk_index];
            let ts = self.by_ts_args[self.ts_index];
            if chunk.last_timestamp() <= ts {
                self.chunk_index += 1;
                should_load = true;
                continue;
            }
            if chunk.first_timestamp() > ts {
                self.ts_index += 1;
                continue;
            }

            if should_load || !self.loaded {
                self.start = ts;
                self.timestamps.clear();
                self.values.clear();
                should_load = false;
                if let Err(_e) = chunk.get_range(self.start, self.end, &mut self.timestamps, &mut self.values) {
                    self.err = true;
                    return None;
                }
                self.loaded = true;
            }

            match self.timestamps.binary_search(&ts) {
                Ok(index) => {
                    self.ts_index += 1;
                    let value = self.values[index];
                    return Some(Sample { timestamp: ts, value });
                }
                Err(_index) => {
                }
            }
            self.ts_index += 1;
        }
        None
    }
}