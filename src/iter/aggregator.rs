use crate::aggregators::{AggOp, Aggregator};
use crate::common::types::{Sample, Timestamp};
use crate::module::types::{AggregationOptions, BucketTimestamp};

#[derive(Debug)]
pub(crate) struct AggrIterator {
    aggregator: Aggregator,
    time_delta: i64,
    bucket_ts: BucketTimestamp,
    last_timestamp: Timestamp,
    aligned_timestamp: Timestamp,
    count: usize,
    report_empty: bool,
    bucket_right_ts: Timestamp,
}

impl AggrIterator {
    pub(crate) const DEFAULT_COUNT: usize = usize::MAX - 1;

    pub(crate) fn new(options: &AggregationOptions, aligned_timestamp: Timestamp, count: Option<usize>) -> Self {
        AggrIterator {
            aligned_timestamp,
            report_empty: options.empty,
            aggregator: options.aggregator.clone(),
            time_delta: options.time_delta,
            bucket_ts: options.timestamp_output,
            count: count.unwrap_or(AggrIterator::DEFAULT_COUNT),
            last_timestamp: 0,
            bucket_right_ts: 0,
        }
    }

    fn add_empty_buckets(&self, samples: &mut Vec<Sample>, first_bucket_ts: Timestamp, end_bucket_ts: Timestamp) {
        let empty_bucket_count = self.calculate_empty_bucket_count(first_bucket_ts, end_bucket_ts);
        let value = self.aggregator.empty_value();
        for _ in 0..empty_bucket_count {
            samples.push(Sample { timestamp: end_bucket_ts, value });
        }
    }

    fn buckets_in_range(&self, start_ts: Timestamp, end_ts: Timestamp) -> usize {
        (((end_ts - start_ts) / self.time_delta) + 1) as usize
    }

    fn calculate_empty_bucket_count(&self, first_bucket_ts: Timestamp, end_bucket_ts: Timestamp) -> usize {
        let total_empty_buckets = self.buckets_in_range(first_bucket_ts, end_bucket_ts);
        let remaining_capacity = self.count - total_empty_buckets;
        total_empty_buckets.min(remaining_capacity)
    }

    fn calculate_bucket_start(&self) -> Timestamp {
        self.bucket_ts.calculate(self.last_timestamp, self.time_delta)
    }

    fn finalize_current_bucket(&mut self) -> Sample {
        let value = self.aggregator.finalize();
        let timestamp = self.calculate_bucket_start();
        self.aggregator.reset();
        Sample { timestamp, value }
    }

    pub fn calculate(&mut self, iterator: impl Iterator<Item=Sample>) -> Vec<Sample> {
        let mut buckets = Vec::new();
        let mut iterator = iterator;

        if let Some(first) = iterator.by_ref().next() {
            self.update_bucket_timestamps(first.timestamp);
            self.aggregator.update(first.value);
        } else {
            return buckets;
        }

        for sample in iterator {
            if self.should_finalize_bucket(sample.timestamp) {
                buckets.push(self.finalize_current_bucket());
                if buckets.len() >= self.count {
                    return buckets;
                }
                self.update_bucket_timestamps(sample.timestamp);
                if self.report_empty {
                    self.finalize_empty_buckets(&mut buckets);
                }
            }
            self.aggregator.update(sample.value);
        }

        // todo: handle last bucket
        buckets.truncate(self.count);
        buckets
    }

    fn should_finalize_bucket(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.bucket_right_ts
    }

    fn update_bucket_timestamps(&mut self, timestamp: Timestamp) {
        self.last_timestamp = self.calc_bucket_start(timestamp).max(0) as Timestamp;
        self.bucket_right_ts = self.last_timestamp + self.time_delta;
    }

    fn finalize_empty_buckets(&self, buckets: &mut Vec<Sample>) {
        let first_bucket = self.bucket_right_ts;
        let last_bucket = self.last_timestamp - self.time_delta;
        if first_bucket < self.last_timestamp {
            self.add_empty_buckets(buckets, first_bucket, last_bucket);
        }
    }

    fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        let timestamp_diff = ts - self.aligned_timestamp;
        let delta = self.time_delta;
        ts - ((timestamp_diff % delta + delta) % delta)
    }
}

pub fn aggregate(options: &AggregationOptions,
                 aligned_timestamp: Timestamp,
                 iter: impl Iterator<Item=Sample>,
                 count: Option<usize>) -> Vec<Sample> {
    let mut aggr_iter = AggrIterator::new(options, aligned_timestamp, count);
    aggr_iter.calculate(iter)
}