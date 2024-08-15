use crate::aggregators::{AggOp, Aggregator};
use crate::storage::time_series::TimeSeries;
use crate::storage::{AggregationOptions, BucketTimestamp, RangeAlignment, RangeOptions, Sample, Timestamp};

pub(crate) struct AggrIterator {
    iterator: Box<dyn Iterator<Item= Sample>>,
    aggregator: Aggregator,
    time_delta: i64,
    bucket_ts: BucketTimestamp,
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
    last_timestamp: Timestamp,
    timestamp_alignment: i64,
    count: Option<usize>,
    empty: bool
}

impl AggrIterator {
    fn normalize_bucket_start(&mut self) -> Timestamp {
        self.last_timestamp = self.last_timestamp.max(0);
        self.last_timestamp
    }

    fn fill_empty_buckets(&self,
                          samples: &mut Vec<Sample>,
                          first_bucket_ts: Timestamp,
                          end_bucket_ts: Timestamp,
                          max_count: usize) {
        let time_delta = self.time_delta;

        debug_assert!(end_bucket_ts >= first_bucket_ts);
        assert_eq!((end_bucket_ts - first_bucket_ts) % time_delta, 0);

        let mut empty_bucket_count = (((end_bucket_ts - first_bucket_ts) / time_delta) + 1) as usize;
        let remainder = max_count - samples.len();

        empty_bucket_count = empty_bucket_count.min(remainder);

        debug_assert!(empty_bucket_count > 0);

        let value = self.aggregator.empty_value();
        for _ in 0..empty_bucket_count {
            samples.push(Sample {
                timestamp: end_bucket_ts,
                value,
            });
        }
    }

    fn calc_bucket_start(&self) -> Timestamp {
        self.bucket_ts.calculate(self.last_timestamp, self.time_delta)
    }

    fn finalize_bucket(&mut self) -> Sample {
        let value = self.aggregator.finalize();
        let timestamp = self.calc_bucket_start();
        self.aggregator.reset();
        Sample {
            timestamp,
            value,
        }
    }

    pub fn calculate(&mut self) -> Vec<Sample> {
        let time_delta = self.time_delta;
        let mut bucket_right_ts = self.last_timestamp + time_delta;
        self.normalize_bucket_start();
        let mut buckets: Vec<Sample> = Default::default();
        let count = self.count.unwrap_or(usize::MAX - 1);

        while let Some(sample) = self.iterator.next() {
            let timestamp = sample.timestamp;
            let value = sample.value;

            // (1) time_delta > 0,
            // (2) self.aggregation_last_timestamp > sample.timestamp -
            // time_delta (3) self.aggregation_last_timestamp = samples.timestamps[0]
            // - mod where 0 <= mod from (1)+(2) context_scope > chunk->samples.timestamps[0]
            // from (3) chunk->samples.timestamps[0] >= self.aggregation_last_timestamp so the
            // following condition should always be false on the first iteration
            if timestamp >= bucket_right_ts {

                buckets.push(self.finalize_bucket());
                if buckets.len() >= count {
                    return buckets;
                }

                self.last_timestamp = calc_bucket_start(timestamp, time_delta, self.timestamp_alignment);
                if self.empty {
                    let first_bucket = bucket_right_ts;
                    let last_bucket = (self.last_timestamp - time_delta).max(0);

                    let has_empty_buckets = if first_bucket >= self.last_timestamp {
                        false
                    } else {
                        true
                    };

                    if has_empty_buckets {
                        self.fill_empty_buckets(&mut buckets, first_bucket, last_bucket, count);
                        if buckets.len() >= count {
                            return buckets;
                        }
                    }
                }

                bucket_right_ts = self.last_timestamp + time_delta;
                self.normalize_bucket_start();

                self.aggregator.update(value);
            }
        }
        // todo: write out last bucket value
        buckets.truncate(count);

        buckets

    }
}

/// Calculate the beginning of aggregation bucket
#[inline]
pub(crate) fn calc_bucket_start(ts: Timestamp, bucket_duration: i64, timestamp_alignment: i64) -> Timestamp {
    let timestamp_diff = ts - timestamp_alignment;
    ts - ((timestamp_diff % bucket_duration + bucket_duration) % bucket_duration)
}

#[inline]
fn bucket_start_normalize(bucket_ts: Timestamp) -> Timestamp {
    bucket_ts.max(0)
}

pub(crate) fn get_range_iter_internal<'a>(
    series: &'a TimeSeries,
    args: &'a RangeOptions,
    start_timestamp: Timestamp,
    end_timestamp: Timestamp
) -> Box<dyn Iterator<Item=Sample> + 'a> {
    if let Some(filter) = &args.filter {
        if let Some(timestamps) = &filter.timestamps {
            return Box::new(series.timestamp_filter_iter(timestamps))
        }
    }
    Box::new(series.iter_range(start_timestamp, end_timestamp))
}

pub(crate) fn get_range_iter<'a>(
    series: &'a TimeSeries,
    args: &'a RangeOptions,
    check_retention: bool
) -> impl Iterator<Item=Sample> + 'a  {
    let (start_timestamp, end_timestamp) = get_date_range(series, args, check_retention);
    get_range_iter_internal(series, args, start_timestamp, end_timestamp)
}

fn get_range_internal(
    series: &TimeSeries,
    args: &RangeOptions,
    check_retention: bool
) -> Vec<Sample> {
    let iterator = get_range_iter(series, args, check_retention);
    let is_aggregation = args.aggregation.is_some();
    let mut samples = if let Some(filter) = args.get_value_filter() {
        iterator
            .into_iter()
            .filter(|s| s.value >= filter.min && s.value <= filter.max)
            .collect::<Vec<_>>()
    } else {
        iterator.collect::<Vec<_>>()
    };

    if !is_aggregation {
        if let Some(count) = args.count {
            samples.truncate(count);
        }
    }
    samples
}

fn get_date_range(series: &TimeSeries, args: &RangeOptions, check_retention: bool) -> (Timestamp, Timestamp) {
    // In case a retention is set shouldn't return chunks older than the retention
    let mut start_timestamp = args.start.to_series_timestamp(series);
    let end_timestamp = args.end.to_series_timestamp(series);
    if check_retention && !series.retention.is_zero() {
        // todo: check for i64 overflow
        let retention_ms = series.retention.as_millis() as i64;
        let earliest = series.last_timestamp - retention_ms;
        start_timestamp = start_timestamp.max(earliest);
    }
    (start_timestamp, end_timestamp)
}

pub(crate) fn get_range(series: &TimeSeries, args: &RangeOptions, check_retention: bool) -> Vec<Sample> {
    let range = get_range_internal(series, args, check_retention);
    if let Some(aggr_options) = &args.aggregation {
        let (start_timestamp, end_timestamp) = get_date_range(series, args, check_retention);
        let mut timestamp_alignment: Timestamp = 0;
        if let Some(alignment) = &args.alignment {
            timestamp_alignment = match alignment {
                RangeAlignment::Default => 0,
                RangeAlignment::Start => start_timestamp,
                RangeAlignment::End => end_timestamp,
                RangeAlignment::Timestamp(ts) => *ts,
            }
        }

        let iterator = get_range_iter_internal(series, args, start_timestamp, end_timestamp);
        let mut iter = AggrIterator {
            iterator,
            timestamp_alignment,
            empty: aggr_options.empty,
            aggregator: aggr_options.aggregator.clone(),
            time_delta: aggr_options.time_delta,
            bucket_ts: aggr_options.timestamp_output,
            start_timestamp,
            end_timestamp,
            last_timestamp: 0,
            count: args.count,
        };

        iter.calculate()

    } else {
        range
    }
}
pub(crate) fn get_series_aggregator(series: &TimeSeries, args: &RangeOptions, aggr_options: &AggregationOptions, check_retention: bool) -> AggrIterator {
    let (start_timestamp, end_timestamp) = get_date_range(series, args, check_retention);
    let iterator = get_range_iter_internal(series, args, start_timestamp, end_timestamp);

    let mut timestamp_alignment: Timestamp = 0;
    if let Some(alignment) = &args.alignment {
        timestamp_alignment = match alignment {
            RangeAlignment::Default => 0,
            RangeAlignment::Start => start_timestamp,
            RangeAlignment::End => end_timestamp,
            RangeAlignment::Timestamp(ts) => *ts,
        }
    }

    AggrIterator {
        iterator: Box::new(iterator),
        timestamp_alignment,
        empty: aggr_options.empty,
        aggregator: aggr_options.aggregator.clone(),
        time_delta: aggr_options.time_delta,
        bucket_ts: aggr_options.timestamp_output,
        start_timestamp,
        end_timestamp,
        last_timestamp: 0,
        count: args.count,
    }
}
