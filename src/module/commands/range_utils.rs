use crate::aggregators::{AggOp, Aggregator};
use crate::common::types::{Sample, Timestamp};
use crate::storage::time_series::TimeSeries;
use ahash::AHashMap;
use crate::module::types::{AggregationOptions, BucketTimestamp, RangeAlignment, RangeOptions, TimestampRange, ValueFilter};

pub struct GroupMeta<'a> {
    pub groups: AHashMap<String, Vec<&'a TimeSeries>>,
}

impl<'a> GroupMeta<'a> {
    pub fn new() -> Self {
        Self {
            groups: Default::default(),
        }
    }

    pub fn add_series(&mut self, series: &'a TimeSeries, label_name: &str) {
        if let Some(value) = series.label_value(label_name) {
            match self.groups.get_mut(value) {
                Some(group) => {
                    group.push(series)
                }
                None => {
                    let mut group = Vec::with_capacity(8);
                    group.push(series);
                    self.groups.insert(value.to_string(), group);
                }
            }
        }
    }
}

pub(crate) struct AggrIterator {
    aggregator: Aggregator,
    time_delta: i64,
    bucket_ts: BucketTimestamp,
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
    last_timestamp: Timestamp,
    bucket_right_ts: Timestamp,
    timestamp_alignment: i64,
    count: Option<usize>,
    empty: bool
}

impl AggrIterator {
    fn normalize_bucket_start(&mut self) -> Timestamp {
        self.last_timestamp = self.last_timestamp.min(0);
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

    pub fn calculate(&mut self, iterator: impl Iterator<Item=Sample>) -> Vec<Sample> {
        let time_delta = self.time_delta;
        self.bucket_right_ts = self.last_timestamp + time_delta;
        self.normalize_bucket_start();
        let mut buckets: Vec<Sample> = Default::default();
        let count = self.count.unwrap_or(usize::MAX - 1);

        for sample in iterator {
            let timestamp = sample.timestamp;
            let value = sample.value;

            if timestamp >= self.bucket_right_ts {
                let time_delta = self.time_delta;

                buckets.push(self.finalize_bucket());
                if buckets.len() >= count {
                    return buckets;
                }

                let last_timestamp = calc_bucket_start(timestamp, time_delta, self.timestamp_alignment);
                if self.empty {
                    let first_bucket = self.bucket_right_ts;
                    let last_bucket = (last_timestamp - time_delta).max(0);

                    let has_empty_buckets = first_bucket < last_timestamp;
                    if has_empty_buckets {
                        self.fill_empty_buckets(&mut buckets, first_bucket, last_bucket, count);
                        if buckets.len() >= count {
                            return buckets;
                        }
                    }
                }

                self.last_timestamp = last_timestamp;
                self.bucket_right_ts = last_timestamp + time_delta;
                self.normalize_bucket_start();
            }

            self.aggregator.update(value);
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

// todo: move elsewhere, better name
pub(super) fn get_range_internal(
    series: &TimeSeries,
    date_range: &TimestampRange,
    check_retention: bool,
    timestamp_filter: &Option<Vec<Timestamp>>,
    value_filter: &Option<ValueFilter>
) -> Vec<Sample> {
    let (start_timestamp, end_timestamp) = date_range.get_series_range(series, check_retention);

    let mut samples = if let Some(timestamps) = timestamp_filter {
        series.samples_by_timestamps(&timestamps)
            .unwrap_or_else(|e| {
                // todo: properly handle error and log
                vec![]
            })
            .into_iter()
            .filter(|sample| sample.timestamp >= start_timestamp && sample.timestamp <= end_timestamp)
            .collect()
    } else {
        series.range_iter(start_timestamp, end_timestamp).collect::<Vec<_>>()
    };

    if let Some(filter) = value_filter {
        samples.retain(|s| s.value >= filter.min && s.value <= filter.max);
    };

    samples
}

pub(crate) fn get_range(series: &TimeSeries, args: &RangeOptions, check_retention: bool) -> Vec<Sample> {
    let mut range = get_range_internal(
                                   series,
                                   &args.date_range,
                                   check_retention,
                                   &args.timestamp_filter,
                                   &args.value_filter);
    if let Some(aggr_options) = &args.aggregation {
        let mut aggr_iterator = get_series_aggregator(series, args, aggr_options, check_retention);
        aggr_iterator.calculate(range.into_iter())
    } else {
        if let Some(count) = args.count {
            range.truncate(count);
        }
        range
    }
    // group by
}

fn get_series_aggregator(
    series: &TimeSeries,
    args: &RangeOptions,
    aggr_options: &AggregationOptions,
    check_retention: bool
) -> AggrIterator {
    let (start_timestamp, end_timestamp) = args.date_range.get_series_range(series, check_retention);

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
        timestamp_alignment,
        empty: aggr_options.empty,
        aggregator: aggr_options.aggregator.clone(),
        time_delta: aggr_options.time_delta,
        bucket_ts: aggr_options.timestamp_output,
        start_timestamp,
        end_timestamp,
        last_timestamp: 0,
        count: args.count,
        bucket_right_ts: 0,
    }
}

pub fn group_series_by_label_value<'a>(series: &'a Vec<&TimeSeries>, label: &str) -> AHashMap<String, Vec<&'a TimeSeries>> {
    let mut grouped: AHashMap<String, Vec<&TimeSeries>> = AHashMap::new();

    for ts in series.iter() {
        if let Some(label_value) = ts.label_value(label) {
            if let Some(list) = grouped.get_mut(label_value) {
                list.push(ts);
            } else {
                grouped.insert(label_value.to_string(), vec![ts]);
            }
        }
    }

    grouped
}
