use crate::common::types::{Sample, Timestamp};
use crate::iterators::aggregator::aggregate;
use crate::join::{JoinIterator, JoinOptions, JoinValue};
use crate::series::{get_series_range_filtered, TimeSeries};
use joinkit::EitherOrBoth;
use metricsql_parser::binaryop::BinopFunc;

// naming is hard :-)
/// Result of a join operation
pub enum JoinResultType {
    Samples(Vec<Sample>),
    Values(Vec<JoinValue>),
}

pub fn process_join(
    left_series: &TimeSeries,
    right_series: &TimeSeries,
    options: &JoinOptions,
) -> JoinResultType {
    let (left_samples, right_samples) = chili::Scope::global().join(
        |_| fetch_samples(left_series, options),
        |_| fetch_samples(right_series, options),
    );
    join_internal(&left_samples, &right_samples, options)
}

fn join_internal(left: &[Sample], right: &[Sample], options: &JoinOptions) -> JoinResultType {
    let join_iter = JoinIterator::new(left, right, options.join_type);

    if let Some(op) = options.reducer {
        let transform = op.get_handler();

        let iter = join_iter.map(|x| transform_join_value_to_sample(&x, transform));

        return if let Some(aggr_options) = &options.aggregation {
            // Aggregation is valid only for transforms (all other options return multiple values per row)
            let (l_min, l_max) = get_sample_ts_range(left);
            let (r_min, r_max) = get_sample_ts_range(right);
            let start_timestamp = l_min.min(r_min);
            let end_timestamp = l_max.max(r_max);

            let aligned_timestamp = aggr_options
                .alignment
                .get_aligned_timestamp(start_timestamp, end_timestamp);

            let result = aggregate(aggr_options, aligned_timestamp, iter, options.count)
                .into_iter()
                .collect::<Vec<_>>();
            JoinResultType::Samples(result)
        } else {
            let result = iter.collect::<Vec<_>>();
            JoinResultType::Samples(result)
        };
    }

    let count = options.count.unwrap_or(usize::MAX);

    JoinResultType::Values(join_iter.take(count).collect::<Vec<_>>())
}

fn get_sample_ts_range(samples: &[Sample]) -> (Timestamp, Timestamp) {
    if samples.is_empty() {
        return (0, i64::MAX - 1);
    }
    let first = &samples[0];
    let last = &samples[samples.len() - 1];
    (first.timestamp, last.timestamp)
}

pub(super) fn transform_join_value_to_sample(item: &JoinValue, f: BinopFunc) -> Sample {
    match item.value {
        EitherOrBoth::Both(l, r) => Sample::new(item.timestamp, f(l, r)),
        EitherOrBoth::Left(l) => Sample::new(item.timestamp, f(l, f64::NAN)),
        EitherOrBoth::Right(r) => Sample::new(item.timestamp, f(f64::NAN, r)),
    }
}

fn fetch_samples(ts: &TimeSeries, options: &JoinOptions) -> Vec<Sample> {
    let (start, end) = options.date_range.get_series_range(ts, true);
    let mut samples = get_series_range_filtered(
        ts,
        start,
        end,
        &options.timestamp_filter,
        &options.value_filter,
    );
    if let Some(count) = &options.count {
        samples.truncate(*count);
    }
    samples
}
