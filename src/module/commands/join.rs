use super::range_utils::get_range_internal;
use crate::common::types::{Sample, Timestamp};
use crate::iter::aggregator::aggregate;
use crate::module::arg_parse::*;
use crate::module::result::sample_to_value;
use crate::module::{invalid_series_key_error, VKM_SERIES_TYPE};
use crate::series::time_series::TimeSeries;
use joinkit::EitherOrBoth;
use metricsql_parser::binaryop::BinopFunc;
use std::time::Duration;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use crate::join::{JoinIterator, JoinOptions, JoinType, JoinValue};
use crate::join::asof::AsOfJoinStrategy;

const CMD_ARG_COUNT: &str = "COUNT";
const CMD_ARG_LEFT: &str = "LEFT";
const CMD_ARG_RIGHT: &str = "RIGHT";
const CMD_ARG_INNER: &str = "INNER";
const CMD_ARG_FULL: &str = "FULL";
const CMD_ARG_ASOF: &str = "ASOF";
const CMD_ARG_PRIOR: &str = "PRIOR";
const CMD_ARG_NEXT: &str = "NEXT";
const CMD_ARG_EXCLUSIVE: &str = "EXCLUSIVE";
const CMD_ARG_REDUCE: &str = "REDUCE";


/// VM.JOIN key1 key2 fromTimestamp toTimestamp
///   [[INNER] | [FULL] | [LEFT [EXCLUSIVE]] | [RIGHT [EXCLUSIVE]] | [ASOF [PRIOR | NEXT] tolerance]]
///   [FILTER_BY_TS ts...]
///   [FILTER_BY_VALUE min max]
///   [COUNT count]
///   [REDUCE op]
///   [AGGREGATION aggregator bucketDuration [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
pub fn join(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let left_key = args.next_arg()?;
    let right_key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;

    let mut options = JoinOptions {
        join_type: Default::default(),
        date_range,
        count: Default::default(),
        timestamp_filter: Default::default(),
        value_filter: Default::default(),
        transform_op: None,
        aggregation: None,
    };

    parse_join_args(&mut args, &mut options)?;

    let left_db_key = ctx.open_key(&left_key);
    let right_db_key = ctx.open_key(&right_key);

    let left_series = left_db_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)?;
    let right_series = right_db_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)?;

    match (left_series, right_series) {
        (Some(left_series), Some(right_series)) => {
            Ok(process_join(left_series, right_series, &options))
        }
        (Some(_), None) => {
            Err(invalid_series_key_error(&right_key))
        }
        (None, Some(_)) => {
            Err(invalid_series_key_error(&left_key))
        }
        _ => {
            Err(ValkeyError::Str("VM: Invalid JOIN key"))
        }
    }
}

fn parse_asof(args: &mut CommandArgIterator) -> ValkeyResult<JoinType> {
    // ASOF already seen
    let mut tolerance = Duration::default();
    let mut direction = AsOfJoinStrategy::Prior;

    // ASOF [PRIOR | NEXT] [tolerance]
    if let Some(next) = advance_if_next_token_one_of(args, &[CMD_ARG_PRIOR, CMD_ARG_NEXT]) {
        if next == CMD_ARG_PRIOR {
            direction = AsOfJoinStrategy::Prior;
        } else if next == CMD_ARG_NEXT {
            direction = AsOfJoinStrategy::Next;
        }
    }

    if let Some(next_arg) = args.peek() {
        if let Ok(arg_str) = next_arg.try_as_str() {
            // see if we have a duration expression
            // durations in all cases start with an ascii digit, e.g 1000 or 10ms
            let ch = arg_str.chars().next().unwrap();
            if ch.is_ascii_digit() {
                let tolerance_ms = parse_duration_ms(arg_str)?;
                if tolerance_ms < 0 {
                    return Err(ValkeyError::Str("TSDB: negative tolerance not valid"));
                }
                tolerance = Duration::from_millis(tolerance_ms as u64);
                let _ = args.next_arg()?;
            }
        }
    }

    Ok(JoinType::AsOf(direction, tolerance))
}

fn possibly_parse_exclusive(args: &mut CommandArgIterator) -> bool {
    advance_if_next_token(args, CMD_ARG_EXCLUSIVE)
}

fn parse_join_args(args: &mut CommandArgIterator, options: &mut JoinOptions) -> ValkeyResult<()> {
    let mut join_type_set = false;

    fn check_join_type_set(is_set: &mut bool) -> ValkeyResult<()> {
        if *is_set {
            Err(ValkeyError::Str("ERR join type already set"))
        } else {
            *is_set = true;
            Ok(())
        }
    }

    fn is_arg_valid(arg: &str) -> bool {
        const VALID_ARGS: &[&str] = &[
            CMD_ARG_FILTER_BY_VALUE,
            CMD_ARG_FILTER_BY_TS,
            CMD_ARG_COUNT,
            CMD_ARG_LEFT,
            CMD_ARG_RIGHT,
            CMD_ARG_INNER,
            CMD_ARG_FULL,
            CMD_ARG_ASOF,
            CMD_ARG_REDUCE
        ];
        VALID_ARGS.contains(&arg)
    }

    while let Ok(arg) = args.next_str() {
        let upper = arg.to_ascii_uppercase();
        match upper.as_str() {
            CMD_ARG_FILTER_BY_VALUE => {
                options.value_filter = Some(parse_value_filter(args)?);
            }
            CMD_ARG_FILTER_BY_TS => {
                options.timestamp_filter = Some(parse_timestamp_filter(args, is_arg_valid)?);
            }
            CMD_ARG_COUNT => {
                options.count = Some(parse_count(args)?);
            }
            CMD_ARG_LEFT => {
                check_join_type_set(&mut join_type_set)?;
                let exclusive = possibly_parse_exclusive(args);
                options.join_type = JoinType::Left(exclusive);
            }
            CMD_ARG_RIGHT => {
                check_join_type_set(&mut join_type_set)?;
                let exclusive = possibly_parse_exclusive(args);
                options.join_type = JoinType::Left(exclusive);
            }
            CMD_ARG_INNER => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = JoinType::Inner;
            }
            CMD_ARG_FULL => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = JoinType::Full;
            }
            CMD_ARG_ASOF => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = parse_asof(args)?;
            }
            CMD_ARG_REDUCE => {
                let arg = args.next_str()?;
                options.transform_op = Some(parse_operator(arg)?);
            }
            CMD_ARG_AGGREGATION => {
                options.aggregation = Some(parse_aggregation_options(args)?)
            }
            _ => return Err(ValkeyError::Str("ERR: invalid JOIN command argument")),
        }
    }

    // aggregations are only valid when there is a transform
    if options.aggregation.is_some() && options.transform_op.is_none() {
        return Err(ValkeyError::Str("ERR: join aggregation requires a reducer"));
    }

    Ok(())
}

fn process_join(left_series: &TimeSeries, right_series: &TimeSeries, options: &JoinOptions) -> ValkeyValue {
    // todo: rayon::join
    let left_samples = fetch_samples(left_series, options);
    let right_samples = fetch_samples(right_series, options);
    join_internal(&left_samples, &right_samples, options)
}

fn join_internal(left: &[Sample], right: &[Sample], options: &JoinOptions) -> ValkeyValue {

    let join_iter = JoinIterator::new(left, right, options.join_type);

    if let Some(op) = options.transform_op {
        let transform = op.get_handler();

        let iter = join_iter.map(|x| {
            transform_join_value_to_sample(&x, transform)
        });

        return if let Some(aggr_options) = &options.aggregation {
            // Aggregation is valid only for transforms (all other options return multiple values per row)
            let (l_min, l_max) = get_sample_ts_range(left);
            let (r_min, r_max) = get_sample_ts_range(right);
            let start_timestamp = l_min.min(r_min);
            let end_timestamp = l_max.max(r_max);

            let aligned_timestamp = aggr_options.alignment
                .get_aligned_timestamp(start_timestamp, end_timestamp);

            let result = aggregate(aggr_options, aligned_timestamp, iter, options.count)
                .into_iter()
                .map(sample_to_value)
                .collect::<Vec<_>>();

            ValkeyValue::Array(result)
        } else {
            let result = iter.map(sample_to_value).collect::<Vec<_>>();
            ValkeyValue::Array(result)
        }
    }

    let count = options.count.unwrap_or(usize::MAX);

    let result = join_iter
        .take(count)
        .map(|jv| join_value_to_valkey_value(jv, false))
        .collect();

    ValkeyValue::Array(result)
}

fn get_sample_ts_range(samples: &[Sample]) -> (Timestamp, Timestamp) {
    if samples.is_empty() {
        return (0, i64::MAX - 1);
    }
    let first = &samples[0];
    let last = &samples[samples.len() - 1];
    (first.timestamp, last.timestamp)
}

fn join_value_to_valkey_value(row: JoinValue, is_transform: bool) -> ValkeyValue {
    let timestamp = ValkeyValue::from(row.timestamp);

    match row.value {
        EitherOrBoth::Both(left, right) => {
            let r_value = ValkeyValue::from(right);
            let l_value = ValkeyValue::from(left);
            let res = if let Some(other_timestamp) = row.other_timestamp {
                vec![timestamp, ValkeyValue::from(other_timestamp), l_value, r_value]
            } else {
                vec![timestamp, l_value, r_value]
            };
            ValkeyValue::Array(res)
        }
        EitherOrBoth::Left(left) => {
            let value = ValkeyValue::from(left);
            if is_transform {
                ValkeyValue::Array(vec![timestamp, value])
            } else {
                ValkeyValue::Array(vec![timestamp, value, ValkeyValue::Null ])
            }
        }
        EitherOrBoth::Right(right) => {
            ValkeyValue::Array(vec![timestamp, ValkeyValue::Null, ValkeyValue::Float(right)])
        }
    }
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
    let mut samples = get_range_internal(ts, start, end, &options.timestamp_filter, &options.value_filter);
    if let Some(count) = &options.count {
        samples.truncate(*count);
    }
    samples
}


#[cfg(test)]
mod tests {
}