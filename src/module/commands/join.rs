use super::range_utils::get_range_internal;
use crate::common::types::Sample;
use crate::module::arg_parse::*;
use crate::module::commands::join_iter::JoinIterator;
use crate::module::get_timeseries;
use crate::module::types::{JoinAsOfDirection, JoinOptions, JoinType, JoinValue};
use crate::storage::time_series::TimeSeries;
use joinkit::EitherOrBoth;
use metricsql_parser::binaryop::BinopFunc;
use std::time::Duration;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const CMD_ARG_TOLERANCE: &str = "TOLERANCE";
const CMD_ARG_RANGE: &str = "RANGE";
const CMD_ARG_FILTER_BY_VALUE: &str = "FILTER_BY_VALUE";
const CMD_ARG_FILTER_BY_TS: &str = "FILTER_BY_TS";
const CMD_ARG_COUNT: &str = "COUNT";
const CMD_ARG_LEFT: &str = "LEFT";
const CMD_ARG_RIGHT: &str = "RIGHT";
const CMD_ARG_INNER: &str = "INNER";
const CMD_ARG_FULL: &str = "FULL";
const CMD_ARG_ASOF: &str = "ASOF";
const CMD_ARG_PRIOR: &str = "PRIOR";
const CMD_ARG_NEXT: &str = "NEXT";
const CMD_ARG_EXCLUSIVE: &str = "EXCLUSIVE";
const CMD_ARG_TRANSFORM: &str = "TRANSFORM";


const VALID_ARGS: [&str; 10] = [
    CMD_ARG_RANGE,
    CMD_ARG_FILTER_BY_VALUE,
    CMD_ARG_FILTER_BY_TS,
    CMD_ARG_COUNT,
    CMD_ARG_LEFT,
    CMD_ARG_RIGHT,
    CMD_ARG_INNER,
    CMD_ARG_FULL,
    CMD_ARG_ASOF,
    CMD_ARG_TRANSFORM
];

/// VKM.JOIN key1 key2 RANGE start end
/// [[INNER] | [FULL] | [LEFT [EXCLUSIVE]] | [RIGHT [EXCLUSIVE]] | [ASOF [PRIOR | NEXT] [TOLERANCE 2ms]]]
/// [FILTER_BY_TS ts...]
/// [FILTER_BY_VALUE min max]
/// [COUNT count]
/// [TRANSFORM op]
pub fn join(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let left_key = args.next_arg()?;
    let right_key = args.next_arg()?;

    let mut options = JoinOptions {
        join_type: Default::default(),
        date_range: Default::default(),
        count: Default::default(),
        timestamp_filter: Default::default(),
        value_filter: Default::default(),
        transform_op: None,
    };

    parse_join_args(&mut options, &mut args)?;

    let left_series = get_timeseries(ctx, &left_key)?;
    let right_series = get_timeseries(ctx, &right_key)?;

    Ok(process_join(left_series, right_series, &options))
}

fn parse_asof(args: &mut CommandArgIterator) -> ValkeyResult<JoinType> {
    // ASOF already seen
    let mut tolerance = Duration::default();
    let mut direction = JoinAsOfDirection::Forward;

    // ASOF [PRIOR | NEXT] [TOLERANCE duration]
    if let Some(next) = advance_if_next_token_one_of(args, &[CMD_ARG_PRIOR, CMD_ARG_NEXT]) {
        if next == CMD_ARG_PRIOR {
            direction = JoinAsOfDirection::Backward;
        } else if next == CMD_ARG_NEXT {
            direction = JoinAsOfDirection::Forward;
        }
    }
    if advance_if_next_token(args, CMD_ARG_TOLERANCE) {
       let arg = args.next_str()?;
        tolerance = parse_duration(arg)?
    }

    Ok(JoinType::AsOf(direction, tolerance))
}

fn possibly_parse_exclusive(args: &mut CommandArgIterator) -> bool {
    advance_if_next_token(args, CMD_ARG_EXCLUSIVE)
}

fn parse_join_args(options: &mut JoinOptions, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    let mut range_found: bool = false;
    let mut join_type_set = false;

    fn check_join_type_set(is_set: &mut bool) -> ValkeyResult<()> {
        if *is_set {
            Err(ValkeyError::Str("TSDB: join type already set"))
        } else {
            *is_set = true;
            Ok(())
        }
    }

    while let Ok(arg) = args.next_str() {
        let upper = arg.to_ascii_uppercase();
        match upper.as_str() {
            CMD_ARG_RANGE => {
                range_found = true;
                options.date_range = parse_timestamp_range(args)?;
            }
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
            CMD_ARG_TRANSFORM => {
                let arg = args.next_str()?;
                options.transform_op = Some(parse_operator(arg)?);
            }
            _ => return Err(ValkeyError::Str("invalid JOIN command argument")),
        }
    }
    if !range_found {
        return Err(ValkeyError::Str("ERR: missing RANGE"));
    }

    Ok(())
}

fn is_arg_valid(arg: &str) -> bool {
    VALID_ARGS.iter().any(|x| x.eq_ignore_ascii_case(arg))
}

fn process_join(left_series: &TimeSeries, right_series: &TimeSeries, options: &JoinOptions) -> ValkeyValue {
    // todo: rayon::join
    let left_samples = fetch_samples(left_series, options);
    let right_samples = fetch_samples(right_series, options);
    join_internal(&left_samples, &right_samples, options)
}

fn join_internal(left: &[Sample], right: &[Sample], options: &JoinOptions) -> ValkeyValue {
    let is_transform = options.transform_op.is_some();

    let join_iter = JoinIterator::new_from_options(left, right, options);

    // todo: aggregations
    let result = join_iter
        .map(|jv| join_value_to_value(jv, is_transform))
        .collect();

    ValkeyValue::Array(result)
}

fn join_value_to_value(row: JoinValue, is_transform: bool) -> ValkeyValue {
    let timestamp = ValkeyValue::from(row.timestamp);
    // todo: for asof, we also need the second timestamp
    match row.value {
        EitherOrBoth::Both(left, right) => {
            ValkeyValue::Array(vec![timestamp, ValkeyValue::Float(left), ValkeyValue::Float(right)])
        }
        EitherOrBoth::Left(left) => {
            if is_transform {
                ValkeyValue::Array(vec![timestamp, ValkeyValue::Float(left)])
            } else {
                ValkeyValue::Array(vec![timestamp, ValkeyValue::Float(left), ValkeyValue::Null ])
            }
        }
        EitherOrBoth::Right(right) => {
            ValkeyValue::Array(vec![timestamp, ValkeyValue::Null, ValkeyValue::Float(right)])
        }
    }
}

pub(super) fn convert_join_item(item: EitherOrBoth<&Sample, &Sample>) -> JoinValue {
    match item {
        EitherOrBoth::Both(l, r) => JoinValue::both(l.timestamp, l.value, r.value),
        EitherOrBoth::Left(l) => JoinValue::left(l.timestamp, l.value),
        EitherOrBoth::Right(r) => JoinValue::right(r.timestamp, r.value),
    }
}

pub(super) fn transform_join_value(item: &JoinValue, f: BinopFunc) -> JoinValue {
    match item.value {
        EitherOrBoth::Both(l, r) => {
            let value = transform_value(f, l, r).unwrap_or(f64::NAN);
            JoinValue::left(item.timestamp, value)
        },
        EitherOrBoth::Left(l) => {
            let value = transform_value(f, l, f64::NAN).unwrap_or(f64::NAN);
            JoinValue::left(item.timestamp, value)
        },
        EitherOrBoth::Right(r) => {
            let value = transform_value(f, f64::NAN, r).unwrap_or(f64::NAN);
            JoinValue::left(item.timestamp, value)
        },
    }
}


fn fetch_samples(ts: &TimeSeries, options: &JoinOptions) -> Vec<Sample> {
    let mut samples = get_range_internal(ts, &options.date_range, true, &options.timestamp_filter, &options.value_filter);
    if let Some(count) = &options.count {
        samples.truncate(*count);
    }
    samples
}

#[inline]
pub fn transform_value(f: BinopFunc, left: f64, right: f64) -> Option<f64> {
    let value = f(left, right);

    if value.is_nan() {
        None
    } else {
        Some(value)
    }
}

#[cfg(test)]
mod tests {
}