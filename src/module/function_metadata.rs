use crate::common::types::Timestamp;
use crate::common::METRIC_NAME_LABEL;
use crate::globals::with_timeseries_index;
use crate::module::arg_parse::{parse_series_selector, TimestampRangeValue};
use crate::module::result::{format_array_result, get_ts_metric_selector};
use crate::module::{normalize_range_args, parse_timestamp_arg, VALKEY_PROMQL_SERIES_TYPE};
use crate::storage::time_series::TimeSeries;
use metricsql_parser::label::Matchers;
use std::collections::BTreeSet;
use valkey_module::{
    Context as RedisContext, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};
// todo: series count

/// https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers
pub(crate) fn series(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let label_args = parse_metadata_command_args(ctx, args, true)?;
    let limit = label_args.limit.unwrap_or(usize::MAX);

    let values = with_matched_series(ctx, Vec::new(), label_args, |mut acc, ts| {
        if acc.len() < limit {
            acc.push(get_ts_metric_selector(ts));
        }
        acc
    })?;

    Ok(format_array_result(values))
}

pub(crate) fn cardinality(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let label_args = parse_metadata_command_args(ctx, args, true)?;
    let count = with_matched_series(ctx, 0, label_args, |acc, _| acc + 1)?;

    Ok(ValkeyValue::from(count as i64))
}

/// https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names
pub(crate) fn label_names(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // todo: this does a lot of cloning :-(
    let label_args = parse_metadata_command_args(ctx, args, false)?;
    let limit = label_args.limit.unwrap_or(usize::MAX);

    let mut acc: BTreeSet<String> = BTreeSet::new();
    acc.insert(METRIC_NAME_LABEL.to_string());

    let names = with_matched_series(ctx, acc, label_args, |mut acc, ts| {
        for label in ts.labels.iter() {
            acc.insert(label.name.clone());
        }
        acc
    })?;

    let labels = names
        .iter()
        .take(limit)
        .map(|v| ValkeyValue::from(v.clone()))
        .collect::<Vec<_>>();

    Ok(format_array_result(labels))
}

// MS.LABEL_VALUES <label_name> [MATCH <match>] [START <timestamp_ms>] [END <timestamp_ms>]
// https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values
pub(crate) fn label_values(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let label_args = parse_metadata_command_args(ctx, args, true)?;
    let limit = label_args.limit.unwrap_or(usize::MAX);

    let acc: BTreeSet<String> = BTreeSet::new();
    let names = with_matched_series(ctx, acc, label_args, |mut acc, ts| {
        for label in ts.labels.iter() {
            acc.insert(label.value.clone());
        }
        acc
    })?;

    let label_values = names
        .iter()
        .take(limit)
        .map(|v| ValkeyValue::from(v.clone()))
        .collect::<Vec<_>>();

    Ok(format_array_result(label_values))
}

fn format_string_array_result(arr: &[String]) -> ValkeyValue {
    let converted = arr.iter().map(ValkeyValue::from).collect();
    format_array_result(converted)
}


pub(crate) fn with_matched_series<F, R>(ctx: &Context, mut acc: R, args: MetadataFunctionArgs, mut f: F) -> ValkeyResult<R>
where
    F: FnMut(R, &TimeSeries) -> R,
{
    with_timeseries_index(ctx, move |index| {
        let keys = index.series_keys_by_matchers(ctx, &args.matchers);
        if keys.is_empty() {
            return Err(ValkeyError::Str("ERR no series found"));
        }
        for key in keys {
            let redis_key = ctx.open_key(&key);
            // get series from redis
            match redis_key.get_value::<TimeSeries>(&VALKEY_PROMQL_SERIES_TYPE) {
                Ok(Some(series)) => {
                    if series.overlaps(args.start, args.end) {
                        acc = f(acc, series)
                    }
                }
                Err(e) => {
                    return Err(e);
                }
                _ => {}
            }
        }
        Ok(acc)
    })
}

pub(crate) struct MetadataFunctionArgs {
    label_name: Option<String>,
    start: Timestamp,
    end: Timestamp,
    matchers: Vec<Matchers>,
    limit: Option<usize>,
}

static CMD_ARG_START: &str = "START";
static CMD_ARG_END: &str = "END";
static CMD_ARG_MATCH: &str = "MATCH";
static CMD_ARG_LABEL: &str = "LABEL";
static CMD_ARG_LIMIT: &str = "LIMIT";

fn parse_metadata_command_args(
    _ctx: &RedisContext,
    args: Vec<ValkeyString>,
    require_matchers: bool,
) -> ValkeyResult<MetadataFunctionArgs> {
    let mut args = args.into_iter().skip(1);
    let label_name = None;
    let mut matchers = Vec::with_capacity(4);
    let mut start_value: Option<TimestampRangeValue> = None;
    let mut end_value: Option<TimestampRangeValue> = None;
    let mut limit: Option<usize> = None;

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_START) => {
                let next = args.next_str()?;
                start_value = Some(parse_timestamp_arg(next, "START")?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_END) => {
                let next = args.next_str()?;
                end_value = Some(parse_timestamp_arg(next, "END")?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_MATCH) => {
                while let Ok(matcher) = args.next_str() {
                    if let Ok(selector) = parse_series_selector(matcher) {
                        matchers.push(selector);
                    } else {
                        return Err(ValkeyError::Str("ERR invalid MATCH series selector"));
                    }
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_LIMIT) => {
                let next = args.next_u64()?;
                if next > usize::MAX as u64 {
                    return Err(ValkeyError::Str("ERR LIMIT too large"));
                }
                limit = Some(next as usize);
            }
            _ => {
                let msg = format!("ERR invalid argument '{}'", arg);
                return Err(ValkeyError::String(msg));
            }
        };
    }

    let (start, end) = normalize_range_args(start_value, end_value)?;

    if require_matchers && matchers.is_empty() {
        return Err(ValkeyError::Str(
            "ERR at least 1 MATCH series selector required",
        ));
    }

    Ok(MetadataFunctionArgs {
        label_name,
        start,
        end,
        matchers,
        limit,
    })
}
