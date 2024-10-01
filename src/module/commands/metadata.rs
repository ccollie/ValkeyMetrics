use crate::common::METRIC_NAME_LABEL;
use crate::globals::with_timeseries_index;
use crate::module::arg_parse::parse_series_selector_list;
use crate::module::result::{format_array_result, get_ts_metric_selector};
use crate::module::types::{MetadataFunctionArgs, TimestampRangeValue};
use crate::module::{normalize_range_args, parse_timestamp_arg, VKM_SERIES_TYPE};
use crate::storage::time_series::TimeSeries;
use std::collections::BTreeSet;
use valkey_module::{
    Context as RedisContext, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};
// todo: series count

/// https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers
pub fn series(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let label_args = parse_metadata_command_args(ctx, args, true)?;
    let limit = label_args.limit.unwrap_or(usize::MAX);

    let values = with_matched_series(ctx, Vec::new(), label_args, |mut acc, ts, key| {
        if acc.len() < limit {
            acc.push(get_ts_metric_selector(ts, Some(key)));
        }
        acc
    })?;

    Ok(format_array_result(values))
}

pub fn cardinality(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let label_args = parse_metadata_command_args(ctx, args, true)?;
    let count = with_matched_series(ctx, 0, label_args, |acc, _, _| acc + 1)?;

    Ok(ValkeyValue::from(count as i64))
}

/// https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names
pub fn label_names(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // todo: this does a lot of cloning :-(
    let label_args = parse_metadata_command_args(ctx, args, false)?;
    let limit = label_args.limit.unwrap_or(usize::MAX);

    let mut acc: BTreeSet<String> = BTreeSet::new();
    acc.insert(METRIC_NAME_LABEL.to_string());

    let names = with_matched_series(ctx, acc, label_args, |mut acc, ts, _| {
        for label in ts.labels.iter() {
            acc.insert(label.name.clone());
        }
        acc
    })?;

    let labels = names
        .into_iter()
        .take(limit)
        .map(ValkeyValue::from)
        .collect::<Vec<_>>();

    Ok(format_array_result(labels))
}

// VM.LABEL_VALUES <label_name> [FILTER <match>] [START <timestamp_ms>] [END <timestamp_ms>]
// https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values
pub(crate) fn label_values(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let label_args = parse_metadata_command_args(ctx, args, true)?;
    let limit = label_args.limit.unwrap_or(usize::MAX);

    let acc: BTreeSet<String> = BTreeSet::new();
    let names = with_matched_series(ctx, acc, label_args, |mut acc, ts, _| {
        for label in ts.labels.iter() {
            acc.insert(label.value.clone());
        }
        acc
    })?;

    let label_values = names
        .into_iter()
        .take(limit)
        .map(ValkeyValue::from)
        .collect::<Vec<_>>();

    Ok(format_array_result(label_values))
}

fn with_matched_series<F, R>(ctx: &Context, mut acc: R, args: MetadataFunctionArgs, mut f: F) -> ValkeyResult<R>
where
    F: FnMut(R, &TimeSeries, &ValkeyString) -> R,
{
    with_timeseries_index(ctx, move |index| {
        let keys = index.series_keys_by_matchers(ctx, &args.matchers);
        if keys.is_empty() {
            return Err(ValkeyError::Str("ERR no series found"));
        }
        for key in keys {
            let redis_key = ctx.open_key(&key);
            // get series from redis
            match redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE) {
                Ok(Some(series)) => {
                    if series.overlaps(args.start, args.end) {
                        acc = f(acc, series, &key)
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

const CMD_ARG_START: &str = "START";
const CMD_ARG_END: &str = "END";
const CMD_ARG_MATCH: &str = "MATCH";
const CMD_ARG_LIMIT: &str = "LIMIT";

fn parse_metadata_command_args(
    _ctx: &RedisContext,
    args: Vec<ValkeyString>,
    require_matchers: bool,
) -> ValkeyResult<MetadataFunctionArgs> {
    const ARG_TOKENS: [&str; 3] = [
        CMD_ARG_END,
        CMD_ARG_START,
        CMD_ARG_LIMIT
    ];

    let mut args = args.into_iter().skip(1).peekable();
    let label_name = None;
    let mut matchers = Vec::with_capacity(4);
    let mut start_value: Option<TimestampRangeValue> = None;
    let mut end_value: Option<TimestampRangeValue> = None;
    let mut limit: Option<usize> = None;

    fn is_cmd_token(s: &str) -> bool {
        ARG_TOKENS.iter().any(|token| token.eq_ignore_ascii_case(s))
    }

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
                let m = parse_series_selector_list(&mut args, is_cmd_token)?;
                matchers.extend(m);
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
