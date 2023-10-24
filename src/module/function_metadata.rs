use crate::common::types::{Timestamp, TimestampRangeValue};
use crate::common::{parse_series_selector, METRIC_NAME_LABEL};
use crate::globals::get_timeseries_index;
use crate::module::result::get_ts_metric_selector;
use crate::module::{normalize_range_args, parse_timestamp_arg};
use metricsql_parser::common::Matchers;
use redis_module::redisvalue::RedisValueKey;
use redis_module::{
    Context as RedisContext, Context, NextArg, RedisError, RedisResult, RedisString, RedisValue,
};
use std::collections::HashMap;

// todo: series count

/// https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers
pub(crate) fn series(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let label_args = parse_metadata_command_args(ctx, args, true)?;
    let ts_index = get_timeseries_index();

    let series =
        ts_index.series_by_matchers(ctx, &label_args.matchers, label_args.start, label_args.end);

    let values: RedisValue = series
        .into_iter()
        .map(|ts| get_ts_metric_selector(ts))
        .collect::<Vec<_>>()
        .into();

    let map: HashMap<RedisValueKey, RedisValue> = [
        (
            RedisValueKey::String("status".into()),
            RedisValue::SimpleStringStatic("success"),
        ),
        (RedisValueKey::String("data".into()), values),
    ]
    .into_iter()
    .collect();

    Ok(RedisValue::Map(map))
}

pub(crate) fn cardinality(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let label_args = parse_metadata_command_args(ctx, args, true)?;
    let ts_index = get_timeseries_index();

    let series =
        ts_index.series_by_matchers(ctx, &label_args.matchers, label_args.start, label_args.end);

    Ok(RedisValue::Integer(series.len() as i64))
}

/// https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names
pub(crate) fn label_names(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let label_args = parse_metadata_command_args(ctx, args, false)?;
    let ts_index = get_timeseries_index();

    let labels =
        ts_index.labels_by_matchers(ctx, &label_args.matchers, label_args.start, label_args.end);
    let mut labels_result = Vec::with_capacity(labels.len());
    if !labels.is_empty() {
        labels_result.push(RedisValue::from(METRIC_NAME_LABEL.to_string()));
        for label in labels {
            labels_result.push(RedisValue::from(label));
        }
    }

    let map: HashMap<RedisValueKey, RedisValue> = [
        (
            RedisValueKey::String("status".into()),
            RedisValue::SimpleStringStatic("success"),
        ),
        (
            RedisValueKey::String("data".into()),
            RedisValue::Array(labels_result),
        ),
    ]
    .into_iter()
    .collect();

    Ok(RedisValue::Map(map))
}

/// https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values
// MS.LABEL_VALUES <label_name> [MATCH <match>] [START <timestamp_ms>] [END <timestamp_ms>]
// https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values
pub(crate) fn label_values(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args;
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let label_name = args.remove(1).try_as_str()?;
    let label_args = parse_metadata_command_args(ctx, args, true)?;
    let ts_index = get_timeseries_index();

    let mut series =
        ts_index.series_by_matchers(ctx, &label_args.matchers, label_args.start, label_args.end);

    let arr_values = series
        .drain(..)
        .filter_map(|series| series.labels.get(label_name))
        .map(|v| RedisValue::from(v))
        .collect::<Vec<_>>();

    let map: HashMap<RedisValueKey, RedisValue> = [
        (
            RedisValueKey::String("status".into()),
            RedisValue::SimpleStringStatic("success"),
        ),
        (
            RedisValueKey::String("data".into()),
            RedisValue::Array(arr_values),
        ),
    ]
    .into_iter()
    .collect();

    Ok(RedisValue::Map(map))
}

struct MetadataFunctionArgs {
    label_name: Option<String>,
    start: Timestamp,
    end: Timestamp,
    matchers: Vec<Matchers>,
}

static CMD_ARG_START: &str = "START";
static CMD_ARG_END: &str = "END";
static CMD_ARG_MATCH: &str = "MATCH";
static CMD_ARG_LABEL: &str = "LABEL";

fn parse_metadata_command_args(
    _ctx: &RedisContext,
    args: Vec<RedisString>,
    require_matchers: bool,
) -> RedisResult<MetadataFunctionArgs> {
    let mut args = args.into_iter().skip(1);
    let label_name = None;
    let mut matchers = Vec::with_capacity(4);
    let mut start_value: Option<TimestampRangeValue> = None;
    let mut end_value: Option<TimestampRangeValue> = None;

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_START) => {
                let next = args.next_str()?;
                start_value = Some(parse_timestamp_arg(&next, "START")?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_END) => {
                let next = args.next_str()?;
                end_value = Some(parse_timestamp_arg( &next, "END")?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_MATCH) => {
                while let Ok(matcher) = args.next_str() {
                    if let Ok(selector) = parse_series_selector(&matcher) {
                        matchers.push(selector);
                    } else {
                        return Err(RedisError::Str("ERR invalid MATCH series selector"));
                    }
                }
            }
            _ => {
                let msg = format!("ERR invalid argument '{}'", arg);
                return Err(RedisError::String(msg));
            }
        };
    }

    let (start, end) = normalize_range_args(start_value, end_value)?;

    if require_matchers && matchers.is_empty() {
        return Err(RedisError::Str(
            "ERR at least 1 MATCH series selector required",
        ));
    }

    Ok(MetadataFunctionArgs {
        label_name,
        start,
        end,
        matchers,
    })
}
