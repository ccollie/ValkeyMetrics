use crate::common::METRIC_NAME_LABEL;
use crate::error_consts;
use crate::module::arg_parse::{
    parse_command_arg_token, 
    parse_series_selector_list, 
    CommandArgToken,
};
use crate::module::result::{format_array_result, get_ts_metric_selector};
use crate::module::{parse_timestamp_arg, VKM_SERIES_TYPE};
use crate::series::index::{series_keys_by_matchers, with_timeseries_index};
use crate::series::time_series::TimeSeries;
use crate::series::types::MetadataFunctionArgs;
use crate::series::{normalize_range_args, TimestampValue};
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

// VM.LABEL_VALUES label [FILTER seriesMatcher] [START fromTimestamp] [END fromTimestamp]
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

fn with_matched_series<F, R>(
    ctx: &Context,
    mut acc: R,
    args: MetadataFunctionArgs,
    mut f: F,
) -> ValkeyResult<R>
where
    F: FnMut(R, &TimeSeries, &ValkeyString) -> R,
{
    with_timeseries_index(ctx, move |index| {
        let keys = series_keys_by_matchers(ctx, index, &args.matchers)?;
        if keys.is_empty() {
            return Err(ValkeyError::Str(error_consts::NO_SERIES_FOUND));
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


fn parse_metadata_command_args(
    _ctx: &RedisContext,
    args: Vec<ValkeyString>,
    require_matchers: bool,
) -> ValkeyResult<MetadataFunctionArgs> {
    const ARG_TOKENS: [CommandArgToken; 3] = [
        CommandArgToken::End,
        CommandArgToken::Start,
        CommandArgToken::Limit,
    ];

    let mut args = args.into_iter().skip(1).peekable();
    let mut matchers = Vec::with_capacity(4);
    let mut start_value: Option<TimestampValue> = None;
    let mut end_value: Option<TimestampValue> = None;
    let mut limit: Option<usize> = None;

    fn is_cmd_token(arg: CommandArgToken) -> bool {
        ARG_TOKENS.contains(&arg)
    }

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::Start => {
                let next = args.next_str()?;
                start_value = Some(parse_timestamp_arg(next, "START")?);
            }
            CommandArgToken::End => {
                let next = args.next_str()?;
                end_value = Some(parse_timestamp_arg(next, "END")?);
            }
            CommandArgToken::Match => {
                let m = parse_series_selector_list(&mut args, is_cmd_token)?;
                matchers.extend(m);
            }
            CommandArgToken::Limit => {
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
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    Ok(MetadataFunctionArgs {
        start,
        end,
        matchers,
        limit,
    })
}
