use redis_module::{Context, NextArg, NotifyEvent, REDIS_OK, RedisError, RedisResult, RedisString};
use std::time::Duration;
use ahash::AHashMap;
use redis_module::key::RedisKeyWritable;
use crate::arg_parse::{parse_chunk_size, parse_duration_arg};
use crate::globals::get_timeseries_index;
use crate::module::{REDIS_PROMQL_SERIES_TYPE};
use crate::ts::{DEFAULT_CHUNK_SIZE_BYTES, DuplicatePolicy, Label, TimeSeriesOptions};
use crate::ts::time_series::TimeSeries;

const CMD_ARG_RETENTION: &str = "RETENTION";
const CMD_ARG_DUPLICATE_POLICY: &str = "DUPLICATE_POLICY";
const CMD_ARG_CHUNK_SIZE: &str = "CHUNK_SIZE";
const CMD_ARG_DEDUPE_INTERVAL: &str = "DEDUPE_INTERVAL";
const CMD_ARG_LABELS: &str = "LABELS";
const CMD_ARG_METRIC_NAME: &str = "METRIC_NAME";


pub fn create(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let (parsed_key, options) = parse_create_options(args)?;
    let key = RedisKeyWritable::open(ctx.ctx, &parsed_key);
    // check if this refers to an existing series
    if !key.is_empty() {
        return Err(RedisError::Str("TSDB: the key already exists"));
    }

    let ts = create_timeseries(&parsed_key, options);

    key.set_value(&REDIS_PROMQL_SERIES_TYPE, ts)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "PROM.CREATE-SERIES", &parsed_key);

    REDIS_OK
}

pub fn parse_create_options(args: Vec<RedisString>) -> RedisResult<(RedisString, TimeSeriesOptions)> {
    let mut args = args.into_iter().skip(1);
    let key = args.next().ok_or_else(|| RedisError::Str("Err missing key argument"))?;

    let mut options = TimeSeriesOptions::default();

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_RETENTION) => {
                let next = args.next_arg()?;
                if let Ok(val) = parse_duration_arg(&next) {
                    options.retention(val);
                } else {
                    return Err(RedisError::Str("ERR invalid RETENTION value"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DEDUPE_INTERVAL) => {
                let next = args.next_arg()?;
                if let Ok(val) = parse_duration_arg(&next) {
                    options.dedupe_interval = Some(val);
                } else {
                    return Err(RedisError::Str("ERR invalid DEDUPE_INTERVAL value"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DUPLICATE_POLICY) => {
                let next = args.next_str()?;
                if let Ok(policy) = DuplicatePolicy::try_from(next) {
                    options.duplicate_policy(policy);
                } else {
                    return Err(RedisError::Str("ERR invalid DUPLICATE_POLICY"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_METRIC_NAME) => {
                options.metric_name = Some(args.next_string()?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_CHUNK_SIZE) => {
                let next = args.next_str()?;
                if let Ok(val) = parse_chunk_size(next) {
                    options.chunk_size(val);
                } else {
                    return Err(RedisError::Str("ERR invalid CHUNK_SIZE value"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_LABELS) => {
                let mut labels: AHashMap<String, String> = Default::default();
                while let Ok(name) = args.next_str() {
                    let value = args.next_str()?;
                    labels.insert(name.to_string(), value.to_string());
                }
                options.labels(labels);
            }
            _ => {
                let msg = format!("ERR invalid argument '{}'", arg);
                return Err(RedisError::String(msg));
            }
        };
    }

    Ok((key, options))
}


pub(crate) fn create_timeseries(
    key: &RedisString,
    options: TimeSeriesOptions,
) -> TimeSeries {
    let mut ts = TimeSeries::new();
    ts.metric_name = options.metric_name.unwrap_or_else(|| key.to_string());
    ts.chunk_size_bytes = options.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE_BYTES);
    ts.retention = options.retention.unwrap_or(Duration::from_millis(0u64));
    ts.dedupe_interval = options.dedupe_interval;
    ts.duplicate_policy = options.duplicate_policy;
    if let Some(labels) = options.labels {
        for (k, v) in labels.iter() {
            ts.labels.push(Label {
                name: k.to_string(),
                value: v.to_string(),
            });
        }
    }
    let ts_index = get_timeseries_index();
    ts_index.index_time_series(&mut ts, key.to_string());
    ts
}

pub(crate) fn create_series_ex(ctx: &Context, key: &RedisString, options: TimeSeriesOptions) -> RedisResult<()> {
    let _key = RedisKeyWritable::open(ctx.ctx, &key);
    // check if this refers to an existing series
    if !_key.is_empty() {
        return Err(RedisError::Str("TSDB: the key already exists"));
    }

    let ts = create_timeseries(&key, options);
    _key.set_value(&REDIS_PROMQL_SERIES_TYPE, ts)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "PROM.CREATE-SERIES", &key);
    ctx.log_verbose("series created");

    Ok(())
}