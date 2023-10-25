use crate::module::function_create::{create_timeseries, TimeSeriesOptions};
use crate::module::{REDIS_PROMQL_SERIES_TYPE};
use crate::ts::{DuplicatePolicy, get_timeseries_mut};
use redis_module::key::RedisKeyWritable;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use ahash::AHashMap;
use crate::common::{parse_duration_arg, parse_number_with_unit, parse_timestamp};
use crate::module::timeseries_api::internal_add;

const CMD_ARG_RETENTION: &str = "RETENTION";
const CMD_ARG_DUPLICATE_POLICY: &str = "DUPLICATE_POLICY";
const CMD_ARG_DEDUPE_INTERVAL: &str = "DEDUPE_INTERVAL";
const CMD_ARG_CHUNK_SIZE: &str = "CHUNK_SIZE";
const CMD_ARG_LABELS: &str = "LABELS";
const CMD_ARG_METRIC_NAME: &str = "METRIC_NAME";

pub fn add(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let timestamp = parse_timestamp(args.next_str()?)?;
    let value = args.next_f64()?;

    let mut options = TimeSeriesOptions::default();

    let series = get_timeseries_mut(ctx, &key, false)?;
    if let Some(series) = series {
        args.done()?;
        internal_add(
            ctx,
            series,
            timestamp,
            value,
            series.duplicate_policy.unwrap_or_default(),
        )?;
        return Ok(RedisValue::Integer(timestamp));
    }

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
                    options.dedupe_interval(val);
                } else {
                    return Err(RedisError::Str("ERR invalid DEDUPE_INTERVAL value"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_CHUNK_SIZE) => {
                let next = args.next_str()?;
                if let Ok(val) = parse_number_with_unit(&next) {
                    options.chunk_size(val as usize);
                } else {
                    return Err(RedisError::Str("ERR invalid CHUNK_SIZE value"));
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
            arg if arg.eq_ignore_ascii_case(CMD_ARG_LABELS) => {
                let mut labels = AHashMap::new();
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

    let mut ts = create_timeseries(&key, options);

    let dupe_policy = ts.duplicate_policy.unwrap_or_default();
    internal_add(
        ctx,
        &mut ts,
        timestamp,
        value,
        dupe_policy,
    )?;

    let redis_key = RedisKeyWritable::open(ctx.ctx, &key);
    redis_key.set_value(&REDIS_PROMQL_SERIES_TYPE, ts)?;

    return Ok(RedisValue::Integer(timestamp));
}