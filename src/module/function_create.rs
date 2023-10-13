use redis_module::{Context, NextArg, REDIS_OK, RedisError, RedisResult, RedisString};
use ahash::AHashMap;
use crate::common::parse_duration;
use crate::module::create_and_store_series;
use crate::ts::{DuplicatePolicy, TimeSeriesOptions};

const CMD_ARG_RETENTION: &str = "RETENTION";
const CMD_ARG_DUPLICATE_POLICY: &str = "DUPLICATE_POLICY";
const CMD_ARG_DEDUPE_INTERVAL: &str = "DEDUPE_INTERVAL";
const CMD_ARG_LABELS: &str = "LABELS";
const CMD_ARG_METRIC_NAME: &str = "METRIC_NAME";


pub fn create(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let (parsed_key, options) = parse_create_options(ctx, args)?;
    create_and_store_series(ctx, &parsed_key, options)?;
    REDIS_OK
}

pub fn parse_create_options(ctx: &Context, args: Vec<RedisString>) -> RedisResult<(RedisString, TimeSeriesOptions)> {
    let mut args = args.into_iter().skip(1);
    let key = args.next().ok_or_else(|| RedisError::Str("Err missing key argument"))?;

    let mut options = TimeSeriesOptions::default();

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_RETENTION) => {
                let next = args.next_str()?;
                if let Ok(val) = parse_duration(&next) {
                    options.retention(val);
                } else {
                    return Err(RedisError::Str("ERR invalid RETENTION value"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DEDUPE_INTERVAL) => {
                let next = args.next_str()?;
                if let Ok(val) = parse_duration(&next) {
                    options.dedupe_interval(val);
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
                let next = args.next_str()?;
                options.metric_name = Some(next.to_string());
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
