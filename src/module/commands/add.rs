use crate::arg_parse::{parse_duration_arg, parse_number_with_unit, parse_timestamp};
use crate::module::commands::create_series;
use crate::module::{with_timeseries_mut, VKM_SERIES_TYPE};
use crate::storage::time_series::TimeSeries;
use crate::storage::{DuplicatePolicy, TimeSeriesOptions};
use ahash::AHashMap;
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const CMD_ARG_RETENTION: &str = "RETENTION";
const CMD_ARG_DUPLICATE_POLICY: &str = "DUPLICATE_POLICY";
const CMD_ARG_DEDUPE_INTERVAL: &str = "DEDUPE_INTERVAL";
const CMD_ARG_CHUNK_SIZE: &str = "CHUNK_SIZE";
const CMD_ARG_LABELS: &str = "LABELS";

pub fn add(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let timestamp = parse_timestamp(args.next_str()?)?;
    let value = args.next_f64()?;

    let redis_key = ctx.open_key_writable(&key);
    let series = redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)?;
    if let Some(series) = series {
        args.done()?;
        if series.add(timestamp, value, None).is_ok() {
            return Ok(ValkeyValue::Integer(timestamp));
        }
        todo!("handle error");
    }

    let existing_result = with_timeseries_mut(ctx, &key, |series| {
        series.add(timestamp, value, None)?;
        Ok(ValkeyValue::Integer(timestamp))
    });

    if let Ok(result) = existing_result {
        args.done()?;
        return Ok(result);
    }

    let mut options = TimeSeriesOptions::default();

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_RETENTION) => {
                let next = args.next_arg()?;
                if let Ok(val) = parse_duration_arg(&next) {
                    options.retention(val);
                } else {
                    return Err(ValkeyError::Str("ERR invalid RETENTION value"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DEDUPE_INTERVAL) => {
                let next = args.next_arg()?;
                if let Ok(val) = parse_duration_arg(&next) {
                    options.dedupe_interval = Some(val);
                } else {
                    return Err(ValkeyError::Str("ERR invalid DEDUPE_INTERVAL value"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_CHUNK_SIZE) => {
                let next = args.next_str()?;
                if let Ok(val) = parse_number_with_unit(next) {
                    options.chunk_size(val as usize);
                } else {
                    return Err(ValkeyError::Str("ERR invalid CHUNK_SIZE value"));
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DUPLICATE_POLICY) => {
                let next = args.next_str()?;
                if let Ok(policy) = DuplicatePolicy::try_from(next) {
                    options.duplicate_policy(policy);
                } else {
                    return Err(ValkeyError::Str("ERR invalid DUPLICATE_POLICY"));
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
                return Err(ValkeyError::String(msg));
            }
        };
    }

    let mut ts = create_series(&key, options, ctx)?;
    ts.add(timestamp, value, None)?;

    let redis_key = ValkeyKeyWritable::open(ctx.ctx, &key);
    redis_key.set_value(&VKM_SERIES_TYPE, ts)?;

    Ok(ValkeyValue::Integer(timestamp))
}