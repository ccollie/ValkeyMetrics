use ahash::AHashMap;
use valkey_module::{Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};
use valkey_module::key::ValkeyKeyWritable;
use crate::arg_parse::{parse_chunk_size, parse_duration_arg};
use crate::error::TsdbResult;
use crate::globals::get_timeseries_index;
use crate::module::{REDIS_PROMQL_SERIES_TYPE};
use crate::storage::{DuplicatePolicy, TimeSeriesOptions};
use crate::storage::time_series::TimeSeries;


const CMD_ARG_RETENTION: &str = "RETENTION";
const CMD_ARG_DUPLICATE_POLICY: &str = "DUPLICATE_POLICY";
const CMD_ARG_CHUNK_SIZE: &str = "CHUNK_SIZE";
const CMD_ARG_DEDUPE_INTERVAL: &str = "DEDUPE_INTERVAL";
const CMD_ARG_LABELS: &str = "LABELS";
const CMD_ARG_METRIC_NAME: &str = "METRIC_NAME";


pub fn create(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let (parsed_key, options) = parse_create_options(args)?;
    let key = ValkeyKeyWritable::open(ctx.ctx, &parsed_key);
    // check if this refers to an existing series
    if !key.is_empty() {
        return Err(ValkeyError::Str("TSDB: the key already exists"));
    }

    let ts = create_series(&parsed_key, options, ctx)
        .map_err(|e| ValkeyError::Str(&format!("TSDB: failed to create series: {:?}", e)))?;

    key.set_value(&REDIS_PROMQL_SERIES_TYPE, ts)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "PROM.CREATE-SERIES", &parsed_key);

    VALKEY_OK
}

pub fn parse_create_options(args: Vec<ValkeyString>) -> ValkeyResult<(ValkeyString, TimeSeriesOptions)> {
    let mut args = args.into_iter().skip(1);
    let key = args.next().ok_or_else(|| ValkeyError::Str("Err missing key argument"))?;

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
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DUPLICATE_POLICY) => {
                let next = args.next_str()?;
                if let Ok(policy) = DuplicatePolicy::try_from(next) {
                    options.duplicate_policy(policy);
                } else {
                    return Err(ValkeyError::Str("ERR invalid DUPLICATE_POLICY"));
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
                    return Err(ValkeyError::Str("ERR invalid CHUNK_SIZE value"));
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
                return Err(ValkeyError::String(msg));
            }
        };
    }

    Ok((key, options))
}


pub(crate) fn create_series(
    key: &ValkeyString,
    options: TimeSeriesOptions,
    ctx: &Context,
) -> TsdbResult<TimeSeries> {
    let mut ts = TimeSeries::with_options(options)?;
    // todo: we need to have a default retention value
    let ts_index = get_timeseries_index(ctx);
    ts_index.index_time_series(&mut ts, key.to_string());
    Ok(ts)
}

pub(crate) fn create_series_ex(ctx: &Context, key: &ValkeyString, options: TimeSeriesOptions) -> ValkeyResult<()> {
    let _key = ValkeyKeyWritable::open(ctx.ctx, &key);
    // check if this refers to an existing series
    if !_key.is_empty() {
        return Err(ValkeyError::Str("TSDB: the key already exists"));
    }

    let ts = create_series(&key, options, ctx)?;
    _key.set_value(&REDIS_PROMQL_SERIES_TYPE, ts)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "PROM.CREATE-SERIES", &key);
    ctx.log_verbose("series created");

    Ok(())
}