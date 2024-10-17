use crate::arg_parse::*;
use crate::module::commands::create_series;
use crate::module::{get_timeseries_mut, VKM_SERIES_TYPE};
use crate::series::TimeSeriesOptions;
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const CMD_ARG_METRIC: &str = "METRIC";

///
/// VKM.ADD key timestamp value
///     [RETENTION duration]
///     [DUPLICATE_POLICY policy]
///     [DEDUPE_INTERVAL duration]
///     [CHUNK_SIZE size]
///     [METRIC name]
///
pub fn add(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let timestamp = parse_timestamp(args.next_str()?)?;
    let value = args.next_f64()?;

    if let Some(series) = get_timeseries_mut(ctx, &key, true)? {
        args.done()?;
        return match series.add(timestamp, value, None) {
            Ok(_) => Ok(ValkeyValue::Integer(timestamp)),
            Err(e) => Err(e),
        }
    }

    let mut options = TimeSeriesOptions::default();

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_RETENTION) => {
                options.retention(parse_retention(&mut args)?)
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DEDUPE_INTERVAL) => {
                options.dedupe_interval = Some(parse_dedupe_interval(&mut args)?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_CHUNK_SIZE) => {
                options.chunk_size(parse_chunk_size(&mut args)?)
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DUPLICATE_POLICY) => {
                options.duplicate_policy(parse_duplicate_policy(&mut args)?)
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_METRIC) => {
                options.labels = parse_metric_name(args.next_str()?)?;
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