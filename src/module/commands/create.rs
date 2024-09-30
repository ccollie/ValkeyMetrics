use crate::error::{TsdbError, TsdbResult};
use crate::globals::with_timeseries_index;
use crate::index::TimeSeriesIndex;
use crate::module::arg_parse::*;
use crate::module::VKM_SERIES_TYPE;
use crate::storage::time_series::TimeSeries;
use crate::storage::TimeSeriesOptions;
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};

const CMD_ARG_RETENTION: &str = "RETENTION";
const CMD_ARG_DUPLICATE_POLICY: &str = "DUPLICATE_POLICY";
const CMD_ARG_CHUNK_SIZE: &str = "CHUNK_SIZE";
const CMD_ARG_DEDUPE_INTERVAL: &str = "DEDUPE_INTERVAL";
const CMD_ARG_SIGNIFICANT_DIGITS: &str = "SIGNIFICANT_DIGITS";
const MAX_SIGNIFICANT_DIGITS: u8 = 16;

/// Create a new time series
///
/// VKM.CREATE key metric
///   [RETENTION retentionPeriod]
///   [ENCODING <COMPRESSED|UNCOMPRESSED>]
///   [CHUNK_SIZE size]
///   [DUPLICATE_POLICY policy]
///   [DEDUPE_INTERVAL duplicateTimediff]
pub fn create(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let (parsed_key, options) = parse_create_options(args)?;
    let key = ValkeyKeyWritable::open(ctx.ctx, &parsed_key);
    // check if this refers to an existing series
    if !key.is_empty() {
        return Err(ValkeyError::Str("TSDB: the key already exists"));
    }

    let ts = create_series(&parsed_key, options, ctx)
        .map_err(|_| ValkeyError::Str("TSDB: failed to create series"))?;

    key.set_value(&VKM_SERIES_TYPE, ts)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "VKM.CREATE-SERIES", &parsed_key);

    VALKEY_OK
}

pub fn parse_create_options(args: Vec<ValkeyString>) -> ValkeyResult<(ValkeyString, TimeSeriesOptions)> {
    let mut args = args.into_iter().skip(1).peekable();

    let mut options = TimeSeriesOptions::default();

    let key = args.next().ok_or(ValkeyError::Str("Err missing key argument"))?;

    let metric = args.next_string()?;
    options.labels = parse_metric_name(&metric)
        .map_err(|_| ValkeyError::Str("ERR invalid METRIC"))?;

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_RETENTION) => {
                options.retention(parse_retention(&mut args)?)
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DEDUPE_INTERVAL) => {
                options.dedupe_interval = Some(parse_dedupe_interval(&mut args)?)
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DUPLICATE_POLICY) => {
                options.duplicate_policy = Some(parse_duplicate_policy(&mut args)?)
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_SIGNIFICANT_DIGITS) => {
                let next = args.next_u64()?;
                if next > MAX_SIGNIFICANT_DIGITS as u64 {
                    let msg = "ERR SIGNIFICANT_DIGITS must be between 0 and 16";
                    return Err(ValkeyError::Str(msg));
                }
                options.significant_digits = Some(next as u8);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_CHUNK_SIZE) => {
                options.chunk_size(parse_chunk_size(&mut args)?);
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
    with_timeseries_index(ctx, |index| {
        // will return an error if the series already exists
        let existing_id = index.get_id_by_name_and_labels(&ts.metric_name, &ts.labels)?;
        if let Some(_id) = existing_id {
            return Err(TsdbError::DuplicateMetric(ts.prometheus_metric_name()));
        }

        ts.id = TimeSeriesIndex::next_id();
        index.index_time_series(&ts, key.iter().as_slice());
        Ok(ts)
    })
}

pub(crate) fn create_series_ex(ctx: &Context, key: &ValkeyString, options: TimeSeriesOptions) -> ValkeyResult<()> {
    let _key = ValkeyKeyWritable::open(ctx.ctx, key);
    // check if this refers to an existing series
    if !_key.is_empty() {
        return Err(ValkeyError::Str("TSDB: the key already exists"));
    }

    let ts = create_series(key, options, ctx)?;
    _key.set_value(&VKM_SERIES_TYPE, ts)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "PROM.CREATE-SERIES", key);
    ctx.log_verbose("series created");

    Ok(())
}