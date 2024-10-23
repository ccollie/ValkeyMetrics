use crate::common::rounding::RoundingStrategy;
use crate::error_consts;
use crate::globals::with_timeseries_index;
use crate::module::arg_parse::*;
use crate::module::VKM_SERIES_TYPE;
use crate::series::time_series::TimeSeries;
use crate::series::{ChunkCompression, TimeSeriesOptions};
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};

const MAX_SIGNIFICANT_DIGITS: u8 = 16;
const MAX_DECIMAL_DIGITS: u8 = 16;

/// Create a new time series
///
/// VM.CREATE-SERIES key metric
///   [RETENTION retentionPeriod]
///   [ENCODING <COMPRESSED|UNCOMPRESSED>]
///   [CHUNK_SIZE size]
///   [DUPLICATE_POLICY policy]
///   [SIGNIFICANT_DIGITS digits | DECIMAL_DIGITS digits]
///   [DEDUPE_INTERVAL duplicateTimediff]
pub fn create(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let (parsed_key, options) = parse_create_options(args)?;

    create_and_store_series(ctx, &parsed_key, options)?;

    VALKEY_OK
}

pub fn parse_create_options(args: Vec<ValkeyString>) -> ValkeyResult<(ValkeyString, TimeSeriesOptions)> {
    let mut args = args.into_iter().skip(1).peekable();

    let mut options = TimeSeriesOptions::default();

    let key = args.next().ok_or(ValkeyError::Str("Err missing key argument"))?;

    let metric = args.next_string()?;
    options.labels = parse_metric_name(&metric)
        .map_err(|_e| ValkeyError::Str(error_consts::INVALID_METRIC))?;

    while let Ok(arg) = args.next_str() {
        let arg_upper = arg.to_ascii_uppercase();
        match arg_upper.as_str() {
            CMD_ARG_RETENTION => {
                options.retention(parse_retention(&mut args)?)
            }
            CMD_ARG_DEDUPE_INTERVAL => {
                options.dedupe_interval = Some(parse_dedupe_interval(&mut args)?)
            }
            CMD_ARG_DUPLICATE_POLICY => {
                options.duplicate_policy = Some(parse_duplicate_policy(&mut args)?)
            }
            CMD_ARG_SIGNIFICANT_DIGITS => {
                let next = args.next_u64()?;
                if next > MAX_SIGNIFICANT_DIGITS as u64 {
                    let msg = format!("ERR SIGNIFICANT_DIGITS must be between 0 and {MAX_SIGNIFICANT_DIGITS}");
                    return Err(ValkeyError::String(msg));
                }
                if options.rounding.is_some() {
                    let msg = "ERR rounding already set";
                    return Err(ValkeyError::Str(msg));
                }
                options.rounding = Some(RoundingStrategy::SignificantDigits(next as i32));
            }
            CMD_ARG_DECIMAL_DIGITS => {
                let next = args.next_u64()?;
                if next > MAX_DECIMAL_DIGITS as u64 {
                    let msg = format!("ERR DECIMAL_DIGITS must be between 0 and {MAX_DECIMAL_DIGITS}");
                    return Err(ValkeyError::String(msg));
                }
                if options.rounding.is_some() {
                    let msg = "ERR rounding already set";
                    return Err(ValkeyError::Str(msg));
                }
                options.rounding = Some(RoundingStrategy::DecimalDigits(next as i32));
            }
            CMD_ARG_CHUNK_SIZE => {
                options.chunk_size(parse_chunk_size(&mut args)?);
            }
            CMD_ARG_COMPRESSION => {
                let enc = args.next_string()?;
                match ChunkCompression::try_from(enc.as_str()) {
                    Ok(compression) => { options.chunk_compression = Some(compression); }
                    Err(_) => {
                        return Err(ValkeyError::Str(error_consts::INVALID_CHUNK_COMPRESSION));
                    }
                }
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
) -> ValkeyResult<TimeSeries> {
    let mut ts = TimeSeries::with_options(options)?;
    with_timeseries_index(ctx, |index| {
        // will return an error if the series already exists
        let existing_id = index.get_id_by_name_and_labels(&ts.metric_name, &ts.labels)?;
        if let Some(_id) = existing_id {
            let msg = format!("ERR: the series already exists : \"{}\"", ts.prometheus_metric_name());
            return Err(ValkeyError::String(msg));
        }

        index.index_time_series(&mut ts, key.iter().as_slice())?;
        Ok(ts)
    })
}

pub(crate) fn create_and_store_series(ctx: &Context, key: &ValkeyString, options: TimeSeriesOptions) -> ValkeyResult<()> {
    let _key = ValkeyKeyWritable::open(ctx.ctx, key);
    // check if this refers to an existing series
    if !_key.is_empty() {
        return Err(ValkeyError::Str("ERR: the key already exists"));
    }

    let ts = create_series(key, options, ctx)?;
    _key.set_value(&VKM_SERIES_TYPE, ts)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "VM.CREATE-SERIES", key);
    ctx.log_verbose("series created");

    Ok(())
}