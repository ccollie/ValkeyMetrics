use metricsql_common::label::Label;
use crate::arg_parse::*;
use crate::module::commands::create_series;
use crate::module::{get_timeseries_mut, VKM_SERIES_TYPE};
use crate::series::TimeSeriesOptions;
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use crate::error_consts;

///
/// VKM.ADD key timestamp value
///     [RETENTION duration]
///     [DUPLICATE_POLICY policy]
///     [DEDUPE_INTERVAL duration]
///     [CHUNK_SIZE chunkSize]
///     [METRIC metric | LABELS labelName labelValue ...]
///     [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///
pub fn add(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let timestamp = parse_timestamp(args.next_str()?)?;
    let value = args.next_f64()?;

    if let Some(series) = get_timeseries_mut(ctx, &key, true)? {
        args.done()?;
        series.add(timestamp, value, None).map(|_| ValkeyValue::Integer(timestamp))?;   
    }

    let mut options = TimeSeriesOptions::default();
    let mut labels_set = false;
    
    const TOKENS: [&str; 9] = [
        CMD_ARG_RETENTION,
        CMD_ARG_DEDUPE_INTERVAL,
        CMD_ARG_CHUNK_SIZE,
        CMD_ARG_DUPLICATE_POLICY,
        CMD_ARG_METRIC,
        CMD_ARG_LABELS,
        CMD_ARG_SIGNIFICANT_DIGITS,
        CMD_ARG_DECIMAL_DIGITS,
        CMD_ARG_COMPRESSION
    ];

    fn is_cmd_token(token: &str) -> bool {
        TOKENS.iter().any(|t| t.eq_ignore_ascii_case(token))
    }
    
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
                if !labels_set {
                    return Err(ValkeyError::Str(error_consts::LABELS_ALREADY_SET));
                }
                options.labels = parse_metric_name(args.next_str()?)?;
                labels_set = true;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_SIGNIFICANT_DIGITS) => {
                if options.rounding.is_some() {
                    return Err(ValkeyError::Str(error_consts::ROUNDING_ALREADY_SET));
                }
                let rounding = parse_significant_digit_rounding(&mut args)?;
                options.rounding = Some(rounding);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_DECIMAL_DIGITS) => {
                if options.rounding.is_some() {
                    return Err(ValkeyError::Str(error_consts::ROUNDING_ALREADY_SET));
                }
                let rounding = parse_decimal_digit_rounding(&mut args)?;
                options.rounding = Some(rounding);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_LABELS) => {
                if labels_set {
                    return Err(ValkeyError::Str(error_consts::LABELS_ALREADY_SET));
                }
                let labels = parse_key_value_pairs(&mut args, is_cmd_token)?;
                for (k, v) in labels {
                    options.labels.push(Label { name: k, value: v });
                }

                labels_set = true;
            }
            CMD_ARG_COMPRESSION => {
                options.chunk_compression = Some(parse_chunk_compression(&mut args)?);
            }
            _ => {
                return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
            }
        };
    }

    let mut ts = create_series(&key, options, ctx)?;
    ts.add(timestamp, value, None)?;

    let redis_key = ValkeyKeyWritable::open(ctx.ctx, &key);
    redis_key.set_value(&VKM_SERIES_TYPE, ts)?;

    Ok(ValkeyValue::Integer(timestamp))
}