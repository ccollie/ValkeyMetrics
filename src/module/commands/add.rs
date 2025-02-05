use crate::arg_parse::*;
use crate::error_consts;
use crate::module::commands::create_series;
use crate::module::{get_timeseries_mut, VKM_SERIES_TYPE};
use crate::series::{SampleAddResult, TimeSeriesOptions};
use metricsql_common::types::Label;
use metricsql_runtime::types::Timestamp;
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{
    Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

///
/// VM.ADD key timestamp value
///     [RETENTION duration]
///     [DUPLICATE_POLICY policy]
///     [DEDUPE_INTERVAL duration]
///     [CHUNK_SIZE chunkSize]
///     [METRIC metric | LABELS labelName labelValue ...]
///     [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///
pub fn add(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let timestamp_str = args[2].try_as_str()?;
    let timestamp = parse_timestamp(timestamp_str)?;
    let value = args[3].parse_float()?;

    if let Some(series) = get_timeseries_mut(ctx, &args[1], true)? {
        // args.done()?;
        return match series.add(timestamp, value, None) {
            SampleAddResult::Ok(ts) | SampleAddResult::Ignored(ts) => {
                let timestamp = if timestamp_str == "*" { Some(ts) } else { None };
                replicate_and_notify(ctx, args, timestamp);
                Ok(ValkeyValue::Integer(ts))
            }
            _ => Ok(ValkeyValue::Null),
        };
    }

    let original_args = args.clone();
    let mut args = args.into_iter().skip(4).peekable();

    let mut options = TimeSeriesOptions::default();
    let mut labels_set = false;

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default(); 
        match token {
            CommandArgToken::ChunkSize => {
                options.chunk_size(parse_chunk_size(&mut args)?)                
            }
            CommandArgToken::Compression => {
                options.chunk_compression = Some(parse_chunk_compression(&mut args)?);
            }
            CommandArgToken::DecimalDigits => {
                if options.rounding.is_some() {
                    return Err(ValkeyError::Str(error_consts::ROUNDING_ALREADY_SET));
                }
                let rounding = parse_decimal_digit_rounding(&mut args)?;
                options.rounding = Some(rounding);
            }
            CommandArgToken::DedupeInterval => {
                options.dedupe_interval = Some(parse_dedupe_interval(&mut args)?);
            }
            CommandArgToken::DuplicatePolicy => {
                options.duplicate_policy(parse_duplicate_policy(&mut args)?)
            }
            CommandArgToken::Labels => {
                if labels_set {
                    return Err(ValkeyError::Str(error_consts::LABELS_ALREADY_SET));
                }
                let labels = parse_key_value_pairs(&mut args, is_cmd_token)?;
                for (k, v) in labels {
                    options.labels.push(Label { name: k, value: v });
                }

                labels_set = true;
            }
            CommandArgToken::Metric => {
                if !labels_set {
                    return Err(ValkeyError::Str(error_consts::LABELS_ALREADY_SET));
                }
                options.labels = parse_metric_name(args.next_str()?)?;
                labels_set = true;                
            }
            CommandArgToken::Retention => {
                options.retention(parse_retention(&mut args)?)
            }
            CommandArgToken::SignificantDigits => {
                if options.rounding.is_some() {
                    return Err(ValkeyError::Str(error_consts::ROUNDING_ALREADY_SET));
                }
                let rounding = parse_significant_digit_rounding(&mut args)?;
                options.rounding = Some(rounding);                
            }
            _ => {
                return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
            }
        };
    }

    let key = &original_args[1];
    let mut ts = create_series(key, options, ctx)?;

    match ts.add(timestamp, value, None) {
        SampleAddResult::Ok(ts) | SampleAddResult::Ignored(ts) => {
            let redis_key = ValkeyKeyWritable::open(ctx.ctx, key);
            redis_key.set_value(&VKM_SERIES_TYPE, ts)?;

            replicate_and_notify(ctx, original_args, Some(timestamp));
            Ok(ValkeyValue::Integer(ts))
        }
        _ => Ok(ValkeyValue::Null),
    }
}

fn replicate_and_notify(ctx: &Context, args: Vec<ValkeyString>, timestamp: Option<Timestamp>) {
    if let Some(ts) = timestamp {
        // "*" could have a completely different value on a replica, so send the current value instead
        let ts_str = ts.to_string();
        let mut args = args;
        args.remove(0);
        args[1] = ctx.create_string(ts_str.as_bytes());
        let replication_args = args.iter().collect::<Vec<_>>();
        ctx.replicate("VM.ADD", &*replication_args);
        let key = args.swap_remove(0);
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "VM.ADD", &key);
    } else {
        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "VM.ADD", &args[2]);
    }
}

const VALID_TOKENS: [CommandArgToken; 9] = [
    CommandArgToken::ChunkSize,
    CommandArgToken::Compression,
    CommandArgToken::Labels,
    CommandArgToken::Metric,
    CommandArgToken::DecimalDigits,
    CommandArgToken::DedupeInterval,
    CommandArgToken::DuplicatePolicy,
    CommandArgToken::Retention,
    CommandArgToken::SignificantDigits,
];

fn is_cmd_token(token: CommandArgToken) -> bool {
    VALID_TOKENS.contains(&token)
}
