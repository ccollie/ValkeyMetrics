use crate::error_consts;
use crate::module::arg_parse::*;
use crate::module::VKM_SERIES_TYPE;
use crate::series::index::with_timeseries_index;
use crate::series::time_series::TimeSeries;
use crate::series::TimeSeriesOptions;
use metricsql_common::types::Label;
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{
    Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK,
};


/// Create a new time series
///
/// VM.CREATE-SERIES key
///   [METRIC metric]
///   [LABELS label1=value1 label2=value2 ...]
///   [RETENTION retentionPeriod]
///   [COMPRESSION <pco|gorilla|uncompressed>]
///   [CHUNK_SIZE chunkSize]
///   [DUPLICATE_POLICY duplicatePolicy]
///   [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///   [DEDUPE_INTERVAL duplicateTimediff]
pub fn create(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let (parsed_key, options) = parse_create_options(args)?;

    create_and_store_series(ctx, &parsed_key, options)?;

    VALKEY_OK
}

const VALID_ARGS: [CommandArgToken; 9] = [
    CommandArgToken::Metric,
    CommandArgToken::Labels,
    CommandArgToken::Retention,
    CommandArgToken::Compression,
    CommandArgToken::ChunkSize,
    CommandArgToken::DedupeInterval,
    CommandArgToken::DuplicatePolicy,
    CommandArgToken::SignificantDigits,
    CommandArgToken::DecimalDigits,
];

fn is_valid_command_arg(arg: CommandArgToken) -> bool {
   VALID_ARGS.contains(&arg)
}

pub fn parse_create_options(
    args: Vec<ValkeyString>,
) -> ValkeyResult<(ValkeyString, TimeSeriesOptions)> {
    let mut args = args.into_iter().skip(1).peekable();
    let metric_set = false;

    let mut options = TimeSeriesOptions::default();

    let key = args
        .next()
        .ok_or(ValkeyError::Str("Err missing key argument"))?;

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::ChunkSize => options.chunk_size = Some(parse_chunk_size(&mut args)?),
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
            CommandArgToken::DedupeInterval => options.dedupe_interval = Some(parse_dedupe_interval(&mut args)?),
            CommandArgToken::DuplicatePolicy => options.duplicate_policy = Some(parse_duplicate_policy(&mut args)?),
            CommandArgToken::Metric => {
                if metric_set {
                    return Err(ValkeyError::Str(error_consts::METRIC_ALREADY_SET));
                }
                let metric = args.next_string()?;
                options.labels = parse_metric_name(&metric)
                    .map_err(|_e| ValkeyError::Str(error_consts::INVALID_METRIC))?;
            }
            CommandArgToken::Labels => {
                if metric_set {
                    return Err(ValkeyError::Str(error_consts::METRIC_ALREADY_SET));
                }
                options.labels = parse_labels(&mut args)?;
            }
            CommandArgToken::Retention => options.retention(parse_retention(&mut args)?),
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

    if options.labels.is_empty() {
        return Err(ValkeyError::Str(
            error_consts::INVALID_OR_MISSING_METRIC_NAME,
        ));
    }

    Ok((key, options))
}

fn parse_labels(args: &mut CommandArgIterator) -> ValkeyResult<Vec<Label>> {
    let label_map = parse_key_value_pairs(args, is_valid_command_arg)?;
    let mut labels = Vec::new();
    for (label_name, label_value) in label_map {
        labels.push(Label::new(label_name, label_value));
    }
    Ok(labels)
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
            return Err(ValkeyError::Str(error_consts::DUPLICATE_SERIES));
        }

        index.index_time_series(&mut ts, key.iter().as_slice())?;
        Ok(ts)
    })
}

pub(crate) fn create_and_store_series(
    ctx: &Context,
    key: &ValkeyString,
    options: TimeSeriesOptions,
) -> ValkeyResult<()> {
    let _key = ValkeyKeyWritable::open(ctx.ctx, key);
    // check if this refers to an existing series
    if !_key.is_empty() {
        return Err(ValkeyError::Str(error_consts::DUPLICATE_KEY));
    }

    let ts = create_series(key, options, ctx)?;
    _key.set_value(&VKM_SERIES_TYPE, ts)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "VM.CREATE-SERIES", key);
    ctx.log_verbose("series created");

    Ok(())
}
