use crate::arg_parse::{parse_chunk_size, parse_duration_arg};
use crate::error::{TsdbError, TsdbResult};
use crate::globals::with_timeseries_index;
use crate::index::TimeSeriesIndex;
use crate::module::VKM_SERIES_TYPE;
use crate::storage::time_series::TimeSeries;
use crate::storage::{DuplicatePolicy, TimeSeriesOptions};
use metricsql_parser::parser::parse_metric_name;
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};
use crate::common::METRIC_NAME_LABEL;

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
    let mut args = args.into_iter().skip(1);

    let mut options = TimeSeriesOptions::default();

    let key = args.next().ok_or(ValkeyError::Str("Err missing key argument"))?;

    let metric = args.next_string()?;
    let mut labels = parse_metric_name(&metric)
        .map_err(|_| ValkeyError::Str("ERR invalid METRIC"))?;

    let label = labels.iter().find(|x| x.name == METRIC_NAME_LABEL)
        .ok_or(ValkeyError::Str("ERR missing METRIC name"))?;

    options.metric = metric;
    options.metric_name = Some(label.value.clone());
    labels.retain(|x| x.name != METRIC_NAME_LABEL);
    options.labels = labels;

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
            arg if arg.eq_ignore_ascii_case(CMD_ARG_SIGNIFICANT_DIGITS) => {
                let next = args.next_u64()?;
                if next > MAX_SIGNIFICANT_DIGITS as u64 {
                    let msg = "ERR SIGNIFICANT_DIGITS must be between 0 and 16";
                    return Err(ValkeyError::Str(msg));
                }
                options.significant_digits = Some(next as u8);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_CHUNK_SIZE) => {
                let next = args.next_str()?;
                if let Ok(val) = parse_chunk_size(next) {
                    options.chunk_size(val);
                } else {
                    return Err(ValkeyError::Str("ERR invalid CHUNK_SIZE value"));
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