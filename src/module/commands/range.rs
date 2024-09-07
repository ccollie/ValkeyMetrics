use crate::aggregators::Aggregator;
use crate::arg_parse::{parse_duration_arg, parse_integer_arg, parse_number_with_unit, parse_timestamp};
use crate::common::types::Timestamp;
use crate::module::result::sample_to_result;
use crate::module::{parse_timestamp_arg, with_timeseries_mut};
use crate::storage::{AggregationOptions, BucketTimestamp, RangeAlignment, RangeOptions};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use std::iter::Skip;
use std::vec::IntoIter;
use crate::module::commands::range_utils::get_range;

const CMD_ARG_FILTER_BY_VALUE: &str = "FILTER_BY_VALUE";
const CMD_ARG_FILTER_BY_TS: &str = "FILTER_BY_TS";
const  CMD_ARG_ALIGN: &str = "ALIGN";
const CMD_ARG_COUNT: &str = "COUNT";
const CMD_ARG_EMPTY: &str = "EMPTY";
const CMD_ARG_AGGREGATION: &str = "AGGREGATION";
const CMD_ARG_BUCKET_TIMESTAMP: &str = "BUCKETTIMESTAMP";
const MAX_TS_VALUES_FILTER: usize = 25;


pub fn range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let options = parse_range_options(&mut args)?;

    args.done()?;

    with_timeseries_mut(ctx, &key, |series| {
        let start = options.start.to_series_timestamp(series);
        let end = options.end.to_series_timestamp(series);

        if start > end {
            return Err(ValkeyError::Str("ERR invalid range"));
        }

        let samples = get_range(series, &options, false);
        let result = samples.iter().map(|s| sample_to_result(s.timestamp, s.value)).collect();
        Ok(ValkeyValue::Array(result))
    })
}

pub fn parse_range_options(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<RangeOptions> {
    let mut options = RangeOptions::default();
    options.start = parse_timestamp_arg(args.next_str()?, "startTimestamp")?;
    options.end = parse_timestamp_arg(args.next_str()?, "endTimestamp")?;

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_ALIGN) => {
                let next = args.next_str()?;
                options.alignment = Some(parse_alignment(next)?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_VALUE) => {
                let min = parse_number_with_unit(args.next_str()?)
                    .map_err(|_| ValkeyError::Str("TSDB: cannot parse filter min parameter"))?;
                let max = parse_number_with_unit(args.next_str()?)
                    .map_err(|_| ValkeyError::Str("TSDB: cannot parse filter max parameter"))?;
                options.set_value_range(min, max)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_TS) => {
                options.set_valid_timestamps(parse_timestamp_filter(args)?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_AGGREGATION) => {
                options.aggregation = Some(parse_aggregation_args(args)?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_COUNT) => {
                let next = args.next_arg()?;
                let count = parse_integer_arg(&next, CMD_ARG_COUNT, false)
                    .map_err(|_| ValkeyError::Str("TSDB: COUNT must be a positive integer"))?;
                if count > usize::MAX as i64 {
                    return Err(ValkeyError::Str("TSDB: COUNT value is too large"));
                }
                options.count = Some(count as usize);
            }
            _ => {}
        }
    }
    Ok(options)
}

fn parse_alignment(align: &str) -> ValkeyResult<RangeAlignment> {
    let alignment = match align {
        arg if arg.eq_ignore_ascii_case("start") => RangeAlignment::Start,
        arg if arg.eq_ignore_ascii_case("end") => RangeAlignment::End,
        arg if arg.len() == 1 => {
            let c = arg.chars().next().unwrap();
            match c {
                '-' => RangeAlignment::Start,
                '+' => RangeAlignment::End,
                _ => return Err(ValkeyError::Str("TSDB: unknown ALIGN parameter")),
            }
        }
        _ => {
            let timestamp = parse_timestamp(align)
                .map_err(|_| ValkeyError::Str("TSDB: unknown ALIGN parameter"))?;
            RangeAlignment::Timestamp(timestamp)
        }
    };
    Ok(alignment)
}

fn is_range_command_keyword(arg: &str) -> bool {
    match arg {
        arg if arg.eq_ignore_ascii_case(CMD_ARG_ALIGN) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_COUNT) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_AGGREGATION) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_BUCKET_TIMESTAMP) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_TS) => true,
        arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_VALUE) => true,
        _ => false,
    }
}

fn parse_timestamp_filter(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<Vec<Timestamp>> {
    let mut values: Vec<Timestamp> = Vec::new();
    while let Ok(arg) = args.next_str() {
        if is_range_command_keyword(arg) {
            break;
        }
        if let Ok(timestamp) = parse_timestamp(arg) {
            values.push(timestamp);
        } else  {
            return Err(ValkeyError::Str("TSDB: cannot parse timestamp"));
        }
        if values.len() == MAX_TS_VALUES_FILTER {
            break
        }
    }
    if values.is_empty() {
        return Err(ValkeyError::Str("TSDB: FILTER_BY_TS one or more arguments are missing"));
    }
    values.sort();
    values.dedup();
    Ok(values)
}


pub fn parse_aggregation_args(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<AggregationOptions> {
    // AGGREGATION token already seen
    let agg_str = args.next_str()
        .map_err(|_e| ValkeyError::Str("TSDB: Error parsing AGGREGATION"))?;
    let aggregator = Aggregator::try_from(agg_str)?;
    let bucket_duration = parse_duration_arg(&args.next_arg()?)
        .map_err(|_e| ValkeyError::Str("Error parsing bucketDuration"))?;

    let mut aggr: AggregationOptions = AggregationOptions {
        aggregator,
        bucket_duration,
        timestamp_output: BucketTimestamp::Start,
        time_delta: 0,
        empty: false,
    };
    let mut arg_count: usize = 0;

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_EMPTY) => {
                aggr.empty = true;
                arg_count += 1;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_BUCKET_TIMESTAMP) => {
                let next = args.next_str()?;
                arg_count += 1;
                aggr.timestamp_output = BucketTimestamp::try_from(next)?;
            }
            _ => {
                return Err(ValkeyError::Str("TSDB: unknown AGGREGATION option"))
            }
        }
        if arg_count == 3 {
            break;
        }
    }

    Ok(aggr)
}
