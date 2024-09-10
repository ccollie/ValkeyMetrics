use std::collections::BTreeSet;
use crate::common::current_time_millis;
use crate::common::types::{Label, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::storage::{MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use chrono::DateTime;
use metricsql_parser::parser::{parse_duration_value, parse_metric_name as parse_metric, parse_number};
use metricsql_parser::prelude::Matchers;
use metricsql_runtime::parse_metric_selector;
use std::iter::Skip;
use std::time::Duration;
use std::vec::IntoIter;
use valkey_module::{NextArg, ValkeyError, ValkeyResult, ValkeyString};
use crate::aggregators::Aggregator;
use crate::module::types::{AggregationOptions, BucketTimestamp, RangeGroupingOptions, TimestampRange, TimestampRangeValue, ValueFilter};

const MAX_TS_VALUES_FILTER: usize = 16;
const CMD_ARG_COUNT: &'static str = "COUNT";
const CMD_PARAM_REDUCER: &'static str = "REDUCE";

pub fn parse_number_arg(arg: &ValkeyString, name: &str) -> ValkeyResult<f64> {
    if let Ok(value) = arg.parse_float() {
        return Ok(value);
    }
    let arg_str = arg.to_string_lossy();
    parse_number_with_unit(&arg_str)
        .map_err(|_| {
            if name.is_empty() {
              ValkeyError::Str("TSDB: invalid number")
            } else {
                ValkeyError::String(format!("TSDB: cannot parse {name} as number"))
            }
        })
}

pub fn parse_integer_arg(arg: &ValkeyString, name: &str, allow_negative: bool) -> ValkeyResult<i64> {
    let value = if let Ok(val) = arg.parse_integer() {
        val
    } else {
        let num = parse_number_arg(arg, name)?;
        if num != num.floor() {
            return Err(ValkeyError::Str("TSDB: value must be an integer"));
        }
        if num > i64::MAX as f64 {
            return Err(ValkeyError::Str("TSDB: value is too large"));
        }
        num as i64
    };
    if !allow_negative && value < 0 {
        let msg = format!("TSDB: {} must be a non-negative integer", name);
        return Err(ValkeyError::String(msg));
    }
    Ok(value)
}

pub fn parse_timestamp_arg(arg: &ValkeyString) -> ValkeyResult<Timestamp> {
    if arg.len() == 1 {
        let arg = arg.as_slice();
        if arg[0] == b'*' {
            return Ok(current_time_millis());
        }
    }
    let value = if let Ok(value) = arg.parse_integer() {
        value
    } else {
        let arg_str = arg.to_string_lossy();
        let value = DateTime::parse_from_rfc3339(&arg_str)
            .map_err(|_| ValkeyError::Str("TSDB: invalid timestamp"))?;
        value.timestamp_millis()
    };
    if value < 0 {
        return Err(ValkeyError::Str("TSDB: invalid timestamp, must be a non-negative value"));
    }
    Ok(value as Timestamp)
}

pub fn parse_timestamp(arg: &str) -> ValkeyResult<Timestamp> {
    // todo: handle +,
    if arg == "*" {
        return Ok(current_time_millis());
    }
    let value = if let Ok(dt) = arg.parse::<i64>() {
        dt
    } else {
        let value = DateTime::parse_from_rfc3339(arg)
            .map_err(|_| ValkeyError::Str("invalid timestamp"))?;
        value.timestamp_millis()
    };
    if value < 0 {
        return Err(ValkeyError::Str("TSDB: invalid timestamp, must be a non-negative integer"));
    }
    Ok(value)
}


pub fn parse_timestamp_range_value(arg: &str) -> ValkeyResult<TimestampRangeValue> {
    TimestampRangeValue::try_from(arg)
}

pub fn parse_duration_arg(arg: &ValkeyString) -> ValkeyResult<Duration> {
    if let Ok(value) = arg.parse_integer() {
        if value < 0 {
            return Err(ValkeyError::Str("TSDB: invalid duration, must be a non-negative integer"));
        }
        return Ok(Duration::from_millis(value as u64));
    }
    let value_str = arg.to_string_lossy();
    parse_duration(&value_str)
}

pub fn parse_duration(arg: &str) -> ValkeyResult<Duration> {
    match parse_duration_value(arg, 1) {
        Ok(d) => Ok(Duration::from_millis(d as u64)),
        Err(_e) => {
            match arg.parse::<i64>() {
                Ok(v) => Ok(Duration::from_millis(v as u64)),
                Err(_e) => {
                    let str = format!("ERR: failed to parse duration: {}", arg);
                    Err(ValkeyError::String(str))
                },
            }
        },
    }
}

pub fn parse_double(arg: &str) -> ValkeyResult<f64> {
    arg.parse::<f64>().map_err(|_e| {
        ValkeyError::Str("TSDB: invalid value")
    })
}

pub fn parse_number_with_unit(arg: &str) -> TsdbResult<f64> {
    parse_number(arg).map_err(|_e| {
        TsdbError::InvalidNumber(arg.to_string())
    })
}

pub fn parse_series_selector(arg: &str) -> TsdbResult<Matchers> {
    parse_metric_selector(arg).map_err(|_e| {
        TsdbError::InvalidSeriesSelector(arg.to_string())
    })
}

pub fn parse_metric_name(arg: &str) -> TsdbResult<Vec<Label>> {
    parse_metric(arg).map_err(|_e| {
        TsdbError::InvalidMetric(arg.to_string())
    })
}

pub fn parse_chunk_size(arg: &str) -> ValkeyResult<usize> {
    fn get_error_result() -> ValkeyResult<usize> {
        let msg = format!("TSDB: CHUNK_SIZE value must be an integer multiple of 2 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]");
        Err(ValkeyError::String(msg))
    }

    let chunk_size = parse_number_with_unit(arg).map_err(|_e| {
        ValkeyError::Str("TSDB: invalid chunk size")
    })?;

    if chunk_size != chunk_size.floor() {
        return get_error_result()
    }
    if chunk_size < MIN_CHUNK_SIZE as f64 || chunk_size > MAX_CHUNK_SIZE as f64 {
        return get_error_result()
    }
    let chunk_size = chunk_size as usize;
    if chunk_size % 2 != 0 {
        return get_error_result()
    }
    Ok(chunk_size)
}


pub fn parse_timestamp_range(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<TimestampRange> {
    let first_arg = args.next_str()?;
    let start = parse_timestamp_range_value(first_arg)?;
    let end_value = if let Ok(arg) = args.next_str() {
        parse_timestamp_range_value(arg).map_err(|_e| {
            ValkeyError::Str("ERR invalid end timestamp")
        })?
    } else {
        TimestampRangeValue::Latest
    };
    TimestampRange::new(start, end_value)
}

pub fn parse_timestamp_filter(args: &mut Skip<IntoIter<ValkeyString>>, is_valid_arg: fn(&str) -> bool) -> ValkeyResult<Vec<Timestamp>> {
    // FILTER_BY_TS already seen
    let mut values: Vec<Timestamp> = Vec::new();
    while let Ok(arg) = args.next_str() {
        // TODO: !!! problem. arg should not be consumed if the func returns true
        if is_valid_arg(arg) {
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

pub fn parse_value_filter(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<ValueFilter> {
    let min = parse_number_with_unit(args.next_str()?)
        .map_err(|_| ValkeyError::Str("TSDB: cannot parse filter min parameter"))?;
    let max = parse_number_with_unit(args.next_str()?)
        .map_err(|_| ValkeyError::Str("TSDB: cannot parse filter max parameter"))?;
    if max < min || min > max {
        return Err(ValkeyError::Str("TSDB: filter min parameter is greater than max"));
    }
    ValueFilter::new(min, max)
}

pub fn parse_count(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<usize> {
    let next = args.next_arg()?;
    let count = parse_integer_arg(&next, CMD_ARG_COUNT, false)
        .map_err(|_| ValkeyError::Str("TSDB: COUNT must be a positive integer"))?;
    if count > usize::MAX as i64 {
        return Err(ValkeyError::Str("TSDB: COUNT value is too large"));
    }
    Ok(count as usize)
}

pub fn parse_label_list(args: &mut Skip<IntoIter<ValkeyString>>, is_cmd_token: fn(&str) -> bool) -> ValkeyResult<BTreeSet<String>> {
    let mut labels: BTreeSet<String> = BTreeSet::new();

    while let Ok(label) = args.next_str() {
        // TODO: !!! problem. arg should not be consumed if the func returns true
        if is_cmd_token(label) {
            break;
        }
        if labels.contains(label) {
            let msg = format!("TSDB: duplicate label: {label}");
            return Err(ValkeyError::String(msg));
        }
        labels.insert(label.to_string());
    }

    Ok(labels)
}

const CMD_ARG_EMPTY: &'static str = "EMPTY";
const CMD_ARG_BUCKET_TIMESTAMP: &str = "BUCKETTIMESTAMP";

pub fn parse_aggregation_options(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<AggregationOptions> {
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


pub fn parse_grouping_params(args: &mut Skip<IntoIter<ValkeyString>>) -> ValkeyResult<RangeGroupingOptions> {
    // GROUPBY token already seen
    let label = args.next_str()?;
    let token = args.next_str()
        .map_err(|_| ValkeyError::Str("TSDB: missing REDUCE"))?;
    if !token.eq_ignore_ascii_case(CMD_PARAM_REDUCER) {
        let msg = format!("TSDB: expected \"{CMD_PARAM_REDUCER}\", found \"{token}\"");
        return Err(ValkeyError::String(msg));
    }
    let agg_str = args.next_str()
        .map_err(|_e| ValkeyError::Str("TSDB: Error parsing grouping reducer"))?;

    let aggregator = Aggregator::try_from(agg_str)
        .map_err(|_| {
            let msg = format!("TSDB: invalid grouping aggregator \"{}\"", agg_str);
            ValkeyError::String(msg)
        })?;

    Ok(
        RangeGroupingOptions {
            group_label: label.to_string(),
            aggregator
        }
    )
}
