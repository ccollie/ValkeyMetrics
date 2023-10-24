use std::iter::Skip;
use std::time::Duration;
use std::vec::IntoIter;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use crate::aggregators::Aggregator;
use crate::common::{parse_duration_arg, parse_integer_arg, parse_number_with_unit, parse_timestamp};
use crate::common::types::Timestamp;
use crate::module::{get_timeseries_mut, parse_timestamp_arg};
use crate::module::result::sample_to_result;


const CMD_ARG_FILTER_BY_VALUE: &str = "FILTER_BY_VALUE";
const CMD_ARG_FILTER_BY_TS: &str = "FILTER_BY_TS";
const  CMD_ARG_ALIGN: &str = "ALIGN";
const CMD_ARG_COUNT: &str = "COUNT";
const CMD_ARG_EMPTY: &str = "EMPTY";
const CMD_ARG_AGGREGATION: &str = "AGGREGATION";
const CMD_ARG_BUCKET_TIMESTAMP: &str = "BUCKETTIMESTAMP";
const MAX_TS_VALUES_FILTER: usize = 25;

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum RangeAlignment {
    #[default]
    Default,
    Start,
    End,
    Timestamp(Timestamp),
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketTimestampOutput {
    #[default]
    Start,
    End,
    Mid
}

impl TryFrom<&str> for BucketTimestampOutput {
    type Error = RedisError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() == 1 {
            let c = value.chars().next().unwrap();
            match c {
                '-' => return Ok(BucketTimestampOutput::Start),
                '+' => return Ok(BucketTimestampOutput::End),
                _ => {}
            }
        }
        match value {
            value if value.eq_ignore_ascii_case("start") => return Ok(BucketTimestampOutput::Start),
            value if value.eq_ignore_ascii_case("end") => return Ok(BucketTimestampOutput::End),
            value if value.eq_ignore_ascii_case("mid") => return Ok(BucketTimestampOutput::Mid),
            _ => {}
        }
        return Err(RedisError::Str("TSDB: invalid BUCKETTIMESTAMP parameter"))
    }
}

impl TryFrom<&RedisString> for BucketTimestampOutput {
    type Error = RedisError;

    fn try_from(value: &RedisString) -> Result<Self, Self::Error> {
        value.to_string_lossy().as_str().try_into()
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct ValueFilter {
    pub min: f64,
    pub max: f64,
}

impl ValueFilter {
    fn new(min: f64, max: f64) -> RedisResult<Self> {
        if min > max {
            return Err(RedisError::Str("ERR invalid range"));
        }
        Ok(Self { min, max })
    }
}

#[derive(Debug, Clone)]
pub struct AggregationOptions {
    pub aggregator: Aggregator,
    pub align: Option<RangeAlignment>,
    pub bucket_duration: Duration,
    pub timestamp_output: BucketTimestampOutput,
    pub empty: bool
}

#[derive(Debug, Default, Clone)]
pub struct RangeOptions {
    pub start: Timestamp,
    pub end: Timestamp,
    pub count: Option<usize>,
    pub aggregation: Option<AggregationOptions>,
    pub value_filter: Option<ValueFilter>,
    pub timestamps_filter: Option<Vec<Timestamp>>
}

pub fn range(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;

    // Safety: passing 'true' as the third parameter ensures the key exists
    let series = get_timeseries_mut(ctx, &key, true)?.unwrap();

    let from = parse_timestamp_arg(args.next_str()?, "startTimestamp")?;
    let to = parse_timestamp_arg(args.next_str()?, "endTimestamp")?;

    args.done()?;

    let start = from.to_timestamp();
    let end = to.to_timestamp();

    if start > end {
        return Err(RedisError::Str("ERR invalid range"));
    }

    let samples = series.get_range(start, end)
        .map_err(|e| {
            ctx.log_warning(format!("ERR fetching range {:?}", e).as_str());
            RedisError::Str("ERR fetching range")
        })?;

    let result = samples.iter().map(|s| sample_to_result(s.timestamp, s.value)).collect();
    Ok(RedisValue::Array(result))
}

pub fn parse_range_options(args: &mut Skip<IntoIter<RedisString>>) -> RedisResult<RangeOptions> {
    let mut options = RangeOptions::default();
    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_VALUE) => {
                let min = parse_number_with_unit(args.next_str()?)
                    .map_err(|_| RedisError::Str("TSDB: cannot parse filter min parameter"))?;
                let max = parse_number_with_unit(args.next_str()?)
                    .map_err(|_| RedisError::Str("TSDB: cannot parse filter max parameter"))?;
                let filter = ValueFilter::new(min, max);
                options.value_filter = Some(filter?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_TS) => {
                options.timestamps_filter = Some(parse_timestamp_filter(args)?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_AGGREGATION) => {
                options.aggregation = Some(parse_aggregation_args(args)?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_COUNT) => {
                let next = args.next_arg()?;
                let count = parse_integer_arg(&next, CMD_ARG_COUNT, false)
                    .map_err(|_| RedisError::Str("TSDB: COUNT must be a positive integer"))?;
                options.count = Some(count as usize);
            }
            _ => {}
        }
    }
    Ok(options)
}

fn parse_alignment(align: &str) -> RedisResult<RangeAlignment> {
    let alignment = match align {
        arg if arg.eq_ignore_ascii_case("start") => RangeAlignment::Start,
        arg if arg.eq_ignore_ascii_case("end") => RangeAlignment::End,
        arg if arg.len() == 1 => {
            let c = arg.chars().next().unwrap();
            match c {
                '-' => RangeAlignment::Start,
                '+' => RangeAlignment::End,
                _ => return Err(RedisError::Str("TSDB: unknown ALIGN parameter")),
            }
        }
        _ => {
            let timestamp = parse_timestamp(align)
                .map_err(|_| RedisError::Str("TSDB: unknown ALIGN parameter"))?;
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

fn parse_timestamp_filter(args: &mut Skip<IntoIter<RedisString>>) -> RedisResult<Vec<Timestamp>> {
    let mut values: Vec<Timestamp> = Vec::new();
    while let Ok(arg) = args.next_str() {
        if is_range_command_keyword(arg) {
            break;
        }
        if let Ok(timestamp) = parse_timestamp(&arg) {
            values.push(timestamp);
        } else  {
            return Err(RedisError::Str("TSDB: cannot parse timestamp"));
        }
        if values.len() == MAX_TS_VALUES_FILTER {
            break
        }
    }
    if values.is_empty() {
        return Err(RedisError::Str("TSDB: FILTER_BY_TS one or more arguments are missing"));
    }
    values.sort();
    values.dedup();
    return Ok(values);
}


pub fn parse_aggregation_args(args: &mut Skip<IntoIter<RedisString>>) -> RedisResult<AggregationOptions> {
    // AGGREGATION token already seen
    let agg_str = args.next_str()
        .map_err(|_e| RedisError::Str("TSDB: Error parsing AGGREGATION"))?;
    let aggregator = Aggregator::try_from(agg_str)?;
    let bucket_duration = parse_duration_arg(&args.next_arg()?)
        .map_err(|_e| RedisError::Str("Error parsing bucketDuration"))?;

    let mut aggr: AggregationOptions = AggregationOptions {
        aggregator,
        align: None,
        bucket_duration,
        timestamp_output: BucketTimestampOutput::Start,
        empty: false,
    };
    let mut arg_count: usize = 0;

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_ALIGN) => {
                let next = args.next_str()?;
                arg_count += 1;
                aggr.align = Some(parse_alignment(next)?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_EMPTY) => {
                aggr.empty = true;
                arg_count += 1;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_BUCKET_TIMESTAMP) => {
                let next = args.next_str()?;
                arg_count += 1;
                aggr.timestamp_output = BucketTimestampOutput::try_from(next)?;
            }
            _ => {
                return Err(RedisError::Str("TSDB: unknown AGGREGATION option"))
            }
        }
        if arg_count == 3 {
            break;
        }
    }


    Ok(aggr)
}

fn get_arg_keyword_position(args: &Vec<RedisString>, arg: &str, requires_param: bool) -> RedisResult<Option<usize>> {
    let needle = arg.as_bytes();
    if let Some(pos) = args.iter().position(|probe| probe.as_slice() == needle) {
        if requires_param && pos + 1 >= args.len() {
            return Err(RedisError::WrongArity);
        }
        return Ok(Some(pos));
    }
    Ok(None)
}