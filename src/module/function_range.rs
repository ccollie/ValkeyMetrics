use std::iter::Skip;
use std::time::Duration;
use std::vec::IntoIter;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use crate::aggregators::Aggregator;
use crate::arg_parse::{parse_duration_arg, parse_integer_arg, parse_number_with_unit, parse_timestamp};
use crate::common::types::Timestamp;
use crate::error::TsdbResult;
use crate::module::{get_timeseries_mut, parse_timestamp_arg};
use crate::module::arg_parse::TimestampRangeValue;
use crate::module::result::sample_to_result;
use crate::storage::{RangeFilter, ValueFilter};


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
pub enum BucketTimestamp {
    #[default]
    Start,
    End,
    Mid
}

impl BucketTimestamp {
    pub fn calculate(&self, ts: Timestamp, time_delta: i64) -> Timestamp {
        match self {
            Self::Start => ts,
            Self::Mid => ts + time_delta / 2,
            Self::End => ts + time_delta,
        }
    }

}
impl TryFrom<&str> for BucketTimestamp {
    type Error = RedisError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() == 1 {
            let c = value.chars().next().unwrap();
            match c {
                '-' => return Ok(BucketTimestamp::Start),
                '+' => return Ok(BucketTimestamp::End),
                _ => {}
            }
        }
        match value {
            value if value.eq_ignore_ascii_case("start") => return Ok(BucketTimestamp::Start),
            value if value.eq_ignore_ascii_case("end") => return Ok(BucketTimestamp::End),
            value if value.eq_ignore_ascii_case("mid") => return Ok(BucketTimestamp::Mid),
            _ => {}
        }
        return Err(RedisError::Str("TSDB: invalid BUCKETTIMESTAMP parameter"))
    }
}

impl TryFrom<&RedisString> for BucketTimestamp {
    type Error = RedisError;
    fn try_from(value: &RedisString) -> Result<Self, Self::Error> {
        value.to_string_lossy().as_str().try_into()
    }
}

#[derive(Debug, Clone)]
pub struct AggregationOptions {
    pub aggregator: Aggregator,
    pub align: Option<RangeAlignment>,
    pub bucket_duration: Duration,
    pub timestamp_output: BucketTimestamp,
    pub empty: bool
}

#[derive(Debug, Default, Clone)]
pub struct RangeOptions {
    pub start: TimestampRangeValue,
    pub end: TimestampRangeValue,
    pub count: Option<usize>,
    pub aggregation: Option<AggregationOptions>,
    pub filter: Option<RangeFilter>,
}

impl RangeOptions {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self {
            start: TimestampRangeValue::Value(start),
            end: TimestampRangeValue::Value(end),
            ..Default::default()
        }
    }

    pub fn set_value_range(&mut self, start: f64, end: f64) -> TsdbResult<()> {
        let mut filter = self.filter.clone().unwrap_or_default();
        filter.value = Some(ValueFilter::new(start, end)?);
        self.filter = Some(filter);
        Ok(())
    }

    pub fn set_valid_timestamps(&mut self, timestamps: Vec<Timestamp>) {
        let mut filter = self.filter.clone().unwrap_or_default();
        filter.timestamps = Some(timestamps);
        self.filter = Some(filter);
    }
}

pub fn range(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;

    // Safety: passing 'true' as the third parameter ensures the key exists
    let series = get_timeseries_mut(ctx, &key, true)?.unwrap();
    let options = parse_range_options(&mut args)?;

    args.done()?;

    let start = options.start.to_series_timestamp(series);
    let end = options.end.to_series_timestamp(series);

    if start > end {
        return Err(RedisError::Str("ERR invalid range"));
    }

    let mut samples = series.get_range(start, end)
        .map_err(|e| {
            ctx.log_warning(format!("ERR fetching range {:?}", e).as_str());
            RedisError::Str("ERR fetching range")
        })?;

    if let Some(value_filter) = options.filter.as_ref() {
        samples = samples.into_iter().filter(|s| value_filter.filter(s.timestamp, s.value)).collect();
    }
    if let Some(aggregation) = options.aggregation.as_ref() {
        samples = series.aggregate_range(samples, aggregation)
            .map_err(|e| {
                ctx.log_warning(format!("ERR aggregating range {:?}", e).as_str());
                RedisError::Str("ERR aggregating range")
            })?;
    }

    let result = samples.iter().map(|s| sample_to_result(s.timestamp, s.value)).collect();
    Ok(RedisValue::Array(result))
}

pub fn parse_range_options(args: &mut Skip<IntoIter<RedisString>>) -> RedisResult<RangeOptions> {
    let mut options = RangeOptions::default();
    options.start = parse_timestamp_arg(args.next_str()?, "startTimestamp")?;
    options.end = parse_timestamp_arg(args.next_str()?, "endTimestamp")?;

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_FILTER_BY_VALUE) => {
                let min = parse_number_with_unit(args.next_str()?)
                    .map_err(|_| RedisError::Str("TSDB: cannot parse filter min parameter"))?;
                let max = parse_number_with_unit(args.next_str()?)
                    .map_err(|_| RedisError::Str("TSDB: cannot parse filter max parameter"))?;
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
                    .map_err(|_| RedisError::Str("TSDB: COUNT must be a positive integer"))?;
                if count > usize::MAX as i64 {
                    return Err(RedisError::Str("TSDB: COUNT value is too large"));
                }
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
        timestamp_output: BucketTimestamp::Start,
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
                aggr.timestamp_output = BucketTimestamp::try_from(next)?;
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