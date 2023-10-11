use std::time::Duration;
use chrono::DateTime;
use metricsql_engine::parse_metric_selector;
use metricsql_parser::common::Matchers;
use metricsql_parser::parser::{parse_duration_value, parse_number};
use redis_module::{RedisError, RedisResult};
use crate::common::current_time_millis;
use crate::common::types::{Timestamp, TimestampRangeValue};
use crate::error::{TsdbError, TsdbResult};
use crate::index::RedisContext;

pub fn parse_timestamp(arg: &str) -> TsdbResult<Timestamp> {
    // todo: handle +,
    if arg == "*" {
        return Ok(current_time_millis());
    }
    if let Ok(dt) = arg.parse::<i64>() {
        Ok(dt)
    } else {
        DateTime::parse_from_rfc3339(arg)
            .map_err(|_| {
                TsdbError::InvalidTimestamp(arg.to_string())
            }).and_then(|dt| {
            Ok(dt.timestamp_millis())
        })
    }
}

pub fn parse_timestamp_range_value(_ctx: &RedisContext, arg: &str) -> RedisResult<TimestampRangeValue> {
    TimestampRangeValue::try_from(arg)
}

pub fn parse_duration(arg: &str) -> RedisResult<Duration> {
    match parse_duration_value(arg, 1) {
        Ok(d) => Ok(Duration::from_millis(d as u64)),
        Err(_e) => {
            match arg.parse::<i64>() {
                Ok(v) => Ok(Duration::from_millis(v as u64)),
                Err(_e) => {
                    let str = format!("ERR: failed to parse duration: {}", arg);
                    Err(RedisError::String(str))
                },
            }
        },
    }
}

pub fn parse_number_with_unit(arg: &str) -> TsdbResult<f64> {
    parse_number(arg).map_err(|_e| {
        TsdbError::InvalidNumber(arg.to_string())
    })
}

pub fn parse_series_selector(arg: &str) -> TsdbResult<Matchers> {
    parse_metric_selector(arg).map_err(|_e| {
        TsdbError::InvalidSeriesSelector(arg.to_string())
    }).and_then(|m| {
        Ok(Matchers::new(m))
    })
}