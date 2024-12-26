use std::fmt::Display;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

use crate::common::types::Timestamp;
use crate::module::arg_parse::parse_timestamp_range_value;
use crate::module::VKM_SERIES_TYPE;
use crate::series::time_series::{SeriesSampleIterator, TimeSeries};
use crate::series::types::ValueFilter;
use crate::series::{TimestampRange, TimestampValue};

pub fn parse_timestamp_arg(arg: &str, name: &str) -> Result<TimestampValue, ValkeyError> {
    parse_timestamp_range_value(arg).map_err(|_e| {
        let msg = format!("ERR invalid {} timestamp", name);
        ValkeyError::String(msg)
    })
}

pub fn get_series_iterator<'a>(
    series: &'a TimeSeries,
    date_range: TimestampRange,
    ts_filter: &'a Option<Vec<Timestamp>>,
    value_filter: &'a Option<ValueFilter>,
) -> SeriesSampleIterator<'a> {
    let (start_ts, end_ts) = date_range.get_series_range(series, false);
    SeriesSampleIterator::new(series, start_ts, end_ts, value_filter, ts_filter)
}

pub(crate) fn invalid_series_key_error<K: Display>(key: &K) -> ValkeyError {
    ValkeyError::String(format!(
        "VM: the key \"{}\" does not exist or is not a timeseries key",
        key
    ))
}

pub(crate) fn with_timeseries<R>(
    ctx: &Context,
    key: &ValkeyString,
    f: impl FnOnce(&TimeSeries) -> ValkeyResult<R>,
) -> ValkeyResult<R> {
    let redis_key = ctx.open_key(key);
    if let Some(series) = redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)? {
        f(series)
    } else {
        Err(invalid_series_key_error(key))
    }
}

pub(crate) fn with_timeseries_mut(
    ctx: &Context,
    key: &ValkeyString,
    f: impl FnOnce(&mut TimeSeries) -> ValkeyResult,
) -> ValkeyResult {
    // expect should not panic, since must_exist will cause an error if the key is non-existent, and `?` will ensure it propagates
    f(get_timeseries_mut(ctx, key, true)?.expect("key does not exist"))
}

pub(crate) fn get_timeseries_mut<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    must_exist: bool,
) -> ValkeyResult<Option<&'a mut TimeSeries>> {
    let redis_key = ctx.open_key_writable(key);
    // Safety: According to docs for `get_value`, it Will panic if RedisModule_ModuleTypeGetValue is missing in redismodule. h
    // it that happens we have a bigger problem than a panic since we're compiling against an incompatible version of valkey.
    let series = redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)?;
    match series {
        Some(series) => Ok(Some(series)),
        None => {
            let msg = format!("VM: the key \"{}\" is not a timeseries", key);
            if must_exist {
                Err(ValkeyError::String(msg))
            } else {
                Ok(None)
            }
        }
    }
}
