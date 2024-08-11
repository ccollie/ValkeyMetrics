use redis_module::{Context, RedisError, RedisResult, RedisString};
pub(crate) use ts_db::*;
pub(crate) use utils::*;
use crate::storage::time_series::TimeSeries;

mod timeseries_api;
mod function_metadata;
mod result;
mod utils;
mod function_query;
mod ts_db;
mod function_create;
mod function_del;
mod function_range;
mod function_madd;
mod function_add;
mod function_alter;
mod function_get;
pub mod arg_parse;
mod aggregation;

pub mod commands {
    pub(crate) use super::function_add::*;
    pub(crate) use super::function_alter::*;
    pub(crate) use super::function_create::*;
    pub(crate) use super::function_del::*;
    pub(crate) use super::function_get::*;
    pub(crate) use super::function_madd::*;
    pub(crate) use super::function_metadata::*;
    pub(crate) use super::function_query::*;
    pub(crate) use super::function_range::*;
}

pub(crate) fn with_timeseries(ctx: &Context, key: &RedisString, f: impl FnOnce(&TimeSeries) -> RedisResult) -> RedisResult {
    let redis_key = ctx.open_key(key);
    let series = redis_key.get_value::<TimeSeries>(&REDIS_PROMQL_SERIES_TYPE)?;
    match series {
        Some(series) => f(series),
        None => Err(RedisError::Str("ERR TSDB: the key is not a timeseries")),
    }
}

pub(crate) fn with_timeseries_mut(ctx: &Context, key: &RedisString, f: impl FnOnce(&mut TimeSeries) -> RedisResult) -> RedisResult {
    let redis_key = ctx.open_key_writable(key);
    let series = redis_key.get_value::<TimeSeries>(&REDIS_PROMQL_SERIES_TYPE)?;
    match series {
        Some(series) => f(series),
        None => Err(RedisError::Str("ERR TSDB: the key is not a timeseries")),
    }
}

pub(crate) fn get_timeseries_mut<'a>(ctx: &'a Context, key: &RedisString, must_exist: bool) -> RedisResult<Option<&'a mut TimeSeries>> {
    let redis_key = ctx.open_key_writable(key);
    let result = redis_key.get_value::<TimeSeries>(&REDIS_PROMQL_SERIES_TYPE)?;
    if must_exist && result.is_none() {
        return Err(RedisError::Str("ERR TSDB: the key is not a timeseries"));
    }
    Ok(result)
}

/*pub(crate) fn get_timeseries<'a>(ctx: &'a Context, key: &RedisString, must_exist: bool) -> RedisResult<Option<&'a TimeSeries>> {
    let redis_key = ctx.open_key(key.into());
    let result = redis_key.get_value::<TimeSeries>(&REDIS_PROMQL_SERIES_TYPE)?;
    if must_exist && result.is_none() {
        return Err(RedisError::Str("ERR TSDB: the key is not a timeseries"));
    }
    Ok(result)
}

pub(crate) fn get_timeseries_multi<'a>(ctx: &'a Context, keys: &[&RedisString]) -> RedisResult<Vec<Option<&'a TimeSeries>>> {
    keys
        .iter()
        .map(|key| get_timeseries(ctx, key, false)).collect::<Result<Vec<_>, _>>()
}

*/
