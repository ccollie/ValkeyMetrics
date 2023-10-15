pub mod time_series;
mod types;
mod dedup;
mod utils;
mod constants;
pub mod chunks;
mod duplicate_policy;

pub(crate) use types::*;
pub use duplicate_policy::*;
use crate::module::REDIS_PROMQL_SERIES_TYPE;
use crate::ts::time_series::TimeSeries;

pub(crate) fn get_timeseries_mut<'a>(ctx: &'a Context, key: &RedisString, must_exist: bool) -> RedisResult<Option<&'a mut TimeSeries>> {
    let redis_key = ctx.open_key_writable(key.into());
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