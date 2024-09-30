use crate::storage::time_series::TimeSeries;
pub(crate) use ts_db::*;
pub(crate) use utils::*;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

mod timeseries_api;
mod result;
mod utils;
mod ts_db;
pub mod arg_parse;
pub(crate) mod commands;
pub mod types;

pub(crate) fn with_timeseries(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&TimeSeries) -> ValkeyResult) -> ValkeyResult {
    let ts = get_timeseries(ctx, key)?;
    f(ts)
}

pub(crate) fn with_timeseries_mut(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&mut TimeSeries) -> ValkeyResult) -> ValkeyResult {
    f(get_timeseries_mut(ctx, key)?)
}

pub(crate) fn get_timeseries<'a>(ctx: &'a Context, key: &ValkeyString) -> ValkeyResult<&'a TimeSeries>  {
    let series = get_timeseries_mut(ctx, key)?;
    Ok(series)
}

pub(crate) fn get_timeseries_mut<'a>(ctx: &'a Context, key: &ValkeyString) -> ValkeyResult<&'a mut TimeSeries>  {
    let redis_key = ctx.open_key_writable(key);
    let series = redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)?;
    match series {
        Some(series) => Ok(series),
        None => {
            let msg = format!("ERR TSDB: the key \"{}\" is not a timeseries", key);
            Err(ValkeyError::String(msg))
        },
    }
}