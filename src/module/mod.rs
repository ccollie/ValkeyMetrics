use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};
pub(crate) use ts_db::*;
pub(crate) use utils::*;
use crate::storage::time_series::TimeSeries;

mod timeseries_api;
mod result;
mod utils;
mod ts_db;
pub mod arg_parse;
pub(crate) mod commands;

pub(crate) fn with_timeseries(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&TimeSeries) -> ValkeyResult) -> ValkeyResult {
    let redis_key = ctx.open_key(key);
    let series = redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)?;
    match series {
        Some(series) => f(series),
        None => Err(ValkeyError::Str("ERR TSDB: the key is not a timeseries")),
    }
}

pub(crate) fn with_timeseries_mut(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&mut TimeSeries) -> ValkeyResult) -> ValkeyResult {
    let redis_key = ctx.open_key_writable(key);
    let series = redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)?;
    match series {
        Some(series) => f(series),
        None => Err(ValkeyError::Str("ERR TSDB: the key is not a timeseries")),
    }
}
