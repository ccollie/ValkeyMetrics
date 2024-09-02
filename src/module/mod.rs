use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};
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
mod range_utils;
mod function_stats;

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
    pub(crate) use super::function_stats::*;
}

pub(crate) fn with_timeseries(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&TimeSeries) -> ValkeyResult) -> ValkeyResult {
    let redis_key = ctx.open_key(key);
    let series = redis_key.get_value::<TimeSeries>(&VALKEY_PROMQL_SERIES_TYPE)?;
    match series {
        Some(series) => f(series),
        None => Err(ValkeyError::Str("ERR TSDB: the key is not a timeseries")),
    }
}

pub(crate) fn with_timeseries_mut(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&mut TimeSeries) -> ValkeyResult) -> ValkeyResult {
    let redis_key = ctx.open_key_writable(key);
    let series = redis_key.get_value::<TimeSeries>(&VALKEY_PROMQL_SERIES_TYPE)?;
    match series {
        Some(series) => f(series),
        None => Err(ValkeyError::Str("ERR TSDB: the key is not a timeseries")),
    }
}
