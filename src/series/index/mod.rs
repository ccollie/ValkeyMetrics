
mod timeseries_index;
#[cfg(test)]
mod index_tests;
mod filters;
mod index_key;
pub mod serialization;

use std::sync::LazyLock;
use papaya::{Guard, HashMap};
use valkey_module::{raw, Context, RedisModule_GetSelectedDb};
pub use timeseries_index::*;

/// Map from db to TimeseriesIndex
pub type TimeSeriesIndexMap = HashMap<u32, TimeSeriesIndex>;

pub(crate) static TIMESERIES_INDEX: LazyLock<TimeSeriesIndexMap> = LazyLock::new(TimeSeriesIndexMap::new);

// Safety: RedisModule_GetSelectedDb is safe to call
pub unsafe fn get_current_db(ctx: *mut raw::RedisModuleCtx) -> u32 {
    let db = RedisModule_GetSelectedDb.unwrap()(ctx);
    db as u32
}

#[inline]
pub fn get_timeseries_index_for_db(db: u32, guard: &impl Guard) -> &TimeSeriesIndex {
    TIMESERIES_INDEX.get_or_insert_with(db, TimeSeriesIndex::new, guard)
}

pub fn with_timeseries_index<F, R>(ctx: &Context, f: F) -> R
where
    F: FnOnce(&TimeSeriesIndex) -> R,
{
    let db = unsafe { get_current_db(ctx.ctx) };
    let guard = TIMESERIES_INDEX.guard();
    let index = get_timeseries_index_for_db(db, &guard);
    let res = f(index);
    drop(guard);
    res
}

// todo: move elsewhere
pub fn clear_timeseries_index(ctx: &Context) {
    let db = unsafe { get_current_db(ctx.ctx) };
    TIMESERIES_INDEX.pin().remove(&db);
}

pub fn clear_all_timeseries_index() {
    TIMESERIES_INDEX.pin().clear();
}

pub fn swap_timeseries_index_dbs(ctx: &Context, from_db: i32, to_db: i32) {
    if from_db > 0 && to_db > 0 {
        let from_db = from_db as u32;
        let to_db = to_db as u32;
        let map = TIMESERIES_INDEX.pin();
        let from = map.remove(&from_db);
        let to = map.remove(&from_db);

        // change this if https://github.com/ibraheemdev/papaya/issues/29 is resolved
        if let Some(to) = to {
            map.insert(from_db, to.clone());
        }
        if let Some(from) = from {
            map.insert(to_db, from.clone());
        }
    }
}

pub fn with_db_timeseries_index<F, R>(db: u32, f: F) -> R
where
    F: FnOnce(&TimeSeriesIndex) -> R,
{
    let guard = TIMESERIES_INDEX.guard();
    let index = get_timeseries_index_for_db(db, &guard);
    let res = f(index);
    drop(guard);
    res
}