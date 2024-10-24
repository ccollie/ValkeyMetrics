use crate::series::index::{TimeSeriesIndex, TimeSeriesIndexMap};

use papaya::Guard;
use std::sync::LazyLock;
use valkey_module::{raw, Context, RedisModule_GetSelectedDb};

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
pub fn clear_timeseries_index() {
    let guard = TIMESERIES_INDEX.guard();
    TIMESERIES_INDEX.clear(&guard);
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