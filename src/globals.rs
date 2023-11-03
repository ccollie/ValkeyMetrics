use crate::index::{TimeSeriesIndex, TimeSeriesIndexMap};
use crate::provider::TsdbDataProvider;
use metricsql_runtime::prelude::Context as QueryContext;
use papaya::Guard;
use valkey_module::{raw, Context, RedisModule_GetSelectedDb};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock};

pub(crate) static TIMESERIES_INDEX: LazyLock<TimeSeriesIndexMap> = LazyLock::new(|| TimeSeriesIndexMap::new());
static QUERY_CONTEXT: LazyLock<QueryContext> = LazyLock::new(create_query_context);

pub fn get_query_context() -> &'static QueryContext {
    &QUERY_CONTEXT
}

fn create_query_context() -> QueryContext {
    // todo: read from config
    let provider = Arc::new(TsdbDataProvider{});
    let ctx = QueryContext::new();
    ctx.with_metric_storage(provider)
}

pub unsafe fn get_current_db(ctx: *mut raw::RedisModuleCtx) -> u32 {
    let db = RedisModule_GetSelectedDb.unwrap()(ctx);
    db as u32
}

// todo: in on_load, we need to set this to the last id + 1
static TIMESERIES_ID_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub fn next_timeseries_id() -> u64 {
    TIMESERIES_ID_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

/// https://docs.rs/papaya/latest/papaya/#advanced-lifetimes
fn get_timeseries_index<'guard>(ctx: &Context, guard: &'guard impl Guard) -> &'guard TimeSeriesIndex {
    let db = unsafe { get_current_db(ctx.ctx) };
    get_timeseries_index_for_db(db, guard)
}

#[inline]
pub fn get_timeseries_index_for_db(db: u32, guard: &impl Guard) -> &TimeSeriesIndex {
    TIMESERIES_INDEX.get_or_insert_with(db, || TimeSeriesIndex::new(), guard)
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