use std::sync::{Arc, OnceLock};
use ahash::HashMapExt;
use metricsql_common::hash::IntMap;
use metricsql_runtime::prelude::{Context as QueryContext};
use crate::index::{TimeSeriesIndex};
use crate::provider::TsdbDataProvider;
use valkey_module::{raw, Context};

pub type TimeSeriesIndexMap = IntMap<u32, TimeSeriesIndex>;

static TIMESERIES_INDEX: OnceLock<TimeSeriesIndexMap> = OnceLock::new();
static QUERY_CONTEXT: OnceLock<QueryContext> = OnceLock::new();

pub fn get_query_context() -> &'static QueryContext {
    QUERY_CONTEXT.get_or_init(create_query_context)
}

fn create_query_context() -> QueryContext {
    // todo: read from config
    let provider = Arc::new(TsdbDataProvider{});
    let ctx = QueryContext::new();
    ctx.with_provider(provider)
}

pub(crate) fn set_query_context(ctx: QueryContext) {
    match QUERY_CONTEXT.set(ctx) {
        Ok(_) => {}
        Err(_) => {
            // how to do this in Valkey context ?
            panic!("set query context failed");
        }
    }
}

pub unsafe fn get_current_db(ctx: *mut raw::ValkeyModuleCtx) -> u32 {
    let db = ValkeyModule_GetSelectedDb.unwrap()(ctx);
    db as u32
}

pub fn get_timeseries_index(ctx: &Context) -> &'static mut TimeSeriesIndex {
    let mut map = TIMESERIES_INDEX.get_or_init(|| TimeSeriesIndexMap::new());
    let db = unsafe { get_current_db(ctx.ctx) };
    map.entry(db).or_insert_with(TimeSeriesIndex::new)
}