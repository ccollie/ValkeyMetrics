use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use metricsql_engine::prelude::{Context as QueryContext};
use rayon::ThreadPool;
use redis_module::RedisGILGuard;
use crate::index::TimeSeriesIndex;
use crate::provider::TsdbDataProvider;

static TIMESERIES_INDEX: OnceLock<TimeSeriesIndex> = OnceLock::new();
static QUERY_CONTEXT: OnceLock<QueryContext> = OnceLock::new();

const DEFAULT_EXECUTION_THREADS: usize = 4;

pub struct GlobalCtx {
    pool: Mutex<Option<ThreadPool>>,
    /// Thread pool which used to run management tasks that should not be
    /// starved by user tasks (which run on [`GlobalCtx::pool`]).
    management_pool: RedisGILGuard<Option<ThreadPool>>,
}

static mut GLOBALS: Option<GlobalCtx> = None;

fn get_globals() -> &'static GlobalCtx {
    unsafe { GLOBALS.as_ref().unwrap() }
}

fn get_globals_mut() -> &'static mut GlobalCtx {
    unsafe { GLOBALS.as_mut().unwrap() }
}

pub(crate) fn get_thread_pool() -> &'static mut ThreadPool {
    let mut pool = get_globals().pool.lock().unwrap();
    pool.get_or_insert_with(|| {
        // todo: get thread count from env
        rayon::ThreadPoolBuilder::new()
            .num_threads(DEFAULT_EXECUTION_THREADS)
            .thread_name(|idx| format!("promql-pool-{}", idx))
            .build()
            .unwrap()
    })
}


/// Executes the passed job object in a dedicated thread allocated
/// from the global module thread pool.
pub(crate) fn execute_on_pool<F: FnOnce() + Send + 'static>(job: F) {
    get_thread_pool()
        .execute(move || {
            job();
        });
}


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
            // how to do this in redis context ?
            panic!("set query context failed");
        }
    }
}


pub fn get_timeseries_index() -> &'static TimeSeriesIndex {
    TIMESERIES_INDEX.get_or_init(|| TimeSeriesIndex::new())
}