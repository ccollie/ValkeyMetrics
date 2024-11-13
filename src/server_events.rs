use std::sync::atomic::AtomicBool;
use valkey_module::{logging, raw, Context, ValkeyError, ValkeyResult, ValkeyString};
use crate::alerts::{clear_all_group_managers, clear_group_manager, swap_group_manager_dbs, with_group_manager};
use crate::series::index::{clear_all_timeseries_index, clear_timeseries_index, swap_timeseries_index_dbs, with_timeseries_index};

static ASYNC_LOADING_IN_PROGRESS: AtomicBool = AtomicBool::new(false);
static ASYNC_LOADING_ABORTED: AtomicBool = AtomicBool::new(false);

pub(crate) fn is_async_loading_in_progress() -> bool {
    ASYNC_LOADING_IN_PROGRESS.load(std::sync::atomic::Ordering::Relaxed)
}
pub(crate) fn is_async_loading_aborted() {
    ASYNC_LOADING_ABORTED.load(std::sync::atomic::Ordering::Relaxed);
}

fn remove_key_from_index(ctx: &Context, key: &[u8]) {
    let key: ValkeyString = ctx.create_string(key);
    // todo: rewrite this to account for groups
    let is_ts = with_timeseries_index(ctx, |ts_index| {
        ts_index.remove_series_by_key(ctx, &key)
    });
    if !is_ts {
        // see if it's a group
        with_group_manager(ctx, |manager| {
            manager.remove_group_by_key(ctx, &key)
        });
        with_timeseries_index(ctx, |ts_index| {
            let key: ValkeyString = ctx.create_string(key);
            ts_index.remove_group_by_key(ctx, &key)
        });

    }
}

unsafe extern "C" fn on_flush_event(
    ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut std::os::raw::c_void
) {
    if sub_event == raw::REDISMODULE_SUBEVENT_FLUSHDB_END {
        let ctx = Context::new(ctx);
        let fi: &raw::RedisModuleFlushInfo =
            unsafe { &*(data as *mut raw::RedisModuleFlushInfo) };

        if fi.dbnum == -1 {
            clear_all_timeseries_index();
            clear_all_group_managers();
        } else {
            clear_group_manager(&ctx);
            clear_timeseries_index(&ctx);
        }
    };
}


unsafe extern "C" fn on_swap_db_event(
    ctx: *mut raw::RedisModuleCtx,
    eid: raw::RedisModuleEvent,
    _sub_event: u64,
    data: *mut ::std::os::raw::c_void) {
    if eid.id == raw::REDISMODULE_EVENT_SWAPDB {
        let ei: &raw::RedisModuleSwapDbInfo =
            unsafe { &*(data as *mut raw::RedisModuleSwapDbInfo) };

        let ctx = Context::new(ctx);

        let from_db = ei.dbnum_first;
        let to_db = ei.dbnum_second;

        swap_timeseries_index_dbs(&ctx, from_db, to_db);
        swap_group_manager_dbs(&ctx, from_db, to_db);
    }
}

unsafe extern "C" fn on_async_load_event(
    _ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    _data: *mut std::os::raw::c_void) {
    match sub_event {
        raw::REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_STARTED => {
            logging::log_notice("Async RDB loading started");
            ASYNC_LOADING_ABORTED.store(false, std::sync::atomic::Ordering::Relaxed);
            ASYNC_LOADING_IN_PROGRESS.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        raw::REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_ABORTED => {
            logging::log_notice("Async AOF loading started");
            ASYNC_LOADING_ABORTED.store(true, std::sync::atomic::Ordering::Relaxed);
            ASYNC_LOADING_IN_PROGRESS.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        raw::REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_COMPLETED => {
            logging::log_notice("Async loading completed");
            ASYNC_LOADING_ABORTED.store(false, std::sync::atomic::Ordering::Relaxed);
            ASYNC_LOADING_IN_PROGRESS.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        _ => {
            logging::log_warning("Unknown async loading sub-event");
        }
    }
}


fn register_server_event_handler(
    ctx: &Context,
    server_event: u64,
    inner_callback: raw::RedisModuleEventCallback,
) -> Result<(), ValkeyError> {
    let res = unsafe {
        raw::RedisModule_SubscribeToServerEvent.unwrap()(
            ctx.ctx,
            raw::RedisModuleEvent {
                id: server_event,
                dataver: 1,
            },
            inner_callback,
        )
    };
    if res != raw::REDISMODULE_OK as i32 {
        return Err(ValkeyError::Str("Failed subscribing to server event"));
    }

    Ok(())
}

pub(super) fn register_server_events(ctx: &Context) -> ValkeyResult<()> {
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_FLUSHDB, Some(on_flush_event))?;
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_SWAPDB, Some(on_swap_db_event))?;
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_REPL_ASYNC_LOAD, Some(on_async_load_event))?;

    Ok(())
}