use crate::alerts::{
    VKM_RULE_GROUP
};
use crate::alerts::meta::{
    clear_all_group_managers,
    clear_group_manager,
    swap_group_manager_dbs,
    with_group_manager,
};
use crate::alerts::rules::Group;
use crate::module::with_timeseries;
use crate::alerts::serialization::alerts_on_async_load_done;
use crate::series::index::serialization::series_on_async_load_done;
use crate::series::index::*;
use std::os::raw::c_void;
use std::sync::atomic::AtomicBool;
use valkey_module::{logging, raw, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString};

static mut RENAME_FROM_KEY : Option<Vec<u8>> = None;
static ASYNC_LOADING_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

pub(crate) fn is_async_loading_in_progress() -> bool {
    ASYNC_LOADING_IN_PROGRESS.load(std::sync::atomic::Ordering::Relaxed)
}

fn handle_key_restore(ctx: &Context, key: &[u8]) {
    let _key = ctx.create_string(key);
    let is_ts = with_timeseries(ctx, &_key, |series| {
        with_timeseries_index(ctx, |index| {
            index.reindex_timeseries(series, key);
            Ok(true)
        })
    }).is_ok();

    if !is_ts {
        if let Ok(Some(group)) = ctx.open_key_writable(&_key).get_value::<Group>(&VKM_RULE_GROUP) {
            let _ = with_group_manager(ctx, |manager| manager.add_group(ctx, group, &_key));
        }
    }
}

fn handle_key_rename(ctx: &Context, old_key: &[u8], new_key: &[u8]) {
    let is_ts = with_timeseries_index(ctx, |index| {
        index.rename_series(ctx, old_key, new_key)
    });
    if !is_ts {
        with_group_manager(ctx, |manager| {
            manager.rename_group(old_key, new_key)
        });
    }
}

fn remove_key_from_index(ctx: &Context, key: &[u8]) {
    let is_ts = with_timeseries_index(ctx, |ts_index| {
        let key: ValkeyString = ctx.create_string(key);
        ts_index.remove_series_by_key(ctx, &key)
    });
    if !is_ts {
        // see if it's a group
        with_group_manager(ctx, |manager| {
            manager.delete_group_by_key(ctx, key)
        });
    }
}

pub(crate) fn generic_key_event_handler(ctx: &Context, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    // todo: AddPostNotificationJob(ctx, event, key);
    match event {
        "del" | "set" | "expired" | "evict" | "evicted" | "expire" | "trimmed" => {
            remove_key_from_index(ctx, key);
        }
        // SAFETY: This is safe because the key is only used in the closure and this function
        // is not called concurrently
        "rename_from" => unsafe {
            RENAME_FROM_KEY.replace(key.to_vec());
        }
        "rename_to" => unsafe {
            if let Some(old_key) = RENAME_FROM_KEY.take() {
                handle_key_rename(ctx, &old_key, key);
            }
        }
        "restore" => {
            handle_key_restore(ctx, key);
        }
        _ => {}
    }
}

unsafe extern "C" fn on_flush_event(
    ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut c_void
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
    _ctx: *mut raw::RedisModuleCtx,
    eid: raw::RedisModuleEvent,
    _sub_event: u64,
    data: *mut c_void) {
    if eid.id == raw::REDISMODULE_EVENT_SWAPDB {
        let ei: &raw::RedisModuleSwapDbInfo =
            unsafe { &*(data as *mut raw::RedisModuleSwapDbInfo) };
        
        let from_db = ei.dbnum_first;
        let to_db = ei.dbnum_second;

        swap_timeseries_index_dbs(from_db, to_db);
        swap_group_manager_dbs(from_db, to_db);
    }
}

fn on_async_load_done(completed: bool) {
    ASYNC_LOADING_IN_PROGRESS.store(false, std::sync::atomic::Ordering::Relaxed);
    alerts_on_async_load_done(completed);
    series_on_async_load_done(completed);
}

unsafe extern "C" fn on_async_load_event(
    _ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    _data: *mut c_void) {
    match sub_event {
        raw::REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_STARTED => {
            logging::log_notice("Async RDB loading started");
            ASYNC_LOADING_IN_PROGRESS.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        raw::REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_ABORTED => {
            logging::log_notice("Async AOF loading aborted");
            on_async_load_done(false);
        }
        raw::REDISMODULE_SUBEVENT_REPL_ASYNC_LOAD_COMPLETED => {
            logging::log_notice("Async loading completed");
            on_async_load_done(true);
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