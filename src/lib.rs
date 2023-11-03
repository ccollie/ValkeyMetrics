extern crate get_size;
extern crate valkey_module_macros;
extern crate tinyvec;
extern crate async_trait;

use valkey_module::{valkey_module, Context as ValkeyContext, NotifyEvent, ValkeyString};
use valkey_module_macros::config_changed_event_handler;

mod aggregators;
mod common;
mod config;
mod error;
mod globals;
mod index;
mod module;
mod provider;
mod storage;

#[cfg(test)]
mod tests;


use crate::globals::{with_timeseries_index};
use module::*;
pub const VALKEY_PROMQL_VERSION: i32 = 1;
pub const MODULE_NAME: &str = "valkey_promql";
pub const MODULE_TYPE: &str = "vktseries";

#[config_changed_event_handler]
fn config_changed_event_handler(ctx: &ValkeyContext, _changed_configs: &[&str]) {
    ctx.log_notice("config changed")
}

fn remove_key_from_cache(ctx: &ValkeyContext, key: &[u8]) {
    with_timeseries_index(ctx, |ts_index| {
        let key: ValkeyString = ctx.create_string(key);
        ts_index.remove_series_by_key(ctx, &key);
    });
}

fn on_event(ctx: &ValkeyContext, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    // todo: AddPostNotificationJob(ctx, event, key);
    match event {
        "del" | "set" | "expired" | "evict" | "evicted" | "expire" | "trimmed" => {
            remove_key_from_cache(ctx, key);
        }
        "rename_from" => {
            // RenameSeriesFrom(ctx, key);
        }
        "storage.alter" => remove_key_from_cache(ctx, key),
        _ => {
            // ctx.log_warning(&format!("Unknown event: {}", event));
        }
    }
}

#[cfg(not(test))]
macro_rules! get_allocator {
    () => {
        valkey_module::alloc::ValkeyAlloc
    };
}

#[cfg(test)]
macro_rules! get_allocator {
    () => {
        std::alloc::System
    };
}

valkey_module! {
    name: MODULE_NAME,
    version: VALKEY_PROMQL_VERSION,
    allocator: (get_allocator!(), get_allocator!()),
    data_types: [VALKEY_PROMQL_SERIES_TYPE],
    commands: [
        ["PROM.CREATE-SERIES", commands::create, "write deny-oom", 1, 1, 1],
        ["PROM.ALTER-SERIES", commands::alter, "write deny-oom", 1, 1, 1],
        ["PROM.ADD", commands::add, "write deny-oom", 1, 1, 1],
        ["PROM.GET", commands::get, "write deny-oom", 1, 1, 1],
        ["PROM.MADD", commands::madd, "write deny-oom", 1, 1, 1],
        ["PROM.DEL", commands::del_range, "write deny-oom", 1, 1, 1],
        ["PROM.QUERY", commands::prom_query, "write deny-oom", 1, 1, 1],
        ["PROM.QUERY-RANGE", commands::query_range, "write deny-oom", 1, 1, 1],
        ["PROM.RANGE", commands::range, "write deny-oom", 1, 1, 1],
        ["PROM.SERIES", commands::series, "write deny-oom", 1, 1, 1],
        ["PROM.CARDINALITY", commands::cardinality, "write deny-oom", 1, 1, 1],
        ["PROM.LABEL_NAMES", commands::label_names, "write deny-oom", 1, 1, 1],
        ["PROM.LABEL_VALUES", commands::label_values, "write deny-oom", 1, 1, 1],
    ],
     event_handlers: [
        [@SET @STRING @GENERIC @EVICTED @EXPIRED @TRIMMED: on_event]
    ],
}
