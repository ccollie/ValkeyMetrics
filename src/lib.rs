use redis_module::{NotifyEvent, redis_module, Context as RedisContext};
use redis_module_macros::{config_changed_event_handler};

extern crate redis_module_macros;
mod common;
mod error;
mod module;
mod index;
mod provider;
mod globals;
mod config;
mod ts;
mod rules;
mod executor;

use module::*;
use crate::globals::get_timeseries_index;

pub const REDIS_PROMQL_VERSION: i32 = 1;
pub const MODULE_NAME: &str = "redis_promql";
pub const MODULE_TYPE: &str = "RedisMetricsqlTimeseries";

#[config_changed_event_handler]
fn config_changed_event_handler(ctx: &RedisContext, changed_configs: &[&str]) {
    ctx.log_notice("config changed")
}

fn remove_key_from_cache(key: &[u8]) {
    let index = get_timeseries_index();
    let key = String::from_utf8_lossy(key).to_string();
    index.remove_series_by_key(&key);
}

fn on_event(_ctx: &RedisContext, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    match event {
        "del" | "set" | "expired" | "evict" | "evicted" | "expire" | "trimmed" => {
            remove_key_from_cache(key);
        }
        "rename_from" => {
            // RenameSeriesFrom(ctx, key);
        }
        "ts.alter" => {
            remove_key_from_cache(key)
        }
        _ => {
            // ctx.log_warning(&format!("Unknown event: {}", event));
        }
    }
}


redis_module! {
    name: MODULE_NAME,
    version: REDIS_PROMQL_VERSION,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [REDIS_PROMQL_SERIES_TYPE],
    commands: [
        ["PROM.CREATE-SERIES", commands::create, "write deny-oom", 1, 1, 1],
        ["PROM.ADD", commands::add, "write deny-oom", 1, 1, 1],
        ["PROM.MADD", commands::madd, "write deny-oom", 1, 1, 1],
        ["PROM.QUERY", commands::prom_query, "write deny-oom", 1, 1, 1],
        ["PROM.QUERY-RANGE", commands::prom_query_range, "write deny-oom", 1, 1, 1],
        ["PROM.RANGE", commands::range, "write deny-oom", 1, 1, 1],
        ["PROM.SERIES", commands::series, "write deny-oom", 1, 1, 1],
        ["PROM.LABEL_NAMES", commands::label_names, "write deny-oom", 1, 1, 1],
        ["PROM.LABEL_VALUES", commands::label_values, "write deny-oom", 1, 1, 1],
    ],
     event_handlers: [
        [@SET @STRING @GENERIC @EVICTED @EXPIRED @TRIMMED: on_event]
    ],
}