extern crate async_trait;
extern crate cfg_if;
extern crate core;
extern crate get_size;
extern crate joinkit;
extern crate smallvec;
extern crate topologic;
extern crate valkey_module_macros;

use valkey_module::{logging, valkey_module, Context, Status, ValkeyString};

use valkey_module_macros::config_changed_event_handler;
mod aggregators;
mod alerts;
mod common;
mod config;
mod error;
mod error_consts;
mod iterators;
mod join;
mod module;
mod query;
mod series;
mod server_events;
#[cfg(test)]
mod tests;
use module::*;

use crate::alerts::VKM_RULE_GROUP;
use crate::common::async_runtime::init_runtime;
use crate::config::load_config;
use crate::series::{start_series_background_worker, stop_series_background_worker};
use crate::server_events::{generic_key_event_handler, register_server_events};

pub const VKMETRICS_VERSION: i32 = 1;
pub const MODULE_NAME: &str = "VKMetrics";
pub const MODULE_TYPE: &str = "vkmetrics";

fn initialize(ctx: &Context, args: &[ValkeyString]) -> Status {
    logging::log_debug("initialize");

    init_runtime();

    if load_config(ctx, args).is_err() {
        logging::log_warning("Failed to load configuration");
        return Status::Err;
    }

    start_series_background_worker();

    match register_server_events(ctx) {
        Ok(_) => Status::Ok,
        Err(e) => {
            let msg = format!("Failed to register server events: {}", e);
            logging::log_warning(msg);
            Status::Err
        }
    }
}

fn deinitialize(_ctx: &Context) -> Status {
    logging::log_notice("deinitialize");
    stop_series_background_worker();
    Status::Ok
}

#[config_changed_event_handler]
fn config_changed_event_handler(ctx: &Context, _changed_configs: &[&str]) {
    ctx.log_notice("config changed")
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

// https://github.com/redis/redis/blob/a38c29b6c861ee59637acdb1618f8f84645061d5/src/module.c
valkey_module! {
    name: MODULE_NAME,
    version: VKMETRICS_VERSION,
    allocator: (get_allocator!(), get_allocator!()),
    data_types: [VKM_SERIES_TYPE, VKM_RULE_GROUP],
    init: initialize,
    deinit: deinitialize,
    commands: [
        ["VM.CREATE-SERIES", commands::create, "write deny-oom", 1, 1, 1],
        ["VM.ALTER-SERIES", commands::alter_series, "write deny-oom", 1, 1, 1],
        ["VM.ADD", commands::add, "write fast deny-oom", 1, 1, 1],
        ["VM.GET", commands::get, "readonly fast", 1, 1, 1],
        ["VM.MGET", commands::mget, "readonly fast", 0, 0, -1],
        ["VM.COLLATE", commands::collate, "readonly", 0, 0, 0],
        ["VM.MADD", commands::madd, "write deny-oom", 1, -1, 3],
        ["VM.DELETE-KEY-RANGE", commands::delete_key_range, "write deny-oom", 1, 1, 1],
        ["VM.DELETE-RANGE", commands::delete_range, "write deny-oom", 0, 0, -1],
        ["VM.DELETE-SERIES", commands::delete_series, "write deny-oom", 1, 1, 1],
        ["VM.JOIN", commands::join, "readonly", 1, 2, 1],
        ["VM.QUERY", commands::query, "readonly deny-oom", 0, 0, 0],
        ["VM.QUERY-RANGE", commands::query_range, "readonly deny-oom", 0, 0, 0],
        ["VM.MRANGE", commands::mrange, "readonly deny-oom", 0, 0, -1],
        ["VM.RANGE", commands::range, "readonly deny-oom", 1, 1, 1],
        ["VM.SERIES", commands::series, "readonly fast", 0, 0, 0],
        ["VM.SERIES-INFO", commands::info, "readonly fast", 1, 1, 1],
        ["VM.TOP-QUERIES", commands::top_queries, "readonly fast", 0, 0, 0],
        ["VM.ACTIVE-QUERIES", commands::active_queries, "readonly fast", 0, 0, 0],
        ["VM.CARDINALITY", commands::cardinality, "readonly fast", 0, 0, -1],
        ["VM.LABEL-NAMES", commands::label_names, "readonly fast", 0, 0, 0],
        ["VM.LABEL-VALUES", commands::label_values, "readonly fast", 0, 0, 0],
        ["VM.STATS", commands::stats, "readonly", 0, 0, 0],
        ["VM.RESET-ROLLUP-CACHE", commands::reset_rollup_cache, "write deny-oom", 0, 0, 0],
    ],
     event_handlers: [
        [@SET @STRING @GENERIC @EVICTED @EXPIRED : generic_key_event_handler]
    ],
}

// todo: handle @TRIMMED
