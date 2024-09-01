use valkey_module::RedisModuleTypeMethods;
use valkey_module::REDISMODULE_AUX_BEFORE_RDB;
use valkey_module::{native_types::ValkeyType, RedisModuleDefragCtx, RedisModuleString, ValkeyString};

use crate::globals::with_timeseries_index;
use crate::index::TimeSeriesIndex;
use crate::storage::defrag_series;
use crate::storage::time_series::TimeSeries;
use std::os::raw::{c_int, c_void};
use valkey_module::raw;
// see https://github.com/redis/redis/blob/unstable/tests/modules

pub static REDIS_PROMQL_SERIES_VERSION: i32 = 1;
pub static VALKEY_PROMQL_SERIES_TYPE: ValkeyType = ValkeyType::new(
    "vk_promTS",
    REDIS_PROMQL_SERIES_VERSION,
    RedisModuleTypeMethods {
        version: valkey_module::TYPE_METHOD_VERSION,
        rdb_load: Some(rdb_load),
        rdb_save: Some(rdb_save),
        aof_rewrite: None,
        free: Some(free),
        mem_usage: Some(mem_usage),
        digest: None,
        aux_load: None,
        aux_save: None,
        aux_save_triggers: REDISMODULE_AUX_BEFORE_RDB as i32,
        free_effort: None,
        unlink: Some(unlink),
        copy: Some(copy),
        defrag: Some(defrag),
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
        aux_save2: None,
    },
);

unsafe extern "C" fn rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let series = &*value.cast::<TimeSeries>();
    series.rdb_save(rdb);
}

unsafe extern "C" fn rdb_load(rdb: *mut raw::RedisModuleIO, encver: c_int) -> *mut c_void {
    TimeSeries::rdb_load(rdb, encver)
    // index.index_time_series(&new_series, &tmp);
}

unsafe extern "C" fn mem_usage(value: *const c_void) -> usize {
    let sm = unsafe { &*(value as *mut TimeSeries) };
    sm.memory_usage()
}

#[allow(unused)]
unsafe extern "C" fn free(value: *mut c_void) {
    if value.is_null() {
        return;
    }
    let sm = value as *mut TimeSeries;
    Box::from_raw(sm);
}

#[allow(non_snake_case, unused)]
unsafe extern "C" fn copy(
    fromkey: *mut RedisModuleString,
    tokey: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let guard = valkey_module::MODULE_CONTEXT.lock();
    with_timeseries_index(&guard, |index| {
        let sm = &*(value as *mut TimeSeries);
        let mut new_series = sm.clone();
        new_series.id = TimeSeriesIndex::next_id();
        // todo: simplify this
        let key = ValkeyString::from_redis_module_string(guard.ctx, tokey);
        let tmp = key.to_string();
        index.index_time_series(&new_series, &tmp);
        Box::into_raw(Box::new(new_series)).cast::<c_void>()
    })
}

unsafe extern "C" fn unlink(_key: *mut RedisModuleString, value: *const c_void) {
    let series = &*(value as *mut TimeSeries);
    if value.is_null() {
        return;
    }
    let guard = valkey_module::MODULE_CONTEXT.lock();
    with_timeseries_index(&guard, |ts_index| {
        ts_index.remove_series(series);
    });
}

unsafe extern "C" fn defrag(
    _ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> c_int {
    let series = &mut *(value as *mut TimeSeries);
    if value.is_null() {
        return 0;
    }
    match defrag_series(series) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}


pub(crate) fn new_from_valkey_string(c: ValkeyString) -> Result<TimeSeries, serde_json::Error> {
    // let mut val = bincode::deserialize::<TimeSeries>(&c.to_string().as_bytes());
    // serde_json::from_str(&c.to_string())
    todo!("bincode::deserialize::<TimeSeries>(&c.to_string().as_bytes())")
}
