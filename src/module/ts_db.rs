use redis_module::RedisModuleTypeMethods;
use redis_module::REDISMODULE_AUX_BEFORE_RDB;
use redis_module::{native_types::RedisType, RedisModuleDefragCtx, RedisModuleString, RedisString};

use crate::globals::get_timeseries_index;
use crate::storage::time_series::TimeSeries;
use redis_module::raw;
use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;
use crate::storage::defrag_series;

// see https://github.com/redis/redis/blob/unstable/tests/modules

pub static REDIS_PROMQL_SERIES_VERSION: i32 = 1;
pub static REDIS_PROMQL_SERIES_TYPE: RedisType = RedisType::new(
    "RedPromTS",
    REDIS_PROMQL_SERIES_VERSION,
    RedisModuleTypeMethods {
        version: redis_module::TYPE_METHOD_VERSION,
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
    let v = &*value.cast::<TimeSeries>();
    // todo: bincode/postcard
    raw::save_string(rdb, &serde_json::to_string(&v).unwrap());
}

unsafe extern "C" fn rdb_load(rdb: *mut raw::RedisModuleIO, _encver: c_int) -> *mut c_void {
    let v = raw::load_string(rdb);
    if v.is_err() {
        return null_mut();
    }
    let f = v.unwrap();
    let sm = new_from_redis_string(f);
    if sm.is_err() {
        return null_mut();
    }
    let ff = sm.unwrap();
    let bb = Box::new(ff);
    let rawbox = Box::into_raw(bb);
    rawbox as *mut c_void
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
    fromkey: *mut raw::RedisModuleString,
    tokey: *mut raw::RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let sm = &*(value as *mut TimeSeries);
    let new_series = sm.clone();
    let mut ts_index = get_timeseries_index();
    ts_index.reindex_timeseries(&new_series);
    Box::into_raw(Box::new(new_series)).cast::<c_void>()
}

unsafe extern "C" fn unlink(_key: *mut raw::RedisModuleString, value: *const c_void) {
    let series = &*(value as *mut TimeSeries);
    if value.is_null() {
        return;
    }
    let mut ts_index = get_timeseries_index();
    ts_index.reindex_timeseries(series)
}

unsafe extern "C" fn defrag(
    _ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> c_int {
    let mut series = &*(value as *mut TimeSeries);
    if value.is_null() {
        return 0;
    }
    match defrag_series(&mut series) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}


pub(crate) fn new_from_redis_string(c: RedisString) -> Result<TimeSeries, serde_json::Error> {
    // let mut val = bincode::deserialize::<TimeSeries>(&c.to_string().as_bytes());
    serde_json::from_str(&c.to_string())
}
