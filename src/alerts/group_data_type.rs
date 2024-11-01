use crate::alerts::rule::Group;
use crate::module::ValkeyDataType;
use std::ffi::c_int;
use std::os::raw::c_void;
use std::ptr::null_mut;
use std::sync::LazyLock;
use valkey_module::native_types::ValkeyType;
use valkey_module::{raw, RedisModuleString, ValkeyString};

const VM_GROUP_VERSION: i32 = 0;

pub static VKM_RULE_GROUP: ValkeyType = ValkeyType::new(
    "vmalrtgrp",
        VM_GROUP_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: None,
        rdb_save: None,
        aof_rewrite: None,
        free: Some(free),

        // Currently unused by Redis
        mem_usage: None,
        digest: None,

        // Aux data
        aux_load: None,
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: 0,

        free_effort: None,
        unlink: Some(unlink),
        copy: Some(copy),
        defrag: None,

        copy2: None,
        free_effort2: None,
        mem_usage2: None,
        unlink2: None,
    },
);

/// Stores all group keys during initialization. We need these keys for later use in the group
/// manager and dispatcher.
pub static GROUP_KEYS: LazyLock<Vec<Box<[u8]>>> = LazyLock::new(|| vec![]);


/// # Safety
pub unsafe extern "C" fn group_rdb_save(_rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let v = &*value.cast::<Group>();
    // todo: !!!!
}

/// # Safety
pub unsafe extern "C" fn group_rdb_load(
    rdb: *mut raw::RedisModuleIO,
    encver: c_int,
) -> *mut c_void {
    if let Some(item) = <Group as ValkeyDataType<Group>>::load_from_rdb(rdb, encver) {
        let bb = Box::new(item);
        Box::into_raw(bb).cast::<c_void>()
    } else {
        null_mut()
    }
}

#[allow(non_snake_case, unused)]
unsafe extern "C" fn copy(
    fromkey: *mut RedisModuleString,
    tokey: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let guard = valkey_module::MODULE_CONTEXT.lock();
    let group = &*(value as *mut Group);
    let mut new_group = group.clone();
    let key = ValkeyString::from_redis_module_string(guard.ctx, tokey);
    Box::into_raw(Box::new(new_group)).cast::<c_void>()
}

#[allow(unused)]
unsafe extern "C" fn free(value: *mut c_void) {
    if value.is_null() {
        return;
    }
    let sm = value as *mut Group;
    Box::from_raw(sm);
}


unsafe extern "C" fn unlink(_key: *mut RedisModuleString, value: *const c_void) {
    let series = &*(value as *mut Group);
    if value.is_null() {
        return;
    }
    let guard = valkey_module::MODULE_CONTEXT.lock();
    todo!("unlink. TODO: remove from group_manager")
}

// todo: defrag - remove stale series