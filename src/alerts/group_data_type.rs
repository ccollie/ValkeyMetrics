use crate::alerts::rules::Group;
use crate::alerts::serialization::{load_group, rdb_load_group_metadata, rdb_save_group_metadata, save_group};
use crate::alerts::meta::with_group_manager;
use crate::common::current_time_millis;
use std::ffi::c_int;
use std::os::raw::c_void;
use std::ptr::null_mut;
use valkey_module::native_types::ValkeyType;
use valkey_module::{
    raw,
    Context,
    RedisModuleDefragCtx,
    RedisModuleString,
    ValkeyString,
    REDISMODULE_AUX_AFTER_RDB
};

const VM_GROUP_VERSION: i32 = 1;

pub static VKM_RULE_GROUP: ValkeyType = ValkeyType::new(
    "vmalrtgrp",
        VM_GROUP_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(group_rdb_load),
        rdb_save: Some(group_rdb_save),
        aof_rewrite: None,
        free: Some(free),

        // Currently unused by Redis
        mem_usage: None,
        digest: None,

        // Aux data
        aux_load: Some(rdb_load_group_metadata),
        aux_save: Some(rdb_save_group_metadata),
        aux_save2: None,
        aux_save_triggers: REDISMODULE_AUX_AFTER_RDB as i32,

        free_effort: None,
        unlink: Some(unlink),
        copy: Some(copy),
        defrag: Some(defrag),

        copy2: None,
        free_effort2: None,
        mem_usage2: None,
        unlink2: None,
    },
);

/// # Safety
pub unsafe extern "C" fn group_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let v = &*value.cast::<Group>();
    save_group(rdb, v);
}

/// # Safety
pub unsafe extern "C" fn group_rdb_load(
    rdb: *mut raw::RedisModuleIO,
    enc_ver: c_int,
) -> *mut c_void {
    if let Ok(group) = load_group(rdb, enc_ver) {
        let bb = Box::new(group);
        Box::into_raw(bb).cast::<c_void>()
    } else {
        null_mut()
    }
}

#[allow(non_snake_case, unused)]
unsafe extern "C" fn copy(
    from_key: *mut RedisModuleString,
    to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let guard = valkey_module::MODULE_CONTEXT.lock();
    let group = &*(value as *mut Group);
    let mut new_group = group.clone();
    // todo: new id.
    let key = ValkeyString::from_redis_module_string(guard.ctx, to_key);
    // TODO: schedule group or set to disabled
    Box::into_raw(Box::new(new_group)).cast::<c_void>()
}

fn remove_group_from_manager(group: &Group) {
    let guard = valkey_module::MODULE_CONTEXT.lock();
    let ctx = Context { ctx: guard.ctx };
    with_group_manager(&ctx, |manager| manager.delete_group(&ctx, group))
}

#[allow(unused)]
unsafe extern "C" fn free(value: *mut c_void) {
    if value.is_null() {
        return;
    }
    let sm = value as *mut Group;
    {
        let group = &*(sm);
        remove_group_from_manager(group);
    }
    Box::from_raw(sm);
}


unsafe extern "C" fn unlink(_key: *mut RedisModuleString, value: *const c_void) {
    let group = &*(value as *mut Group);
    remove_group_from_manager(group);
}

// todo: defrag - remove stale series
unsafe extern "C" fn defrag(
    _ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> c_int {
    let group = &mut *(value as *mut Group);
    let now = current_time_millis();
    
    group.remove_inactive_alerts(now);

    0
}