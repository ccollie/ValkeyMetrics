use crate::alerts::group_data_type::VKM_RULE_GROUP;
use crate::alerts::rules::Group;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

pub(crate) fn with_group<T>(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&Group) -> ValkeyResult<T>) -> ValkeyResult<T> {
    let redis_key = ctx.open_key(key);
    let group = redis_key.get_value::<Group>(&VKM_RULE_GROUP)?;
    match group {
        Some(group) => f(group),
        None => Err(ValkeyError::Str("ERR TSDB: the key is not a group")),
    }
}

pub(crate) fn with_group_mut<T>(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&mut Group) -> ValkeyResult<T>) -> ValkeyResult<T> {
    let redis_key = ctx.open_key_writable(key);
    let group = redis_key.get_value::<Group>(&VKM_RULE_GROUP)?;
    match group {
        Some(group) => f(group),
        None => Err(ValkeyError::Str("ERR TSDB: the key is not a group")),
    }
}