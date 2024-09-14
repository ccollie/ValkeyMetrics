use crate::module::VALKEY_PROMQL_SERIES_TYPE;
use crate::rules::Group;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

pub(crate) fn with_group_mut<T>(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&mut Group) -> ValkeyResult<T>) -> ValkeyResult<T> {
    let redis_key = ctx.open_key_writable(key);
    let group = redis_key.get_value::<Group>(&VALKEY_PROMQL_SERIES_TYPE)?;
    match group {
        Some(group) => f(group),
        None => Err(ValkeyError::Str("ERR TSDB: the key is not a group")),
    }
}
