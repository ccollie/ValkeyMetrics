use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};
use crate::globals::get_query_context;

pub fn reset_rollup_cache(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 1 {
        return Err(ValkeyError::WrongArity);
    }

    let context = get_query_context();
    if context.config.disable_cache {
        return Err(ValkeyError::Str("Cache is already disabled"));
    }
    context.rollup_result_cache.clear();

    // todo: report cache size ?
    VALKEY_OK
}