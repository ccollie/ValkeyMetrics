use crate::globals::with_timeseries_index;
use crate::module::arg_parse::parse_series_selector;
use crate::module::VKM_SERIES_TYPE;
use crate::storage::time_series::TimeSeries;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

///
/// VM.DELETE-SERIES selector..
///
/// Deletes the valkey keys for the given series selectors.
pub fn delete_series(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);

    let mut matchers = Vec::with_capacity(args.len() - 1);

    while let Ok(selector) = args.next_str() {
        let parsed_matcher = parse_series_selector(selector)?;
        matchers.push(parsed_matcher);
    }

    // todo: with_matched_series
    let res = with_timeseries_index(ctx, move |index| {
        let keys = index.series_keys_by_matchers(ctx, &matchers);
        if keys.is_empty() {
            return Err(ValkeyError::Str("ERR no series found"));
        }
        let mut deleted: usize = 0;
        for key in keys {
            let redis_key = ctx.open_key_writable(&key);
            // get series from redis
            match redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE) {
                Ok(Some(_)) => {
                    // todo: remove from index
                    redis_key.delete()?;
                }
                Ok(None) => {
                    let msg = format!("ERR series not found '{}'", key);
                    return Err(ValkeyError::String(msg));
                }
                Err(e) => {
                    return Err(e)
                }
            }
            deleted += 1;
        }
        Ok(deleted)
    });

    ctx.replicate_verbatim();

    match res {
        Ok(deleted) => Ok(ValkeyValue::from(deleted)),
        Err(e) => Err(e),
    }
}