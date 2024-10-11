use crate::module::with_timeseries_mut;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};
use valkey_module::ValkeyError::WrongArity;

pub fn get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    if let Ok(key) = args.next_arg() {
        with_timeseries_mut(ctx, &key, |series| {
            args.done()?;

            let result = if series.is_empty() {
                vec![]
            } else {
                vec![ValkeyValue::from(series.last_timestamp), ValkeyValue::from(series.last_value)]
            };

            Ok(ValkeyValue::Array(result))
        })
    } else {
        Err(WrongArity)
    }
}
