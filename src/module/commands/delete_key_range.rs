use crate::module::arg_parse::parse_timestamp_range;
use crate::module::with_timeseries_mut;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

/// VM.DELETE-KEY-RANGE key fromTimestamp toTimestamp
///
/// Deletes the data points in the given range for the given key.
pub fn delete_key_range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let key = args.next_arg()?;
    with_timeseries_mut(ctx, &key, |series| {
        let date_range = parse_timestamp_range(&mut args)?;

        args.done()?;

        let (start, end) = date_range.get_series_range(series, false);

        let sample_count = series.remove_range(start, end)?;

        Ok(ValkeyValue::from(sample_count))
    })
}
