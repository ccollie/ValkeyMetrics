use crate::globals::with_timeseries_index;
use crate::module::arg_parse::{parse_series_selector, parse_timestamp_range};
use crate::module::VKM_SERIES_TYPE;
use crate::series::time_series::TimeSeries;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use crate::error_consts;
// todo: change cmd name to delete_series_mrange. we want another function to delete the series
// keys completely, not just the data points.

///
/// VM.DELETE-RANGE fromTimestamp toTimestamp <series selector>..
///
/// ref: https://prometheus.io/docs/prometheus/latest/querying/api/#delete-series
pub fn delete_range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let date_range = parse_timestamp_range(&mut args)?;

    let mut matchers = vec![];
    for arg in args.by_ref() {
        let selector = arg.try_as_str()?;
        let parsed_matcher = parse_series_selector(selector)?;
        matchers.push(parsed_matcher);
    }

    if matchers.is_empty() {
        return Err(ValkeyError::Str("ERR at least one series selector is required"));
    }

    let count = with_timeseries_index(ctx, move |ts_index| {
        let keys = ts_index.series_keys_by_matchers(ctx, &matchers);
        if keys.is_empty() {
            return Err(ValkeyError::Str(error_consts::NO_SERIES_FOUND));
        }
        let mut deleted: usize = 0;
        for key in keys {
            let db_key = ctx.open_key_writable(&key);
            // get series from redis
            if let Some(series) = db_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)? {
                let (start_ts, end_ts) = date_range.get_series_range(series, false);
                deleted += series.remove_range(start_ts, end_ts)?;
            }
        }
        Ok(deleted)
    })?;

    Ok(ValkeyValue::from(count))
}