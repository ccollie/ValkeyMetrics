use crate::globals::with_timeseries_index;
use crate::module::arg_parse::{parse_series_selector, TimestampRangeValue};
use crate::module::{normalize_range_args, parse_timestamp_arg, VKM_SERIES_TYPE};
use crate::storage::time_series::TimeSeries;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const CMD_ARG_START: &str = "START";
const CMD_ARG_END: &str = "END";

// todo: change cmd name to delete_series_mrange. we want another function to delete the series
// keys completely, not just the data points.

///
/// VKM.DELETE-RANGE <series selector>..
///     [START rfc3339 | unix_timestamp | + | - | * ]
///     [END rfc3339 | unix_timestamp | + | - | * ]
///
/// ref: https://prometheus.io/docs/prometheus/latest/querying/api/#delete-series
pub fn delete_range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    let mut start_value: Option<TimestampRangeValue> = None;
    let mut end_value: Option<TimestampRangeValue> = None;
    let mut end_matchers = false;
    let mut selectors = vec![args.next_str()?];

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_START) => {
                let next = args.next_str()?;
                start_value = Some(parse_timestamp_arg(next, "START")?);
                end_matchers = true;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_END) => {
                let next = args.next_str()?;
                end_value = Some(parse_timestamp_arg(next, "END")?);
                end_matchers = true;
            }
            _ => {
                if !end_matchers {
                    selectors.push(args.next_str()?);
                } else {
                    let msg = format!("ERR invalid argument '{}'", arg);
                    return Err(ValkeyError::String(msg));
                }
            }
        };
    }

    let (start_ts, end_ts) = normalize_range_args(start_value, end_value)?;

    let mut matchers = Vec::with_capacity(selectors.len());
    for selector in selectors {
        let parsed_matcher = parse_series_selector(selector)?;
        matchers.push(parsed_matcher);
    }

    let res = with_timeseries_index(ctx, move |ts_index| {
        let keys = ts_index.series_keys_by_matchers(ctx, &matchers);
        if keys.is_empty() {
            return Err(ValkeyError::Str("ERR no series found"));
        }
        let mut deleted: usize = 0;
        for key in keys {
            let redis_key = ctx.open_key_writable(&key);
            // get series from redis
            match redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE) {
                Ok(Some(series)) => {
                    deleted += series.remove_range(start_ts, end_ts)?;
                }
                Err(e) => {
                    return Err(e);
                }
                _ => {}
            }
        }
        Ok(deleted)
    });

    match res {
        Ok(deleted) => Ok(ValkeyValue::from(deleted)),
        Err(e) => Err(e),
    }
}