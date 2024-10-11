use std::borrow::Cow;
use std::ffi::CStr;
use std::fmt::Display;
use valkey_module::{CallOptionResp, CallOptions, CallOptionsBuilder, CallResult, Context, RedisModuleString, RedisModule_StringPtrLen, ValkeyError, ValkeyResult, ValkeyString};

use crate::common::current_time_millis;
use crate::common::types::{Matchers, Timestamp};
use crate::config::get_global_settings;
use crate::globals::with_timeseries_index;
use crate::module::arg_parse::parse_timestamp_range_value;
use crate::module::types::{TimestampRange, TimestampRangeValue, ValueFilter};
use crate::module::VKM_SERIES_TYPE;
use crate::series::time_series::{SeriesSampleIterator, TimeSeries};

#[no_mangle]
/// Perform a lossy conversion of a module string into a `Cow<str>`.
pub unsafe extern "C" fn string_from_module_string(
    s: *const RedisModuleString,
) -> Cow<'static, str> {
    let mut len = 0;
    let c_str = RedisModule_StringPtrLen.unwrap()(s, &mut len);
    CStr::from_ptr(c_str).to_string_lossy()
}

pub(crate) fn call_valkey_command<'a>(ctx: &Context, cmd: &'a str, args: &'a [String]) -> ValkeyResult {
    let call_options: CallOptions = CallOptionsBuilder::default()
        .resp(CallOptionResp::Resp3)
        .build();
    let args = args.iter().map(|x| x.as_bytes()).collect::<Vec<_>>();
    ctx.call_ext::<_, CallResult>(cmd, &call_options, args.as_slice())
        .map_or_else(|e| Err(e.into()), |v| Ok((&v).into()))
}

pub fn parse_timestamp_arg(
    arg: &str,
    name: &str,
) -> Result<TimestampRangeValue, ValkeyError> {
    parse_timestamp_range_value(arg).map_err(|_e| {
        let msg = format!("ERR invalid {} timestamp", name);
        ValkeyError::String(msg)
    })
}

pub(crate) fn normalize_range_args(
    start: Option<TimestampRangeValue>,
    end: Option<TimestampRangeValue>,
) -> ValkeyResult<(Timestamp, Timestamp)> {
    let config = get_global_settings();
    let now = current_time_millis();

    let start = if let Some(val) = start {
        val.as_timestamp()
    } else {
        let ts = now - (config.default_step.as_millis() as i64); // todo: how to avoid overflow?
        ts as Timestamp
    };

    let end = if let Some(val) = end {
        val.as_timestamp()
    } else {
        now
    };

    if start > end {
        return Err(ValkeyError::Str(
            "ERR end timestamp must not be before start time",
        ));
    }

    Ok((start, end))
}


pub fn get_series_iterator<'a>(series: &'a TimeSeries,
                               date_range: TimestampRange,
                               ts_filter: &'a Option<Vec<Timestamp>>,
                               value_filter: &'a Option<ValueFilter>) -> SeriesSampleIterator<'a> {
    let (start_ts, end_ts) = date_range.get_series_range(series, false);
    SeriesSampleIterator::new(series, start_ts, end_ts, value_filter, ts_filter)
}

pub(crate) fn invalid_series_key_error<K: Display>(key: &K) -> ValkeyError {
    ValkeyError::String(format!("VM: the key \"{}\" does not exist or is not a timeseries key", key))
}

pub(crate) fn with_timeseries(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&TimeSeries) -> ValkeyResult) -> ValkeyResult {
    let redis_key = ctx.open_key(key);
    if let Some(series) = redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)? {
        f(series)
    } else {
        Err(invalid_series_key_error(key))
    }
}

pub(crate) fn with_timeseries_mut(ctx: &Context, key: &ValkeyString, f: impl FnOnce(&mut TimeSeries) -> ValkeyResult) -> ValkeyResult {
    // unwrap is ok, since must_exist will cause an error if the key is non-existent, and `?` will ensure it propagates
    f(get_timeseries_mut(ctx, key, true)?.unwrap())
}

pub(crate) fn get_timeseries_mut<'a>(ctx: &'a Context, key: &ValkeyString, must_exist: bool) -> ValkeyResult<Option<&'a mut TimeSeries>>  {
    let redis_key = ctx.open_key_writable(key);
    let series = redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)?;
    match series {
        Some(series) => Ok(Some(series)),
        None => {
            let msg = format!("VM: the key \"{}\" is not a timeseries", key);
            if must_exist {
                Err(ValkeyError::String(msg))
            } else {
                Ok(None)
            }
        },
    }
}

pub(crate) fn with_matched_series<F, STATE>(ctx: &Context, acc: &mut STATE, matchers: &[Matchers], mut f: F) -> ValkeyResult<()>
where
    F: FnMut(&mut STATE, &TimeSeries, ValkeyString) -> ValkeyResult<()>,
{
    with_timeseries_index(ctx, move |index| {
        let keys = index.series_keys_by_matchers(ctx, matchers);
        if keys.is_empty() {
            return Err(ValkeyError::Str("ERR no series found"));
        }
        for key in keys {
            let db_key = ctx.open_key(&key);
            if let Some(series) = db_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)? {
                f(acc, series, key)?
            }
        }
        Ok(())
    })
}