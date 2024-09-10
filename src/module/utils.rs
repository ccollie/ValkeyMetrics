use std::borrow::Cow;
use std::ffi::CStr;
use valkey_module::{
    CallOptionResp,
    CallOptions,
    CallOptionsBuilder,
    CallResult,
    RedisModuleString,
    RedisModule_StringPtrLen,
    ValkeyError,
    ValkeyResult
};

use crate::common::current_time_millis;
use crate::common::types::Timestamp;
use crate::config::get_global_settings;
use crate::module::arg_parse::{parse_timestamp_range_value};
use crate::module::types::TimestampRangeValue;

#[no_mangle]
/// Perform a lossy conversion of a module string into a `Cow<str>`.
pub unsafe extern "C" fn string_from_module_string(
    s: *const RedisModuleString,
) -> Cow<'static, str> {
    let mut len = 0;
    let c_str = RedisModule_StringPtrLen.unwrap()(s, &mut len);
    CStr::from_ptr(c_str).to_string_lossy()
}

pub(crate) fn call_valkey_command<'a>(ctx: &valkey_module::Context, cmd: &'a str, args: &'a [String]) -> ValkeyResult {
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
        val.to_timestamp()
    } else {
        let ts = now - (config.default_step.as_millis() as i64); // todo: how to avoid overflow?
        ts as Timestamp
    };

    let end = if let Some(val) = end {
        val.to_timestamp()
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


/// Calculate the beginning of aggregation bucket
pub(crate) fn calc_bucket_start(ts: Timestamp, bucket_duration: i64, timestamp_alignment: i64) -> Timestamp {
    let timestamp_diff = ts - timestamp_alignment;
    ts - ((timestamp_diff % bucket_duration + bucket_duration) % bucket_duration)
}

// If bucket_ts is negative converts it to 0
pub fn normalize_bucket_start(bucket_ts: Timestamp) -> Timestamp {
    bucket_ts.max(0)
}
