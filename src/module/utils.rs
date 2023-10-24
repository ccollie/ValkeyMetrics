use std::borrow::Cow;
use redis_module::{CallOptionResp, CallOptions, CallOptionsBuilder, CallResult, RedisError, RedisResult, RedisValue};
use crate::common::{current_time_millis, parse_timestamp_range_value};
use crate::common::types::{Timestamp, TimestampRangeValue};
use crate::config::get_global_settings;
pub(crate) fn redis_value_as_str(value: &RedisValue) -> RedisResult<Cow<str>> {
    match value {
        RedisValue::SimpleStringStatic(s) => Ok(Cow::Borrowed(s)),
        RedisValue::SimpleString(s) => Ok(Cow::Borrowed(&s.as_str())),
        RedisValue::BulkString(s) => Ok(Cow::Borrowed(&s.as_str())),
        RedisValue::BulkRedisString(s) => {
            let val = if let Ok(s) = s.try_as_str() {
                Cow::Borrowed(s)
            } else {
                Cow::Owned(s.to_string())
            };
            Ok(val)
        },
        RedisValue::StringBuffer(s) => {
            let value = String::from_utf8_lossy(s);
            Ok(Cow::Owned(value.to_string()))
        },
        _ => Err(RedisError::Str("TSDB: cannot convert value to str")),
    }
}

pub(crate) fn redis_value_as_string(value: &RedisValue) -> RedisResult<Cow<String>> {
    match value {
        RedisValue::SimpleStringStatic(s) => Ok(Cow::Owned(s.to_string())),
        RedisValue::SimpleString(s) => Ok(Cow::Borrowed(&s)),
        RedisValue::BulkString(s) => Ok(Cow::Borrowed(&s)),
        RedisValue::BulkRedisString(s) => Ok(Cow::Owned(s.to_string())),
        RedisValue::StringBuffer(s) => {
            let value = String::from_utf8_lossy(s);
            Ok(Cow::Owned(value.to_string()))
        },
        _ => Err(RedisError::Str("TSDB: cannot convert value to str")),
    }
}

pub(crate) fn call_redis_command<'a>(ctx: &redis_module::Context, cmd: &'a str, args: &'a [String]) -> RedisResult {
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
) -> Result<TimestampRangeValue, RedisError> {
    parse_timestamp_range_value(arg).map_err(|_e| {
        let msg = format!("ERR invalid {} timestamp", name);
        RedisError::String(msg)
    })
}

pub(crate) fn normalize_range_args(
    start: Option<TimestampRangeValue>,
    end: Option<TimestampRangeValue>,
) -> RedisResult<(Timestamp, Timestamp)> {
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
        return Err(RedisError::Str(
            "ERR end timestamp must not be before start time",
        ));
    }

    Ok((start, end))
}
