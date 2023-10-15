use std::borrow::Cow;
use redis_module::{CallOptionResp, CallOptions, CallOptionsBuilder, CallReply, CallResult, RedisError, RedisResult, RedisValue};
use redis_module::redisvalue::RedisValueKey;
use crate::common::{current_time_millis, parse_timestamp_range_value};
use crate::common::types::{Timestamp, TimestampRangeValue};
use crate::config::get_global_settings;
use crate::index::RedisContext;

// todo: utils
pub fn call_reply_to_i64(reply: &CallReply) -> i64 {
    match reply {
        CallReply::I64(i) => i.to_i64(),
        _ => panic!("unexpected reply type"),
    }
}

pub fn call_reply_to_f64(reply: &CallReply) -> f64 {
    match reply {
        CallReply::Double(d) => d.to_double(),
        CallReply::I64(i) => i.to_i64() as f64,
        _ => panic!("unexpected reply type"),
    }
}

pub(crate) fn redis_value_to_f64(value: &RedisValue) -> RedisResult<f64> {
    match value {
        RedisValue::Integer(i) => Ok(*i as f64),
        RedisValue::Float(f) => Ok(*f),
        _ => Err(RedisError::Str("TSDB: cannot convert value to float")),
    }
}


pub(crate) fn redis_value_to_i64(value: &RedisValue) -> RedisResult<i64> {
    match value {
        RedisValue::Integer(i) => Ok(*i),
        RedisValue::Float(f) => Ok(*f as i64),  // todo: handle overflow
        _ => Err(RedisError::Str("TSDB: cannot convert value to i64")),
    }
}

pub(crate) fn redis_value_key_as_str(value: &RedisValueKey) -> RedisResult<Cow<str>> {
    match value {
        RedisValueKey::String(s) => Ok(Cow::Borrowed(s)),
        RedisValueKey::BulkString(s) => {
            let value = String::from_utf8_lossy(s);
            Ok(Cow::Owned(value.to_string()))
        },
        RedisValueKey::BulkRedisString(s) => {
            let val = if let Ok(s) = s.try_as_str() {
                Cow::Borrowed(s)
            } else {
                Cow::Owned(s.to_string())
            };
            Ok(val)
        },
        _ => Err(RedisError::Str("TSDB: cannot convert value to str")),
    }

}

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
    ctx: &RedisContext,
    arg: &str,
    name: &str,
) -> Result<TimestampRangeValue, RedisError> {
    parse_timestamp_range_value(ctx, arg).map_err(|_e| {
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
