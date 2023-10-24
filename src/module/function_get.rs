use redis_module::{Context, NextArg, RedisResult, RedisString, RedisValue};
use crate::module::get_timeseries_mut;

pub(crate) fn get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;

    // Safety: the 3rd parameter means that wee Err is not found and the error is propagated out
    // before the unwrap()
    let series = get_timeseries_mut(ctx, &key, true)?.unwrap();

    let result = if series.is_empty() {
        vec![]
    } else {
        vec![RedisValue::from(series.last_timestamp), RedisValue::from(series.last_value)]
    };

    Ok(RedisValue::Array(result))
}
