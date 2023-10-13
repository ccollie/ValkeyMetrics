use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use crate::module::{get_timeseries, parse_timestamp_arg};
use crate::module::result::sample_to_result;

pub fn range(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;

    // Safety: we just checked that the key exists
    let series = get_timeseries(ctx, &key, true)?.unwrap();

    let from = parse_timestamp_arg(ctx, args.next_str()?, "startTimestamp")?;
    let to = parse_timestamp_arg(ctx, args.next_str()?, "endTimestamp")?;

    args.done()?;

    let start = from.to_timestamp();
    let end = to.to_timestamp();

    if start > end {
        return Err(RedisError::Str("ERR invalid range"));
    }

    let samples = series.get_range(start, end)
        .map_err(|e| {
            ctx.log_warning(&*format!("ERR fetching range {:?}", e));
            RedisError::Str("ERR fetching range")
        })?;

    let result = samples.iter().map(|s| sample_to_result(s.timestamp, s.value)).collect();
    Ok(RedisValue::Array(result))
}
