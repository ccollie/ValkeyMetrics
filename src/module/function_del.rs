use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use crate::module::parse_timestamp_arg;
use crate::ts::get_timeseries_mut;

pub fn del_range(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;

    // Safety: we just checked that the key exists
    let series = get_series_mut(ctx, &key, true)?.unwrap();

    let from = parse_timestamp_arg( args.next_str()?, "startTimestamp")?;
    let to = parse_timestamp_arg( args.next_str()?, "endTimestamp")?;

    args.done()?;

    let start = from.to_timestamp();
    let end = to.to_timestamp();

    if start > end {
        return Err(RedisError::Str("ERR invalid range"));
    }

    let sample_count = series.remove_range(start, end)?;

    Ok(RedisValue::from(sample_count))
}
