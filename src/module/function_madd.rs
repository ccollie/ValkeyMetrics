use redis_module::{Context, NextArg, REDIS_OK, RedisError, RedisResult, RedisString, RedisValue};
use crate::common::parse_timestamp;
use crate::module::get_timeseries_mut;

pub(crate) fn madd(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let arg_count = args.len() - 1;
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;

    if arg_count < 3 {
        return Err(RedisError::WrongArity);
    }

    if arg_count % 3 != 0 {
        return Err(RedisError::Str("ERR TSDB: wrong number of arguments for 'TS.MADD' command"));
    }

    let sample_count = arg_count / 3;

    if let Some(series) = get_series_mut(ctx, &key, true)? {
        let policy = series.duplicate_policy.unwrap_or_default();

        let mut values: Vec<RedisValue> = Vec::with_capacity(sample_count);

        while let Some(arg) = args.next() {
            let res = get_series_mut(ctx, &arg, true);
            if res.is_err() {
                values.push(RedisValue::SimpleStringStatic("ERR TSDB: the key is not a timeseries") );
            } else {
                let timestamp = parse_timestamp(args.next_str()?)?;
                let value = args.next_f64()?;
                // Safety: we checked above that the key exists
                let series = res.unwrap().unwrap();
                if let Ok(res) = series.add( timestamp, value, Some(policy)) {
                }
            }
        }

        return Ok(RedisValue::Array(values));
    }

    REDIS_OK
}
