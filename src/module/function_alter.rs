use redis_module::{Context, NotifyEvent, REDIS_OK, RedisResult, RedisString};
use crate::globals::get_timeseries_index;
use crate::module::commands::parse_create_options;
use crate::module::get_timeseries_mut;

pub fn alter(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let (parsed_key, options) = parse_create_options(args)?;

    let mut series = get_series_mut(ctx, &parsed_key, true)?.unwrap();

    if let Some(retention) = options.retention {
        series.retention = retention;
    }

    series.dedupe_interval = options.dedupe_interval;

    if let Some(duplicate_policy) = options.duplicate_policy {
        series.duplicate_policy = Some(duplicate_policy);
    }

    if let Some(chunk_size) = options.chunk_size {
        series.chunk_size_bytes = chunk_size;
    }

    if let Some(labels) = options.labels {
        let ts_index = get_timeseries_index();
        let key = parsed_key.to_string();

        ts_index.remove_series_by_key(&key);
        ts_index.index_time_series(&mut series, key);

        series.labels = labels;
    }

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "PROM.ALTER", &parsed_key);

    REDIS_OK
}
