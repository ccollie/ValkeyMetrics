use std::fmt::Display;
use std::time::Duration;
use ahash::AHashMap;
use metricsql_engine::Timestamp;
use redis_module::{Context, NotifyEvent, RedisError, RedisResult, RedisString};
use redis_module::key::RedisKeyWritable;
use crate::globals::get_timeseries_index;
use crate::module::REDIS_PROMQL_SERIES_TYPE;
use crate::ts::{DuplicatePolicy, TimeSeriesOptions};
use crate::ts::time_series::TimeSeries;

pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 4 * 1024;
pub type Labels = AHashMap<String, String>;  // todo use ahash

pub(crate) fn create_timeseries(
    key: &RedisString,
    options: TimeSeriesOptions,
) -> TimeSeries {
    let mut ts = TimeSeries::new();
    ts.metric_name = options.metric_name.unwrap_or_else(|| key.to_string());
    ts.chunk_size_bytes = options.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE_BYTES);
    ts.retention = options.retention.unwrap_or(Duration::from_millis(0u64));
    ts.dedupe_interval = options.dedupe_interval;
    ts.duplicate_policy = options.duplicate_policy;
    if let Some(labels) = options.labels {
        ts.labels = labels;
    }

    let ts_index = get_timeseries_index();
    ts_index.index_time_series(&mut ts, key.to_string());
    ts
}

pub(crate) fn create_and_store_series(ctx: &Context, key: &RedisString, options: TimeSeriesOptions) -> RedisResult<()> {
    let series_key = RedisKeyWritable::open(ctx.ctx, &key);
    // check if this refers to an existing series
    if !series_key.is_empty() {
        return Err(RedisError::Str("TSDB: the key already exists"));
    }

    let ts = create_timeseries(&key, options);
    series_key.set_value(&REDIS_PROMQL_SERIES_TYPE, ts)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "PROM.CREATE-SERIES", &key);
    ctx.log_verbose("series created");

    Ok(())
}

pub(crate) fn series_exists(ctx: &Context, key: &RedisString) -> bool {
    let series_key = ctx.open_key(key.into());
    !series_key.is_empty()
}

pub(crate) fn get_series_mut<'a>(ctx: &'a Context, key: &RedisString, must_exist: bool) -> RedisResult<Option<&'a mut TimeSeries>> {
    let redis_key = ctx.open_key_writable(key.into());
    let result = redis_key.get_value::<TimeSeries>(&REDIS_PROMQL_SERIES_TYPE)?;
    if must_exist && result.is_none() {
        return Err(RedisError::Str("ERR TSDB: the key is not a timeseries"));
    }
    Ok(result)
}

pub(crate) fn get_timeseries<'a>(ctx: &'a Context, key: &RedisString, must_exist: bool) -> RedisResult<Option<&'a TimeSeries>> {
    let redis_key = ctx.open_key(key.into());
    let result = redis_key.get_value::<TimeSeries>(&REDIS_PROMQL_SERIES_TYPE)?;
    if must_exist && result.is_none() {
        return Err(RedisError::Str("ERR TSDB: the key is not a timeseries"));
    }
    Ok(result)
}

pub(crate) fn get_timeseries_multi<'a>(ctx: &'a Context, keys: &[&RedisString]) -> RedisResult<Vec<Option<&'a TimeSeries>>> {
    keys
        .iter()
        .map(|key| get_timeseries(ctx, key, false)).collect::<Result<Vec<_>, _>>()
}



pub(super) fn internal_add(
    ctx: &Context,
    series: &mut TimeSeries,
    timestamp: Timestamp,
    value: f64,
    dp_override: DuplicatePolicy,
) -> Result<(), RedisError> {
    let last_ts = series.last_timestamp;
    let retention = series.retention.as_millis() as i64;
    // ensure inside retention period.
    if retention > 0 && (timestamp < last_ts) && retention < (last_ts - timestamp) {
        return Err(RedisError::Str("TSDB: Timestamp is older than retention"));
    }

    if let Some(dedup_interval) = series.dedupe_interval {
        let millis = dedup_interval.as_millis() as i64;
        if millis > 0 && (timestamp - last_ts) < millis {
            return Err(RedisError::Str(
                "TSDB: new sample in less than dedupe interval",
            ));
        }
    }

    if timestamp <= series.last_timestamp && series.total_samples != 0 {
        let val = series.upsert_sample(timestamp, value, Some(dp_override));
        if val.is_err() {
            let msg = "TSDB: Error at upsert, cannot update when DUPLICATE_POLICY is set to BLOCK";
            return Err(RedisError::Str(msg));
        }
        return Ok(());
    }

    series
        .append(timestamp, value)
        .map_err(|_| RedisError::Str("TSDB: Error at append"))
}
