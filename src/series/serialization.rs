use crate::common::serialization::*;
use crate::series::time_series::{TIMESTAMP_TYPE_U32, TIMESTAMP_TYPE_U64};
use crate::series::{
    rdb_load_series_chunk, rdb_save_series_chunk, Chunk, ChunkCompression, DuplicatePolicy,
    TimeSeries, TimeseriesId, TIMESTAMP_TYPE,
};
use metricsql_common::label::Label;
use valkey_module::{raw, ValkeyError, ValkeyResult};

pub const SERIES_ENC_VERSION: u64 = 1;

pub fn rdb_save_series(series: &TimeSeries, rdb: *mut raw::RedisModuleIO) {
    raw::save_string(rdb, TIMESTAMP_TYPE);
    raw::save_unsigned(rdb, series.id as u64);
    raw::save_string(rdb, &series.metric_name);
    rdb_save_usize(rdb, series.labels.len());
    for label in series.labels.iter() {
        raw::save_string(rdb, &label.name);
        raw::save_string(rdb, &label.value);
    }
    rdb_save_duration(rdb, &series.retention);
    rdb_save_optional_duration(rdb, &series.dedupe_interval);

    let mut tmp = series.duplicate_policy.as_str();
    raw::save_string(rdb, tmp);

    tmp = series.chunk_compression.name();
    raw::save_string(rdb, tmp);

    rdb_save_optional_rounding(rdb, &series.rounding);
    rdb_save_usize(rdb, series.chunk_size_bytes);
    rdb_save_usize(rdb, series.chunks.len());
    for chunk in series.chunks.iter() {
        rdb_save_series_chunk(chunk, rdb);
    }
}

pub fn rdb_load_series(rdb: *mut raw::RedisModuleIO, enc_ver: i32) -> ValkeyResult<TimeSeries> {
    let id_type: String = rdb_load_string(rdb)?;
    if id_type != TIMESTAMP_TYPE {
        let other_type = if id_type == TIMESTAMP_TYPE_U32 {
            TIMESTAMP_TYPE_U64
        } else {
            TIMESTAMP_TYPE_U32
        };
        let msg = format!("ERR module compiled with {other_type} timestamp support, found {id_type}. See the \"id64\" feature");
        return Err(ValkeyError::String(msg));
    }
    let id = raw::load_unsigned(rdb)? as TimeseriesId;
    let metric_name = rdb_load_string(rdb)?;
    let labels_len = rdb_load_usize(rdb)?;
    let mut labels = Vec::with_capacity(labels_len);
    for _ in 0..labels_len {
        let name = rdb_load_string(rdb)?;
        let value = rdb_load_string(rdb)?;
        labels.push(Label { name, value });
    }
    let retention = rdb_load_duration(rdb)?;

    let dedupe_interval = rdb_load_optional_duration(rdb)?;
    let duplicate_policy = DuplicatePolicy::try_from(rdb_load_string(rdb)?)?;

    let chunk_compression = ChunkCompression::try_from(rdb_load_string(rdb)?)?;

    let rounding = rdb_load_optional_rounding(rdb)?;
    let chunk_size_bytes = rdb_load_usize(rdb)?;
    let chunks_len = rdb_load_usize(rdb)?;
    let mut chunks = Vec::with_capacity(chunks_len);
    let mut last_value = f64::NAN;
    let mut total_samples: usize = 0;
    let mut first_timestamp = 0;
    let mut last_timestamp = 0;

    for _ in 0..chunks_len {
        let chunk = rdb_load_series_chunk(rdb, enc_ver)?;
        last_value = chunk.last_value();
        total_samples += chunk.len();
        if first_timestamp == 0 {
            first_timestamp = chunk.first_timestamp();
        }
        last_timestamp = last_timestamp.max(chunk.last_timestamp());
        chunks.push(chunk);
    }

    let ts = TimeSeries {
        id,
        metric_name,
        labels,
        retention,
        dedupe_interval,
        dedupe_value_delta: None,
        duplicate_policy,
        chunk_compression,
        rounding,
        chunk_size_bytes,
        chunks,
        total_samples,
        first_timestamp,
        last_timestamp,
        last_value,
    };

    // ts.update_meta();
    // add to index
    Ok(ts)
}
