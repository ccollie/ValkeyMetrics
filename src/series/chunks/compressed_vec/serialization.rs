use compressed_vec::{VectorF32XorAppender, VectorReader, VectorU64Appender};
use valkey_module::{raw, ValkeyError, ValkeyResult};
use crate::common::serialization::{rdb_load_timestamp, rdb_load_usize, rdb_save_timestamp, rdb_save_usize};
use crate::series::chunks::compressed_vec::CompressedVecChunk;

// TODO: track https://github.com/velvia/compressed-vec/issues/8
pub fn rdb_save_compressed_vec_chunk(chunk: &CompressedVecChunk, rdb: *mut raw::RedisModuleIO) {
    let (value_buffer, ts_buffer) = match get_chunk_buffers(chunk) {
        Some((value_buffer, ts_buffer)) => (value_buffer, ts_buffer),
        None => return
    };
    rdb_save_usize(rdb, chunk.max_size_bytes);
    rdb_save_timestamp(rdb, chunk.start_ts);
    rdb_save_timestamp(rdb, chunk.end_ts);
    raw::save_double(rdb, chunk.last_value);

    raw::save_slice(rdb, &ts_buffer);
    raw::save_slice(rdb, &value_buffer);
}

pub fn rdb_load_compressed_vec_chunk(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<CompressedVecChunk> {
    let max_size_bytes = rdb_load_usize(rdb)?;
    let start_ts = rdb_load_timestamp(rdb)?;
    let end_ts = rdb_load_timestamp(rdb)?;
    let last_value = raw::load_double(rdb)?;

    let ts_buffer = raw::load_string_buffer(rdb)?;
    let timestamps = load_timestamps(ts_buffer.as_ref())?;

    let value_buffer = raw::load_string_buffer(rdb)?;
    let values = load_values(value_buffer.as_ref())?;

    let chunk = CompressedVecChunk {
        max_size_bytes,
        start_ts,
        end_ts,
        last_value,
        values,
        timestamps,
        init_size: CompressedVecChunk::calc_init_size(max_size_bytes),
    };

    Ok(chunk)
}

fn load_timestamps(buffer: &[u8]) -> ValkeyResult<VectorU64Appender> {
    VectorReader::<u64>::try_new(buffer)
        .and_then(|reader| {
            VectorU64Appender::try_new(reader.num_elements())
                .and_then(|mut appender| {
                    for ts in reader.iterate() {
                        appender.append(*ts)?;
                    }
                    Ok(appender)
                })
        }).map_err(|_| ValkeyError::Str("Failed to create timestamp reader")) // todo: put err in error_consts
}

fn load_values(buffer: &[u8]) -> ValkeyResult<VectorF32XorAppender> {
    VectorReader::<f32>::try_new(buffer)
        .and_then(|reader| {
            VectorF32XorAppender::try_new(reader.num_elements())
                .and_then(|mut appender| {
                    for value in reader.iterate() {
                        appender.append(*value)?;
                    }
                    Ok(appender)
                })
        }).map_err(|_| ValkeyError::Str("Failed to create value reader")) // todo: put err in error_consts
}

fn get_chunk_buffers(chunk: &CompressedVecChunk) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut data = chunk.values.clone();
    let mut timestamps = chunk.timestamps.clone();
    let value_buffer = data.finish(data.len());
    let ts_buffer = timestamps.finish(timestamps.len());
    match (value_buffer, ts_buffer) {
        (Some(value_buffer), Some(ts_buffer)) => {
            Some((value_buffer, ts_buffer))
        }
        _ => None
    }
}