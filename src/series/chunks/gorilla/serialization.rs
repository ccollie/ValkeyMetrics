use crate::common::serialization::{
    rdb_load_timestamp, rdb_load_usize, rdb_save_timestamp, rdb_save_usize,
};
use crate::series::chunks::gorilla::GorillaEncoder;
use crate::series::GorillaChunk;
use valkey_module::{raw, ValkeyResult};

pub fn rdb_save_gorilla_chunk(chunk: &GorillaChunk, rdb: *mut raw::RedisModuleIO) {
    rdb_save_usize(rdb, chunk.max_size);
    rdb_save_timestamp(rdb, chunk.first_ts);
    chunk.encoder.rdb_save(rdb);
}

pub fn rdb_load_gorilla_chunk(
    rdb: *mut raw::RedisModuleIO,
    _encver: i32,
) -> ValkeyResult<GorillaChunk> {
    let max_size = rdb_load_usize(rdb)?;
    let first_timestamp = rdb_load_timestamp(rdb)?;
    let xor_encoder = GorillaEncoder::rdb_load(rdb)?;
    let chunk = GorillaChunk {
        encoder: xor_encoder,
        first_ts: first_timestamp,
        max_size,
    };
    Ok(chunk)
}
