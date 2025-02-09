use crate::common::serialization::{
    rdb_load_timestamp, rdb_load_usize, rdb_save_timestamp, rdb_save_usize,
};
use crate::series::chunks::gorilla::GorillaEncoder;
use crate::series::GorillaChunk;
use valkey_module::{raw, ValkeyError, ValkeyResult};
use super::buffered_writer::BufferedWriter;

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
    let first_ts = rdb_load_timestamp(rdb)?;
    let encoder = GorillaEncoder::rdb_load(rdb)?;
    let chunk = GorillaChunk {
        encoder,
        first_ts,
        max_size,
    };
    Ok(chunk)
}

pub(super) fn save_bitwriter_to_rdb(rdb: *mut raw::RedisModuleIO, writer: &BufferedWriter) {
    let bytes = writer.get_ref();
    raw::save_slice(rdb, bytes);

    raw::save_unsigned(rdb, writer.position() as u64);
}

pub(super) fn load_bitwriter_from_rdb(
    rdb: *mut raw::RedisModuleIO,
) -> Result<BufferedWriter, ValkeyError> {
    // the load_string_buffer does not return an Err, so we can unwrap
    let bytes = raw::load_string_buffer(rdb)?.as_ref().to_vec();
    let pos = raw::load_unsigned(rdb)? as u32;

    let writer = BufferedWriter::hydrate(bytes, pos);

    Ok(writer)
}