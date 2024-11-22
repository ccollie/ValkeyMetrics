use valkey_module::raw;
use crate::series::chunks::compressed_vec::CompressedVecChunk;


pub fn rdb_save_compressed_vec_chunk(chunk: &CompressedVecChunk, rdb: *mut raw::RedisModuleIO) {
    rdb_save_usize(rdb, chunk.max_size);
    rdb_save_timestamp(rdb, chunk.first_timestamp);
    chunk.xor_encoder.rdb_save(rdb);
}