use crate::common::serialization::{rdb_load_usize, rdb_save_usize};
use crate::common::types::IntMap;
use crate::series::index::index_key::IndexKey;
use crate::series::index::timeseries_index::IndexInner;
use crate::series::index::{ARTBitmap, IdBitmap, KeyType, TimeSeriesIndex, TimeSeriesIndexMap, TIMESERIES_INDEX};
use crate::series::TimeseriesId;
use ahash::HashMapExt;
use blart::AsBytes;
use std::os::raw::c_int;
use std::sync::{LazyLock, RwLock};
use croaring::Portable;
use valkey_module::{logging, raw, ValkeyError, ValkeyResult};
use crate::server_events::is_async_loading_in_progress;

pub(super) static STAGED_TIMESERIES_INDEX: LazyLock<TimeSeriesIndexMap> = LazyLock::new(TimeSeriesIndexMap::new);


fn serialize_art_bitmap(rdb: *mut raw::RedisModuleIO, bmp: &ARTBitmap) {
    // Serialize the ARTBitmap data to the RDB file here.
    let count = bmp.len();
    rdb_save_usize(rdb, count);
    
    let mut buffer: Vec<u8> = Vec::new();
    
    for (key, value) in bmp.iter() {
        raw::save_slice(rdb, key.as_bytes());
        let slice = value.serialize_into_vec::<Portable>(&mut buffer);
        raw::save_slice(rdb, slice);
        buffer.clear();
    }
}


fn deserialize_art_bitmap(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<ARTBitmap> {
    let count = rdb_load_usize(rdb)?;
    let mut index_map = ARTBitmap::new();
    
    for _ in 0..count {
        let key_buf = raw::load_string_buffer(rdb)?;
        let value_buf = raw::load_string_buffer(rdb)?;
        
        let value_bmp = IdBitmap::deserialize::<Portable>(value_buf.as_ref());
        
        let key: IndexKey = key_buf.as_ref().into();
        index_map.try_insert(key, value_bmp)
            .map_err(|_| ValkeyError::Str("Error deserializing bitmap"))?;
    }
    
    Ok(index_map)
}

fn serialize_int_key_map(rdb: *mut raw::RedisModuleIO, map: &IntMap<TimeseriesId, KeyType>) {
    // Serialize the IntMap data to the RDB file here.
    let count = map.len();
    rdb_save_usize(rdb, count);
    
    for (key, value) in map.iter() {
        raw::save_unsigned(rdb, *key as u64);
        raw::save_slice(rdb, value.as_bytes());
    }
}

fn deserialize_int_key_map(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<IntMap<TimeseriesId, KeyType>> {
    let count = rdb_load_usize(rdb)?;
    let mut map: IntMap<TimeseriesId, KeyType> = IntMap::with_capacity(count);
    
    for _ in 0..count {
        let key = raw::load_unsigned(rdb)? as TimeseriesId;
        let value_buf = raw::load_string_buffer(rdb)?;
        
        let value: KeyType = value_buf.as_ref().into();
        map.insert(key, value);
    }
    
    Ok(map)
}

fn serialize_index_inner(rdb: *mut raw::RedisModuleIO, inner: &IndexInner) {
    rdb_save_usize(rdb, inner.label_count);
    serialize_art_bitmap(rdb, &inner.label_index);
    serialize_int_key_map(rdb, &inner.id_to_key);
}

fn deserialize_index_inner(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<IndexInner> {
    let label_count = rdb_load_usize(rdb)?;
    let label_index = deserialize_art_bitmap(rdb)?;
    let id_to_key = deserialize_int_key_map(rdb)?;
    
    Ok(IndexInner {
        label_count,
        label_index,
        id_to_key,
    })
}

pub fn serialize_timeseries_index(rdb: *mut raw::RedisModuleIO, index: &TimeSeriesIndex) {
    let inner = index.inner.read().unwrap();
    serialize_index_inner(rdb, &inner);
}

pub fn deserialize_timeseries_index(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<TimeSeriesIndex> {
    let inner = deserialize_index_inner(rdb)?;
    Ok(TimeSeriesIndex { inner: RwLock::new(inner) })
}

fn aux_save(rdb: *mut raw::RedisModuleIO) {
    let map = TIMESERIES_INDEX.pin();
    let len = map.len() as u64;
    raw::save_unsigned(rdb, len);
    
    for (k, v) in map.iter() {
        raw::save_unsigned(rdb, *k as u64);
        serialize_timeseries_index(rdb, v);
    }
}

fn aux_load(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<()> {
    let is_async = is_async_loading_in_progress();
    let len = raw::load_unsigned(rdb)?;
    
    let map = if is_async {
        STAGED_TIMESERIES_INDEX.pin()
    } else {
        TIMESERIES_INDEX.pin()
    };

    map.clear();
    
    for _ in 0..len {
        let ts_id = raw::load_unsigned(rdb)? as u32;
        let index = deserialize_timeseries_index(rdb)?;
        map.insert(ts_id, index);
    }
    
    Ok(())
} 


/// Load the auxiliary data outside the regular keyspace from the RDB file
pub extern "C" fn ts_index_rdb_aux_load(
    rdb: *mut raw::RedisModuleIO,
    _enc_ver: c_int,
    _when: c_int,
) -> c_int {
    logging::log_notice("Ignoring AUX fields during RDB load.");
    if let Err(e) = aux_load(rdb) {
        logging::log_warning(format!("Error loading AUX fields: {}", e));
        return raw::Status::Err as i32
    }
    raw::Status::Ok as i32
}

pub extern "C" fn ts_index_rdb_aux_save(rdb: *mut raw::RedisModuleIO, when: c_int) {
    logging::log_notice(format!("Saving AUX fields for time {}.", when).as_str());
    aux_save(rdb)
}

pub fn rdb_on_async_load_completed() {
    let staged = STAGED_TIMESERIES_INDEX.pin();
    let mut ts_index = TIMESERIES_INDEX.pin();
    // todo: it's much faster to do a swap, but LazyLock doesn't support it and using
    // a Mutex would be a performance hit.
    staged.iter().collect_into(&mut ts_index);
}

pub fn rdb_on_async_load_aborted() {
    STAGED_TIMESERIES_INDEX.pin().clear();
}