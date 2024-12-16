use std::sync::RwLock;
use crate::common::serialization::{rdb_load_usize, rdb_save_usize};
use crate::common::types::IntMap;
use crate::series::index::index_key::IndexKey;
use crate::series::index::postings::Postings;
use crate::series::index::{ARTBitmap, IdBitmap, KeyType, TimeSeriesIndex, TIMESERIES_INDEX};
use crate::series::TimeseriesId;
use crate::server_events::is_async_loading_in_progress;
use ahash::HashMapExt;
use blart::AsBytes;
use croaring::Portable;
use std::os::raw::c_int;
use std::sync::{LazyLock, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use valkey_module::{logging, raw, ValkeyError, ValkeyResult};

pub(super) static STAGED_TIMESERIES_INDEX: LazyLock<Mutex<std::collections::HashMap<i32, TimeSeriesIndex>>> 
    = LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));


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

fn serialize_index_inner(rdb: *mut raw::RedisModuleIO, inner: &Postings) {
    rdb_save_usize(rdb, inner.label_count);
    serialize_art_bitmap(rdb, &inner.label_index);
    serialize_int_key_map(rdb, &inner.id_to_key);
    raw::save_unsigned(rdb, inner.changes_since_last_optimize as u64);
}

fn deserialize_index_inner(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Postings> {
    let label_count = rdb_load_usize(rdb)?;
    let label_index = deserialize_art_bitmap(rdb)?;
    let id_to_key = deserialize_int_key_map(rdb)?;
    let changes_since_last_optimize = raw::load_unsigned(rdb)? as usize;
    
    Ok(Postings {
        label_count,
        label_index,
        id_to_key,
        changes_since_last_optimize,
    })
}

pub fn serialize_timeseries_index(rdb: *mut raw::RedisModuleIO, index: &TimeSeriesIndex) {
    let inner = index.inner.read().unwrap();
    let id = index.last_id.load(Ordering::Relaxed);
    raw::save_unsigned(rdb, id);
    serialize_index_inner(rdb, &inner);
}

pub fn deserialize_timeseries_index(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<TimeSeriesIndex> {
    let inner = deserialize_index_inner(rdb)?;
    let id = raw::load_unsigned(rdb)?;
    
    Ok(TimeSeriesIndex { inner: RwLock::new(inner), last_id: AtomicU64::new(id) })
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
    let len = raw::load_unsigned(rdb)?;
    
    if is_async_loading_in_progress() {
        let mut staged = STAGED_TIMESERIES_INDEX.lock()
            .map_err(|_| ValkeyError::Str("Error loading AUX fields"))?;
        for _ in 0..len {
            let ts_id = raw::load_signed(rdb)? as i32;
            let index = deserialize_timeseries_index(rdb)?;
            staged.insert(ts_id, index);
        }
    } else {
        let map = TIMESERIES_INDEX.pin();

        for _ in 0..len {
            let ts_id = raw::load_signed(rdb)? as i32;
            let index = deserialize_timeseries_index(rdb)?;
            map.insert(ts_id, index);
        }    
    }
    
    Ok(())
} 


/// Load the auxiliary data outside the regular keyspace from the RDB file
pub extern "C" fn ts_index_rdb_aux_load(
    rdb: *mut raw::RedisModuleIO,
    _enc_ver: c_int,
    _when: c_int,
) -> c_int {
    logging::log_notice("Loading timeseries AUX fields during RDB load.");
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

pub fn series_on_async_load_done(completed: bool) {
    let staged = std::mem::take(&mut *STAGED_TIMESERIES_INDEX.lock().unwrap());
    if completed {
        let ts_index = TIMESERIES_INDEX.pin();
        // todo: it's much faster to do a swap, but LazyLock doesn't support it and using
        // a Mutex would be a performance hit.
        for (k, v) in staged.into_iter() {
            ts_index.insert(k, v);
        }
    }
}
