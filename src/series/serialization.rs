use std::time::Duration;
use metricsql_runtime::types::Timestamp;
use valkey_module::{raw, ValkeyError, ValkeyResult};
const OPTIONAL_MARKER_PRESENT: u64 = 0xfe;
const OPTIONAL_MARKER_ABSENT: u64 = 0xff;

fn value_present(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<bool> {
    let marker = raw::load_unsigned(rdb)?;
    match marker {
        OPTIONAL_MARKER_PRESENT => Ok(true),
        OPTIONAL_MARKER_ABSENT => Ok(false),
        _ => {
            Err(ValkeyError::String(format!("Invalid marker: {marker}")))
        }
    }
}

pub fn rdb_save_duration(rdb: *mut raw::RedisModuleIO, duration: &Duration) {
    let millis = duration.as_millis() as i64;
    raw::save_signed(rdb, millis);
}

pub(crate) fn rdb_save_optional_duration(rdb: *mut raw::RedisModuleIO, duration: &Option<Duration>) {
    if let Some(duration) = duration {
        raw::save_unsigned(rdb, OPTIONAL_MARKER_PRESENT);
        rdb_save_duration(rdb, duration);
    } else {
        raw::save_unsigned(rdb, OPTIONAL_MARKER_ABSENT);
    }
}

pub(crate) fn rdb_load_duration(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Duration> {
    let millis = raw::load_unsigned(rdb)?;
    Ok(Duration::from_millis(millis as u64))
}

pub(crate) fn rdb_load_optional_duration(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Option<Duration>> {
    if value_present(rdb)? {
        let duration = rdb_load_duration(rdb)?;
        Ok(Some(duration))
    } else {
        Ok(None)
    }
}

pub(crate) fn rdb_save_usize(rdb: *mut raw::RedisModuleIO, value: usize) {
    raw::save_unsigned(rdb, value as u64)
}

pub(crate) fn rdb_load_usize(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<usize> {
    let value = raw::load_unsigned(rdb)?;
    Ok(value as usize)
}

pub(crate) fn rdb_save_timestamp(rdb: *mut raw::RedisModuleIO, value: Timestamp) {
    raw::save_signed(rdb, value)
}

pub(crate) fn rdb_load_timestamp(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Timestamp> {
    let value = raw::load_signed(rdb)?;
    Ok(value as Timestamp)
}

pub(crate) fn rdb_load_u8(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<u8> {
    let value = raw::load_unsigned(rdb)?;
    Ok(value as u8)
}