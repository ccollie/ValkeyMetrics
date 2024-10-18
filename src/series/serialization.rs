use std::time::Duration;
use metricsql_runtime::types::Timestamp;
use valkey_module::{raw, ValkeyError, ValkeyResult, ValkeyString};
use valkey_module::error::Error;
use crate::series::types::RoundingStrategy;

const OPTIONAL_MARKER_PRESENT: u64 = 0xfe;
const OPTIONAL_MARKER_ABSENT: u64 = 0xff;

fn load_optional_marker(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<bool> {
    let marker = raw::load_unsigned(rdb)?;
    match marker {
        OPTIONAL_MARKER_PRESENT => Ok(true),
        OPTIONAL_MARKER_ABSENT => Ok(false),
        _ => {
            Err(ValkeyError::String(format!("Invalid marker: {marker}")))
        }
    }
}

fn rdb_save_optional_marker(rdb: *mut raw::RedisModuleIO, is_some: bool) {
    if is_some {
        raw::save_unsigned(rdb, OPTIONAL_MARKER_PRESENT);
    } else {
        raw::save_unsigned(rdb, OPTIONAL_MARKER_ABSENT);
    }
}

pub fn rdb_save_duration(rdb: *mut raw::RedisModuleIO, duration: &Duration) {
    let millis = duration.as_millis() as i64;
    raw::save_signed(rdb, millis);
}

pub fn rdb_save_optional_duration(rdb: *mut raw::RedisModuleIO, duration: &Option<Duration>) {
    if let Some(duration) = duration {
        rdb_save_optional_marker(rdb, true);
        rdb_save_duration(rdb, duration);
    } else {
        rdb_save_optional_marker(rdb, false);
    }
}

pub fn rdb_load_duration(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Duration> {
    let millis = raw::load_unsigned(rdb)?;
    Ok(Duration::from_millis(millis))
}

pub(crate) fn rdb_load_optional_duration(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Option<Duration>> {
    if load_optional_marker(rdb)? {
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


pub(crate) fn rdb_save_u8(rdb: *mut raw::RedisModuleIO, value: u8) {
    raw::save_unsigned(rdb, value as u64)
}

pub(crate) fn rdb_load_u8(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<u8> {
    let value = raw::load_unsigned(rdb)?;
    // todo: validate that value is in range
    Ok(value as u8)
}

#[inline]
pub(crate) fn rdb_save_u32(rdb: *mut raw::RedisModuleIO, value: u32) {
    raw::save_unsigned(rdb, value as u64)
}

#[inline]
pub(crate) fn rdb_load_u32(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<u32> {
    let value = raw::load_signed(rdb)?;
    // todo: validate that value is in range
    Ok(value as u32)
}

#[inline]
pub(crate) fn rdb_save_i32(rdb: *mut raw::RedisModuleIO, value: i32) {
    raw::save_signed(rdb, value as i64)
}

#[inline]
pub(crate) fn rdb_load_i32(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<i32> {
    let value = raw::load_signed(rdb)?;
    // todo: validate that value is in range
    Ok(value as i32)
}

pub(crate) fn rdb_save_rounding(rdb: *mut raw::RedisModuleIO, rounding: &RoundingStrategy) {
    match rounding {
        RoundingStrategy::SignificantDigits(sig_figs) => {
            rdb_save_u8(rdb, 1);
            rdb_save_i32(rdb, *sig_figs)
        }
        RoundingStrategy::DecimalDigits(digits) => {
            rdb_save_u8(rdb, 2);
            rdb_save_i32(rdb, *digits)
        }
    }
}

pub(crate) fn rdb_load_rounding(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<RoundingStrategy> {
    let marker = rdb_load_u8(rdb)?;
    match marker {
        1 => {
            let sig_figs = rdb_load_i32(rdb)?;
            Ok(RoundingStrategy::SignificantDigits(sig_figs))
        }
        2 => {
            let digits = rdb_load_i32(rdb)?;
            Ok(RoundingStrategy::DecimalDigits(digits))
        }
        _ => Err(ValkeyError::String(format!("Invalid rounding marker: {marker}"))),
    }
}

pub(crate) fn rdb_save_optional_rounding(rdb: *mut raw::RedisModuleIO, rounding: &Option<RoundingStrategy>) {
    if let Some(rounding) = rounding {
        rdb_save_optional_marker(rdb, true);
        rdb_save_rounding(rdb, rounding)
    } else {
        rdb_save_optional_marker(rdb, false);
    }
}

pub(crate) fn rdb_load_optional_rounding(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Option<RoundingStrategy>> {
    if load_optional_marker(rdb)? {
        let rounding = rdb_load_rounding(rdb)?;
        Ok(Some(rounding))
    } else {
        Ok(None)
    }
}


#[inline]
pub(crate) fn rdb_save_string(rdb: *mut raw::RedisModuleIO, value: &str) {
    raw::save_string(rdb, value);
}

#[inline]
pub(crate) fn rdb_load_string(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<String> {
    Ok(String::from(raw::load_string(rdb)?))
}

#[inline]
pub(crate) fn rdb_load_valkey_string(rdb: *mut raw::RedisModuleIO) -> Result<ValkeyString, Error> {
    raw::load_string(rdb)
}