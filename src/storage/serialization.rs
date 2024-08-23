use std::error::Error;
use std::fmt::Display;
use std::mem::size_of;
use std::str::FromStr;

use get_size::GetSize;
use metricsql_encoding::marshal::{marshal_var_i64, unmarshal_var_i64};
use rand_distr::num_traits::Zero;
use serde::{Deserialize, Serialize};

use crate::error::{TsdbError, TsdbResult};

pub(super) fn read_date_range(compressed: &mut &[u8]) -> TsdbResult<Option<(i64, i64)>> {
    if (*compressed).is_empty() {
        return Ok(None);
    }
    let first_ts = read_timestamp(compressed)?;
    let last_ts = read_timestamp(compressed)?;
    Ok(Some((first_ts, last_ts)))
}

pub(super) fn read_timestamp(compressed: &mut &[u8]) -> TsdbResult<i64> {
    let (value, remaining) = unmarshal_var_i64(compressed)
        .map_err(|e| TsdbError::CannotDeserialize(format!("timestamp: {}", e)))?;
    *compressed = remaining;
    Ok(value)
}

pub(super) fn write_timestamp(dest: &mut Vec<u8>, ts: i64) {
    marshal_var_i64(dest, ts);
}

pub(super) fn write_usize(slice: &mut Vec<u8>, size: usize) {
    slice.extend_from_slice(&size.to_le_bytes());
}

pub(super) fn write_usize_in_place(vec: &mut Vec<u8>, index: usize, value: usize) {
    let bytes = value.to_le_bytes();
    let end = index + bytes.len();
    if end > vec.len() {
        panic!("Index out of bounds");
    }
    vec[index..end].copy_from_slice(&bytes);
}

pub(super) fn read_usize(input: &mut &[u8], field: &str) -> TsdbResult<usize> {
    let (int_bytes, rest) = input.split_at(size_of::<usize>());
    let buf = int_bytes
        .try_into()
        .map_err(|_| TsdbError::CannotDeserialize(format!("invalid usize reading {}", field).to_string()))?;

    *input = rest;
    Ok(usize::from_le_bytes(buf))
}


#[cfg(test)]
mod tests {
    // Single series
}
