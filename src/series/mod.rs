use get_size::GetSize;
use metricsql_common::prelude::Label;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::mem::size_of;
use std::str::FromStr;
use std::time::Duration;
use valkey_module::ValkeyError;

mod constants;
pub mod time_series;
pub(crate) mod utils;
mod defrag;
pub mod index;
pub mod chunks;
mod test_utils;
mod serialization;
pub mod types;
pub(crate) mod join_reducer;
mod series_storage;
mod merge;
mod timestamp_range;

use crate::common::types::{Sample};
pub(super) use chunks::*;
pub(crate) use constants::*;
pub(crate) use defrag::*;
pub(crate) use time_series::*;
pub(crate) use timestamp_range::*;
pub(crate) use types::*;


cfg_if::cfg_if! {
    if #[cfg(test)] {
        mod types_tests;
        mod timestamp_range_tests;
        mod timeseries_tests;
    }
}

pub const SAMPLE_SIZE: usize = size_of::<Sample>();

#[non_exhaustive]
#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
#[derive(GetSize)]
pub enum ChunkEncoding {
    #[default]
    Compressed,
    Uncompressed,
}

impl ChunkEncoding {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChunkEncoding::Compressed => "COMPRESSED",
            ChunkEncoding::Uncompressed => "UNCOMPRESSED",
        }
    }
}

impl Display for ChunkEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<&str> for ChunkEncoding {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            s if s.eq_ignore_ascii_case("compressed") => Ok(ChunkEncoding::Compressed),
            s if s.eq_ignore_ascii_case("uncompressed") => Ok(ChunkEncoding::Uncompressed),
            _ => Err(format!("invalid encoding: {}", value)),
        }
    }
}
