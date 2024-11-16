use std::mem::size_of;
use std::sync::LazyLock;
use std::time::Duration;

mod constants;
pub mod time_series;
pub(crate) mod utils;
mod defrag;
pub mod index;
pub mod chunks;
pub mod types;
mod merge;
mod timestamp_range;
mod serialization;

use crate::common::types::Sample;
pub(super) use chunks::*;
pub(crate) use constants::*;
pub(crate) use defrag::*;
pub(crate) use time_series::*;
pub(crate) use timestamp_range::*;
pub(crate) use types::*;
use crate::common::rounding::RoundingStrategy;
use crate::config::get_series_settings;

cfg_if::cfg_if! {
    if #[cfg(test)] {
        mod timeseries_tests;
        pub mod test_utils;
    }
}

pub const SAMPLE_SIZE: usize = size_of::<Sample>();

#[derive(Clone, Copy)]
pub struct SeriesSettings {
    pub retention_period: Option<Duration>,
    pub chunk_size_bytes: usize,
    pub chunk_size_min: usize,
    pub duplicate_policy: DuplicatePolicy,
    pub rounding: Option<RoundingStrategy>,
}

static ONE_HOUR_MILLIS: u64 = 60 * 60 * 1000;

impl Default for SeriesSettings {
    fn default() -> Self {
        Self {
            retention_period: None,
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            chunk_size_min: 0,
            duplicate_policy: DuplicatePolicy::Block,
            rounding: None,
        }
    }
}

pub static SERIES_SETTINGS: LazyLock<SeriesSettings>  = LazyLock::new(get_series_settings);