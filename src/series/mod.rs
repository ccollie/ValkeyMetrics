use std::mem::size_of;
mod constants;
pub mod time_series;
pub(crate) mod utils;
mod defrag;
pub mod index;
pub mod chunks;
pub mod types;
mod merge;
mod timestamp_range;

use crate::common::types::Sample;
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
        pub mod test_utils;
    }
}

pub const SAMPLE_SIZE: usize = size_of::<Sample>();