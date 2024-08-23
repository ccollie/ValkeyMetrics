use std::mem::size_of;

pub(crate) const I64_SIZE: usize = size_of::<i64>();
pub(crate) const F64_SIZE: usize = size_of::<f64>();
pub(crate) const VEC_BASE_SIZE: usize = 24;
pub const BLOCK_SIZE_FOR_TIME_SERIES: usize = 4 * 1024;
pub const SPLIT_FACTOR: f64 = 1.2;
pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 4 * 1024;
pub const MAX_TIMESTAMP: i64 = 253402300799;
pub const METRIC_NAME_LABEL: &str = "__name__";