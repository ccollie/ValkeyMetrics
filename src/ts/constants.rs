pub(crate) const U8_SIZE: usize = std::mem::size_of::<u8>();
pub(crate) const U16_SIZE: usize = std::mem::size_of::<u16>();
pub(crate) const U32_SIZE: usize = std::mem::size_of::<u32>();
pub(crate) const U64_SIZE: usize = std::mem::size_of::<u64>();
pub const BLOCK_SIZE_FOR_TIME_SERIES: usize = 4 * 1024;
pub const SPLIT_FACTOR: f64 = 1.2;
pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 4 * 1024;