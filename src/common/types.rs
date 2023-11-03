
pub trait AToAny: 'static {
    fn as_any(&self) -> &dyn Any;
}

impl<T: 'static> AToAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}


pub type Timestamp = metricsql_runtime::prelude::Timestamp;
pub type PooledTimestampVec = metricsql_common::pool::PooledVecI64;
pub type PooledValuesVec = metricsql_common::pool::PooledVecF64;
pub const MAX_TIMESTAMP: i64 = 253402300799;

