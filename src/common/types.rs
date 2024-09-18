
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
pub type Sample = metricsql_runtime::types::Sample;
pub type Label = metricsql_runtime::types::Label;

