pub use common::types::QueryParams;
use metricsql_runtime::prelude::Context as QueryContext;
use std::sync::{Arc, LazyLock};

use crate::common;

mod vm_metric_storage;
pub mod datasource;
mod series_querier;
mod handlers;
mod tracing;

cfg_if::cfg_if! {
    if #[cfg(test)] {
        mod bench_test;
        mod test_metric_storage;
        mod test_utils;
        mod query_tests;

        pub(super) use test_metric_storage::*;
    } else {
        pub use vm_metric_storage::VMMetricStorage;
    }
}

pub(crate) static QUERY_CONTEXT: LazyLock<QueryContext> = LazyLock::new(create_query_context);

pub fn get_query_context() -> &'static QueryContext {
    &QUERY_CONTEXT
}

pub(super) fn create_query_context() -> QueryContext {
    // todo: read settings from config
    #[cfg(test)]
    let provider = Arc::new(TestMetricStorage::new());
    #[cfg(not(test))]
    let provider = Arc::new(VMMetricStorage {});
    let ctx = QueryContext::new();
    ctx.with_metric_storage(provider)
}

pub use datasource::*;
pub(crate) use handlers::*;
pub use series_querier::*;
