use std::sync::{Arc, LazyLock};
use metricsql_runtime::execution::query::{
    query as base_query, query_range as base_query_range,
};
use metricsql_runtime::prelude::{Context as QueryContext, MetricName};
use metricsql_runtime::prelude::query::QueryParams;
use metricsql_runtime::{QueryResult, RuntimeResult};
use crate::common::types::Label;
use crate::query::vm_metric_storage::VMMetricStorage;
use crate::series::TimeSeries;

mod vm_metric_storage;
cfg_if::cfg_if! {
    if #[cfg(test)] {
        mod bench_test;
        mod test_metric_storage;
        mod test_utils;
        mod query_tests;

        pub(super) use test_metric_storage::*;
    }
}

pub(crate) static QUERY_CONTEXT: LazyLock<QueryContext> = LazyLock::new(create_query_context);

pub fn get_query_context() -> &'static QueryContext {
    &QUERY_CONTEXT
}

fn create_query_context() -> QueryContext {
    // todo: read settings from config
    let provider = Arc::new(VMMetricStorage {});
    let ctx = QueryContext::new();
    ctx.with_metric_storage(provider)
}


pub(crate) fn query(query_params: &QueryParams) -> RuntimeResult<Vec<QueryResult>> {
    // todo: should be Vec<Sample>
    base_query(&QUERY_CONTEXT, query_params)
}

pub(crate) fn query_range(query_params: &QueryParams) -> RuntimeResult<Vec<QueryResult>> {
    let query_context = get_query_context();
    base_query_range(query_context, &query_params)
}

pub(super) fn to_metric_name(ts: &TimeSeries) -> MetricName {
    let mut mn = MetricName::new(&ts.metric_name);
    for Label { name, value } in ts.labels.iter() {
        mn.add_label(name, value);
    }
    mn
}
