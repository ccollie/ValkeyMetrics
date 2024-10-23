use crate::common::types::{Label, Sample};
use crate::query::vm_metric_storage::VMMetricStorage;
use crate::series::TimeSeries;
use metricsql_common::async_runtime::block_on;
use metricsql_runtime::execution::query::{query, query_range};
use metricsql_runtime::prelude::{Context as QueryContext, MetricName};
use metricsql_runtime::RuntimeError;
use std::sync::{Arc, LazyLock};
use valkey_module::{ValkeyError, ValkeyResult};
pub use common::types::QueryParams;

use crate::common;

pub struct InstantQueryResult {
    pub metric: MetricName,
    pub sample: Sample
}

pub struct RangeQueryResult {
    pub metric: MetricName,
    pub samples: Vec<Sample>
}

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

fn map_error(err: RuntimeError) -> ValkeyError {
    let err_msg = format!("ERR: query execution error: {:?}", err);
    // todo: log errors
    ValkeyError::String(err_msg.to_string())
}

pub(crate) fn run_instant_query(params: &QueryParams) -> ValkeyResult<Vec<InstantQueryResult>> {
    let results = block_on(async move {
        match query(&QUERY_CONTEXT, params) {
            Ok(samples) => Ok(samples),
            Err(e) => Err(map_error(e))
        }
    })?;
    Ok(
        results.into_iter()
        .map(|result| {
            // if this panics, we have problems in the base library
            let sample = Sample {
                timestamp: result.timestamps[0],
                value: result.values[0]
            };
            InstantQueryResult {
                metric: result.metric,
                sample
            }
        }).collect()
    )
}

pub(crate) fn run_range_query(params: &QueryParams) -> ValkeyResult<Vec<RangeQueryResult>> {
    let results = block_on(async move {
        match query_range(&QUERY_CONTEXT, params) {
            Ok(samples) => Ok(samples),
            Err(e) => Err(map_error(e))
        }
    })?;
    Ok(
        results.into_iter().map(|result| {
            let samples = result.timestamps.iter()
                .zip(result.values.iter())
                .map(|(&ts, &value)| Sample { timestamp: ts, value })
                .collect();
            RangeQueryResult {
                metric: result.metric,
                samples,
            }
        }).collect()
    )
}


pub(super) fn to_metric_name(ts: &TimeSeries) -> MetricName {
    let mut mn = MetricName::new(&ts.metric_name);
    for Label { name, value } in ts.labels.iter() {
        mn.add_label(name, value);
    }
    mn
}
