use std::sync::LazyLock;
use std::time::Duration;
use get_size::GetSize;
use crate::alerts::{
    AlertsError,
    AlertsResult,
};

use crate::common::types::Timestamp;
use crate::query::{
    create_query_context,
    run_instant_query_internal,
    run_range_query_internal,
    InstantResult,
    Querier,
    QuerierBuilder,
    QuerierParams,
    RangeResult,
    SeriesQuerier
};

use metricsql_runtime::prelude::Context as QueryContext;

pub(crate) static ALERT_QUERY_CONTEXT: LazyLock<QueryContext> = LazyLock::new(create_query_context);


/// AlertDatasource represents entity with ability to read and write metrics
#[derive(Debug, Copy, Clone, Default, GetSize)]
pub struct AlertDatasource {
    querier: SeriesQuerier
}

impl AlertDatasource {
    /// construct a RedisDatasource with default values
    pub fn new(look_back: Duration, query_step: Duration) -> Self {
        let querier: SeriesQuerier = SeriesQuerier::new(look_back, query_step);
        AlertDatasource { querier }
    }

    /// apply_params - changes given querier params.
    pub fn apply_params(mut self, params: QuerierParams) -> Self {
        self.querier = self.querier.apply_params(params);
        self
    }
}

impl Querier for AlertDatasource {
    /// executes the given query and returns an instant vector
    fn query(&self, query: &str, ts: Timestamp) -> AlertsResult<InstantResult> {
        let params = self.querier.get_instant_req_params(query.to_string(), ts);
        let query_result = run_instant_query_internal(&ALERT_QUERY_CONTEXT, &params)
            .map_err(|_e| AlertsError::QueryExecutionError(query.to_string()))?;
        Ok(InstantResult(query_result))
    }

    /// `query_range` executes the given query on the given time range.
    /// For Prometheus type see https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries
    /// Graphite type isn't supported.
    fn query_range(&self, query: &str, from: Timestamp, to: Timestamp) -> AlertsResult<RangeResult> {
        let params = self.querier.get_range_req_params(query.to_string(), from, to);
        let query_result = run_range_query_internal(&ALERT_QUERY_CONTEXT, &params)
            .map_err(|_e| AlertsError::QueryExecutionError(query.to_string()))?;
        Ok(RangeResult{ data: query_result })
    }
}

impl QuerierBuilder for AlertDatasource {
    fn build_with_params(&self, params: QuerierParams) -> Box<dyn Querier> {
        Box::new((*self).apply_params(params))
    }
}