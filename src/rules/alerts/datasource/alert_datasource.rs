use std::ops::Add;
use std::time::Duration;

use crate::globals::get_query_context;
use crate::rules::alerts::{
    AlertsError,
    AlertsResult,
    Querier,
    QuerierBuilder,
    QuerierParams,
    QueryResult,
};
use crate::rules::Metric;
use crate::storage::Timestamp;
use metricsql_runtime::execution::query::{
    query as engine_query, query_range as engine_query_range,
};
use metricsql_runtime::prelude::query::QueryParams;
use metricsql_runtime::types::TimestampTrait;

/// AlertDatasource represents entity with ability to read and write metrics
#[derive(Clone, Debug)]
pub struct AlertDatasource {
    /// look_back defines how far to look into past for alerts timeseries.
    /// For example, if look_back=1h then range from now() to now()-1h will be scanned.
    look_back: Duration,
    query_step: Duration,

    /// Whether to align "time" parameter with evaluation interval. Alignment is supposed to produce deterministic
    /// results despite number of replicas or time they were started.
    query_time_alignment: bool,
    /// evaluation_interval will align the request's timestamp if `provider.QUERY_TIME_ALIGNMENT`
    /// is enabled, will set request's `step` param as well.
    evaluation_interval: Duration,
    /// evaluation_offset shifts the request's timestamp, will be equal to the offset specified
    /// evaluation_interval.
    /// See https://github.com/VictoriaMetrics/VictoriaMetrics/pull/4693
    evaluation_offset: Duration,
    /// whether to print additional log messages for each sent request
    debug: bool,
}

impl AlertDatasource {
    /// construct a RedisDatasource with default values
    pub fn new(look_back: Duration, query_step: Duration) -> Self {
        AlertDatasource {
            look_back,
            query_step,
            query_time_alignment: true,
            evaluation_interval: Default::default(),
            evaluation_offset: Default::default(),
            debug: false,
        }
    }

    /// apply_params - changes given querier params.
    fn apply_params(mut self, params: QuerierParams) -> Self {
        self.evaluation_interval = params.evaluation_interval;
        self.evaluation_offset = params.eval_offset;
        self.debug = params.debug;
        self
    }

    fn get_instant_req_params(&self, query: String, timestamp: Timestamp) -> QueryParams {
        let timestamp = self.adjust_req_timestamp(timestamp);
        let mut params = QueryParams::default();
        params.query = query;
        params.start = timestamp;
        params.end = timestamp;

        if !self.evaluation_interval.is_zero() {
            // set step as evaluation_interval by default always convert to seconds to keep
            // compatibility with older Prometheus versions. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1943
            params.step = duration_to_chrono(&self.evaluation_interval);
        }
        if !self.query_step.is_zero() {
            // override step with user-specified value
            // always convert to seconds to keep compatibility with older
            // Prometheus versions. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1943
            params.step = duration_to_chrono(&self.query_step);
        }
        params
    }

    fn get_range_req_params(&self, query: String, start: Timestamp, end: Timestamp) -> QueryParams {
        let mut start = start;
        if !self.evaluation_offset.is_zero() {
            let offset = self.evaluation_offset.as_millis() as i64; // todo: check for overflow
            start = start
                .truncate(self.evaluation_interval)
                .add(offset);
        }

        let mut params = QueryParams::default();
        params.query = query;
        params.start = start;
        params.end = end;

        if !self.evaluation_interval.is_zero() {
            // set step as evaluationInterval by default
            // always convert to seconds to keep compatibility with older
            // Prometheus versions. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1943
            params.step = duration_to_chrono(&self.evaluation_interval);
        }
        params
    }

    fn adjust_req_timestamp(&self, timestamp: Timestamp) -> Timestamp {
        let mut timestamp = timestamp;
        if self.evaluation_offset.is_zero() {
            let eval_interval = self.evaluation_interval.as_millis() as i64; // todo: check for overflow
            let evaluation_offset = self.evaluation_offset.as_millis() as i64; // todo: check for overflow

            // calculate the min timestamp on the evaluationInterval
            let interval_start = timestamp.truncate(self.evaluation_interval);
            let ts = interval_start.saturating_add(evaluation_offset);
            if timestamp < ts {
                // if passed timestamp is before the expected evaluation offset,
                // then we should adjust it to the previous evaluation round.
                // E.g. request with evaluationInterval=1h and evaluationOffset=30m
                // was evaluated at 11:20. Then the timestamp should be adjusted
                // to 10:30, to the previous evaluationInterval.
                return ts.saturating_add(eval_interval);
            }
            // evaluationOffset shouldn't interfere with QUERY_TIME_ALIGNMENT or lookBack,
            // so we return it immediately
            return ts;
        }
        if self.query_time_alignment {
            // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1232
            timestamp = timestamp.truncate(self.evaluation_interval);
        }
        if !self.look_back.is_zero() {
            let look_back = self.look_back.as_millis() as i64; // todo: check for overflow
            timestamp = timestamp.saturating_sub(look_back)
        }

        timestamp
    }
}

impl Querier for AlertDatasource {
    /// Query executes the given query and returns parsed response
    fn query(&self, query: &str, ts: Timestamp) -> AlertsResult<QueryResult> {
        let query_context = get_query_context();
        let params = self.get_instant_req_params(query.to_string(), ts);
        let query_result = engine_query(query_context, &params)
            .map_err(|e| AlertsError::QueryExecutionError(e.into()))?;

        let res = query_result.into_iter()
            .map(|r| {
                let mut metric = Metric::new();

                QueryResult {
                    data: vec![],
                    series_fetched: 0,
                }
            })
            .collect();
        Ok(res)
    }

    /// query_range executes the given query on the given time range.
    /// For Prometheus type see https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries
    /// Graphite type isn't supported.
    fn query_range(
        &self,
        query: &str,
        from: Timestamp,
        to: Timestamp,
    ) -> AlertsResult<QueryResult> {
        let query_context = get_query_context();
        let params = self.get_range_req_params(query.to_string(), from, to);
        let query_result = engine_query_range(query_context, &params)
            .map_err(|e| AlertsError::QueryExecutionError(e.into()))?;
        todo!()
    }
}

impl QuerierBuilder for AlertDatasource {
    fn build_with_params(&self, params: QuerierParams) -> Box<dyn Querier> {
        let querier = self.clone().apply_params(params);
        Box::new(querier)
    }
}

fn duration_to_chrono(duration: &Duration) -> chrono::Duration {
    chrono::Duration::from_std(*duration).unwrap()
}

fn query_result_to_metric(result: QueryResult) -> Vec<Metric> {
    let mut metrics = Vec::new();
    for r in result.data {
        let metric = Metric {
            key: "".to_string(),
            labels: vec![],
            timestamps: r.timestamps,
            values: r.values,
        };
        metrics.push(metric);
    }
    metrics
}