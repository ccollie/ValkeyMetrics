use std::collections::HashMap;
use std::ops::Add;
use std::time::Duration;

use crate::globals::get_query_context;
use metricsql_engine::prelude::query::QueryParams;
use metricsql_engine::TimestampTrait;
use metricsql_engine::execution::query::{
    query as engine_query, query_range as engine_query_range,
};
use crate::rules::alerts::{AlertsError, AlertsResult, DataSourceType, Querier, QuerierBuilder, QuerierParams, QueryResult};
use crate::ts::Timestamp;

/// RedisDatasource represents entity with ability to read and write metrics
#[derive(Clone, Debug)]
pub struct RedisDatasource {
    /// look_back defines how far to look into past for alerts timeseries.
    /// For example, if look_back=1h then range from now() to now()-1h will be scanned.
    look_back: Duration,
    query_step: Duration,
    data_source_type: DataSourceType,

    /// Whether to align "time" parameter with evaluation interval.Alignment supposed to produce deterministic
    /// results despite number of vmalert replicas or time they were started. See more details here
    /// https://github.com/VictoriaMetrics/VictoriaMetrics/pull/1257 (default true)
    query_time_alignment: bool,
    /// evaluation_interval will align the request's timestamp if `provider.QUERY_TIME_ALIGNMENT`
    /// is enabled, will set request's `step` param as well.
    evaluation_interval: Duration,
    /// evaluation_offset shifts the request's timestamp, will be equal to the offset specified
    /// evaluation_interval.
    /// See https://github.com/VictoriaMetrics/VictoriaMetrics/pull/4693
    evaluation_offset: Duration,
    /// extra_params contains params to be attached to each HTTP request
    extra_params: HashMap<String, String>,
    /// extra_headers are headers to be attached to each HTTP request
    extra_headers: HashMap<String, String>,
    /// whether to print additional log messages for each sent request
    debug: bool,
}

impl RedisDatasource {
    /// construct a RedisDatasource with default values
    pub fn new(look_back: Duration, query_step: Duration) -> Self {
        return RedisDatasource {
            look_back,
            query_step,
            data_source_type: DataSourceType::Redis,
            query_time_alignment: true,
            evaluation_interval: Default::default(),
            evaluation_offset: Default::default(),
            extra_params: Default::default(),
            extra_headers: Default::default(),
            debug: false,
        };
    }

    /// apply_params - changes given querier params.
    fn apply_params(mut self, params: QuerierParams) -> Self {
        self.data_source_type = params.data_source_type;
        self.evaluation_interval = params.evaluation_interval;
        self.evaluation_offset = params.eval_offset;
        if !params.query_params.is_empty() {
            for (k, vl) in params.query_params {
                // custom query params are prior to default ones
                self.extra_params.remove(&k);
                for v in vl {
                    // don't use .set() instead of del/add since it is allowed for GET params to be duplicated
                    // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4908
                    self.extra_params.insert(k.to_string(), v)
                }
            }
        }
        for (key, value) in params.headers {
            self.extra_headers.insert(key, value)
        }
        self.debug = params.debug;
        return self;
    }

    fn get_instant_req_params(&self, query: String, timestamp: Timestamp) -> QueryParams {
        let timestamp = self.adjust_req_timestamp(timestamp);
        let mut params = QueryParams::new(query, timestamp);
        if !self.evaluation_interval.is_zero() {
            // set step as evaluation_interval by default always convert to seconds to keep
            // compatibility with older Prometheus versions. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1943
            // -- eliminate this unwrap
            let step = duration_to_chrono(&self.evaluation_interval);
            params.step = Some(step);
        }
        if !self.query_step.is_zero() {
            // override step with user-specified value
            // always convert to seconds to keep compatibility with older
            // Prometheus versions. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1943
            let step = duration_to_chrono(&self.query_step);
            params.step = Some(step);
        }
        params
    }

    fn get_range_req_params(&self, query: String, start: Timestamp, end: Timestamp) -> QueryParams {
        let mut start = start;
        if !self.evaluation_offset.is_zero() {
            start = start
                .truncate(self.evaluation_interval)
                .add(&self.evaluation_offset);
        }
        let mut params = QueryParams::new(query, start);
        params.end = end;
        if !self.evaluation_interval.is_zero() {
            // set step as evaluationInterval by default
            // always convert to seconds to keep compatibility with older
            // Prometheus versions. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1943
            let step = duration_to_chrono(&self.evaluation_interval);
            params.step = Some(step);
        }
        params
    }

    fn adjust_req_timestamp(&self, timestamp: Timestamp) -> Timestamp {
        let mut timestamp = timestamp;
        if self.evaluation_offset.is_zero() {
            // calculate the min timestamp on the evaluationInterval
            let interval_start = timestamp.truncate(self.evaluation_interval);
            let ts = interval_start.add(&self.evaluation_offset);
            if timestamp < ts {
                // if passed timestamp is before the expected evaluation offset,
                // then we should adjust it to the previous evaluation round.
                // E.g. request with evaluationInterval=1h and evaluationOffset=30m
                // was evaluated at 11:20. Then the timestamp should be adjusted
                // to 10:30, to the previous evaluationInterval.
                return ts.add(-self.evaluation_interval);
            }
            // evaluationOffset shouldn't interfere with QUERY_TIME_ALIGNMENT or lookBack,
            // so we return it immediately
            return ts;
        }
        if self.query_time_alignment {
            // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1232
            timestamp = timestamp.truncate(self.evaluation_interval);
        }
        if self.look_back.as_millis() > 0 {
            timestamp = timestamp.add(-self.look_back)
        }

        timestamp
    }
}

impl Querier for RedisDatasource {
    /// Query executes the given query and returns parsed response
    fn query(&self, query: &str, ts: Timestamp) -> AlertsResult<QueryResult> {
        let query_context = get_query_context();
        let params = self.get_instant_req_params(query.to_string(), ts);
        let query_result = engine_query(query_context, &params)
            .map_err(|e| AlertsError::QueryExecutionError(e.into()))?;

        todo!()
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

impl QuerierBuilder for RedisDatasource {
    fn build_with_params(&self, params: QuerierParams) -> Box<dyn Querier> {
        let querier = self.clone().apply_params(params);
        Box::new(querier)
    }
}

fn duration_to_chrono(duration: &Duration) -> chrono::Duration {
    return chrono::Duration::from_std(*duration).unwrap();
}
