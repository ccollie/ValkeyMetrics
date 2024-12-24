use crate::common::types::{Sample, Timestamp};
use crate::module::VKM_SERIES_TYPE;
use crate::series::index::{with_timeseries_index, TimeSeriesIndex};
use crate::series::time_series::TimeSeries;
use async_trait::async_trait;
use metricsql_runtime::prelude::{Deadline, MetricStorage, QueryResult, QueryResults, RuntimeResult, SearchQuery};
use metricsql_runtime::RuntimeError;
use metricsql_runtime::types::MetricName;
use valkey_module::{Context, ValkeyString};

/// Interface between the time series database and the metricsql runtime.
pub struct VMMetricStorage {}

impl VMMetricStorage {

    fn get_range(series: &TimeSeries, start_ts: Timestamp, end_ts: Timestamp) -> Option<QueryResult> {
        let samples = series.get_range(start_ts, end_ts);
        if samples.is_empty() {
            return None;
        }
        let metric = super::to_metric_name(series);
        let count = samples.len();
        let mut timestamps = Vec::with_capacity(count);
        let mut values = Vec::with_capacity(count);

        for Sample { timestamp, value } in samples {
            timestamps.push(timestamp);
            values.push(value);
        }

        Some(QueryResult::new(metric, timestamps, values))
    }

    fn get_series(&self, ctx: &Context, key: &ValkeyString, start_ts: Timestamp, end_ts: Timestamp) -> RuntimeResult<Option<QueryResult>> {
        let valkey_key = ctx.open_key(key);
        match valkey_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE) {
            Ok(Some(series)) => {
                Ok(Self::get_range(series, start_ts, end_ts))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                ctx.log_warning(&format!("ERR: {:?}", e));
                // TODO return a proper error message. For nowm, return an empty data set
                Ok(Some(QueryResult::new(MetricName::default(), vec![], vec![])))
            }
        }
    }

    fn _get_series_internal(scope: &mut chili::Scope,
                            series: &[TimeSeries],
                            start_ts: Timestamp,
                            end_ts: Timestamp) -> Vec<QueryResult> {

        match series {
            [] => Vec::new(),
            [series] => {
                if let Some(r) = Self::get_range(series, start_ts, end_ts) {
                    vec![r]
                } else {
                    Vec::new()
                }
            }
            [s1, s2] => {
                let (r1, r2) = scope.join(
                    |_| Self::get_range(s1, start_ts, end_ts),
                    |_| Self::get_range(s2, start_ts, end_ts)
                );
                match (r1, r2) {
                    (Some(r1), Some(r2)) => {
                        vec![r1, r2]
                    }
                    (Some(r1), None) => {
                        vec![r1]
                    }
                    (None, Some(r2)) => {
                        vec![r2]
                    }
                    _ => Vec::new()
                }
            }
            _ => {
                let mid = series.len() / 2;
                let (left, right) = series.split_at(mid);
                let (mut left_results, right_results) = scope.join(
                    |s1| Self::_get_series_internal(s1, left, start_ts, end_ts),
                    |s2| Self::_get_series_internal(s2, right, start_ts, end_ts)
                );
                left_results.extend(right_results);
                left_results
            }
        }
    }

    fn get_series_data(
        &self,
        ctx: &Context,
        index: &TimeSeriesIndex,
        search_query: SearchQuery,
    ) -> RuntimeResult<Vec<QueryResult>> {
        let map = index.series_keys_by_matchers(ctx, &search_query.matchers)
            .map_err(|e| {
                ctx.log_warning(&format!("ERR: {:?}", e));
                // TODO. 1. on the lib side, use a better enum variant
                RuntimeError::General("Error getting series keys".to_string())
            })?;
        let mut results: Vec<QueryResult> = Vec::with_capacity(map.len());
        let start_ts = search_query.start;
        let end_ts = search_query.end;

        // use rayon ?
        for key in map.iter() {
            if let Some(result) = self.get_series(ctx, key, start_ts, end_ts)? {
                results.push(result);
            }
        }
        Ok(results)
    }
}

#[async_trait]
impl MetricStorage for VMMetricStorage {
    async fn search(&self, sq: SearchQuery, _deadline: Deadline) -> RuntimeResult<QueryResults> {
        // see: https://github.com/RedisLabsModules/redismodule-rs/blob/master/examples/call.rs#L144
        let ctx_guard = valkey_module::MODULE_CONTEXT.lock();
        with_timeseries_index(&ctx_guard, |index| {
            let data = self.get_series_data(&ctx_guard, index, sq)?;
            let result = QueryResults::new(data);
            Ok(result)
        })
    }
}
