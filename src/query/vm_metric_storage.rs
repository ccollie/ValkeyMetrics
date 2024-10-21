use crate::common::types::{Sample, Timestamp};
use crate::globals::with_timeseries_index;
use crate::module::VKM_SERIES_TYPE;
use crate::series::index::TimeSeriesIndex;
use crate::series::time_series::TimeSeries;
use async_trait::async_trait;
use metricsql_runtime::prelude::{Deadline, MetricStorage, QueryResult, QueryResults, RuntimeResult, SearchQuery};
use metricsql_runtime::types::MetricName;
use valkey_module::{Context, ValkeyString};

/// Interface between the time series database and the metricsql runtime.
pub(super) struct VMMetricStorage {}

impl VMMetricStorage {

    fn get_series(&self, ctx: &Context, key: &ValkeyString, start_ts: Timestamp, end_ts: Timestamp) -> RuntimeResult<Option<QueryResult>> {
        let valkey_key = ctx.open_key(key);
        match valkey_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE) {
            Ok(Some(series)) => {
                let metric = super::to_metric_name(series);
                let samples = series.get_range(start_ts, end_ts);

                let count = samples.len();
                let (mut timestamps, mut values) = if count > 0 {
                    (Vec::with_capacity(count), Vec::with_capacity(count))
                } else {
                    (vec![], vec![])
                };

                for Sample { timestamp, value } in samples {
                    timestamps.push(timestamp);
                    values.push(value);
                }

                Ok(Some(QueryResult::new(metric, timestamps, values)))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                ctx.log_warning(&format!("ERR: {:?}", e));
                // TODO return a proper error message. For nowm, return an empty data set
                Ok(Some(QueryResult::new(MetricName::default(), vec![], vec![])))
            }
        }
    }

    fn get_series_data(
        &self,
        ctx: &Context,
        index: &TimeSeriesIndex,
        search_query: SearchQuery,
    ) -> RuntimeResult<Vec<QueryResult>> {
        let map = index.series_keys_by_matchers(ctx, &[search_query.matchers]);
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
