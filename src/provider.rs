use crate::common::types::{Label, Timestamp};
use crate::globals::with_timeseries_index;
use crate::series::index::TimeSeriesIndex;
use crate::module::VKM_SERIES_TYPE;
use crate::series::time_series::TimeSeries;
use async_trait::async_trait;
use metricsql_runtime::prelude::{Deadline, MetricStorage, QueryResult, QueryResults, RuntimeError, RuntimeResult, SearchQuery};
use metricsql_runtime::types::MetricName;
use valkey_module::{Context, ValkeyString};

pub struct TsdbDataProvider {}

impl TsdbDataProvider {

    fn get_series(&self, ctx: &Context, key: &ValkeyString, start_ts: Timestamp, end_ts: Timestamp) -> RuntimeResult<Option<QueryResult>> {
        let valkey_key = ctx.open_key(key);
        match valkey_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE) {
            Ok(Some(series)) => {
                if series.overlaps(start_ts, end_ts) {
                    let mut timestamps: Vec<Timestamp> = Vec::new();
                    let mut values: Vec<f64> = Vec::new();
                    return match series.select_raw(
                        start_ts,
                        end_ts,
                        &mut timestamps,
                        &mut values,
                    ) {
                        Ok(_) => {
                            // what do wee do in case of error ?
                            let metric = to_metric_name(series);
                            Ok(Some(QueryResult::new(metric, timestamps, values)))
                        }
                        Err(e) => {
                            ctx.log_warning(&format!("PROMQL: Error: {:?}", e));
                            // TODO!: we need a specific error for series backends
                            Err(RuntimeError::General("Error".to_string()))
                        }
                    }
                }
            }
            Err(e) => {
                ctx.log_warning(&format!("PROMQL: Error: {:?}", e));
            }
            _ => {}
        }
        Ok(None)
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
impl MetricStorage for TsdbDataProvider {
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

fn to_metric_name(ts: &TimeSeries) -> MetricName {
    let mut mn = MetricName::new(&ts.metric_name);
    for Label { name, value } in ts.labels.iter() {
        mn.add_label(name, value);
    }
    mn
}
