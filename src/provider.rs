use crate::ts::time_series::TimeSeries;
use crate::ts::Timestamp;
use metricsql_engine::provider::MetricDataProvider;
use metricsql_engine::{
    Deadline, MetricName, QueryResult, QueryResults, RuntimeResult, SearchQuery,
};
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use redis_module::Context;
use crate::globals::get_timeseries_index;
use crate::index::TimeSeriesIndex;

pub struct TsdbDataProvider {}

impl TsdbDataProvider {
    fn get_series_data(
        &self,
        ctx: &Context,
        index: &TimeSeriesIndex,
        search_query: &SearchQuery,
    ) -> Vec<QueryResult> {
        index
            .series_by_matchers(
                ctx,
                &search_query.matchers,
                search_query.start,
                search_query.end,
            )
            .par_iter()
            .map(|ts| {
                let mut timestamps: Vec<Timestamp> = Vec::new();
                let mut values: Vec<f64> = Vec::new();
                let res = ts.get_range_raw(
                    search_query.start,
                    search_query.end,
                    &mut timestamps,
                    &mut values,
                );
                // what do wee do in case of error ?
                let metric = to_metric_name(ts);
                QueryResult::new(metric, timestamps, values)
            })
            .collect()
    }
}

impl MetricDataProvider for TsdbDataProvider {
    fn search(&self, sq: &SearchQuery, _deadline: &Deadline) -> RuntimeResult<QueryResults> {
        // see: https://github.com/RedisLabsModules/redismodule-rs/blob/master/examples/call.rs#L144
        let ctx_guard = redis_module::MODULE_CONTEXT.lock();
        let index = get_timeseries_index();

        let data = self.get_series_data(&ctx_guard, index, sq);
        let result = QueryResults::new(data);
        Ok(result)
    }
}

fn to_metric_name(ts: &TimeSeries) -> MetricName {
    let mut mn = MetricName::new(&ts.metric_name);
    for (k, v) in ts.labels.iter() {
        mn.add_tag(k, v);
    }
    mn
}
