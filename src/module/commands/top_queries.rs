use crate::module::arg_parse::parse_duration;
use metricsql_common::humanize::humanize_duration;
use std::collections::HashMap;
use std::time::Duration;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use crate::query::get_query_context;

const CMD_ARG_TOP_K: &str = "TOP_K";
const CMD_ARG_MAX_LIFETIME: &str = "MAX_LIFETIME";
const DEFAULT_TOP_K: usize = 20;
const ONE_DAY_IN_MS: u64 = 24 * 60 * 60 * 1000;
const DEFAULT_MAX_LIFETIME: Duration = Duration::from_secs(ONE_DAY_IN_MS);

/// https://play.victoriametrics.com/select/accounting/1/6a716b0f-38bc-4856-90ce-448fd713e3fe/prometheus/api/v1/status/top_queries
/// https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers
pub fn top_queries(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    let mut top_k = DEFAULT_TOP_K;
    let mut max_lifetime = DEFAULT_MAX_LIFETIME;

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_TOP_K) => {
                let candidate = args.next_u64()?;
                if candidate > 0 {
                    top_k = (candidate as usize).max(usize::MAX);
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_MAX_LIFETIME) => {
                let candidate = args.next_str()?;
                max_lifetime = parse_duration(candidate)?;
            }
            _ => return Err(ValkeyError::Str("ERR invalid argument")),
        }
    }

    let query_context = get_query_context();
    if !query_context.query_stats.is_enabled() {
        return Err(ValkeyError::Str("ERR query stats are disabled"));
    }
    Ok(get_query_stats(top_k, max_lifetime))
}

fn get_query_stats(
    top_n: usize,
    max_lifetime: Duration,
) -> ValkeyValue {
    let ctx = get_query_context();
    let mut res: HashMap<String, ValkeyValue> = HashMap::new();

    let last_queries_count = ctx.query_stats.get_last_queries_count();
    let duration: Duration = ctx.query_stats
        .get_min_query_duration()
        .to_std()
        .unwrap_or_else(|_| Duration::from_secs(0));

    let min_duration = humanize_duration(&duration);

    res.insert("topK".into(), ValkeyValue::from(top_n));
    res.insert("maxLifetime".into(), ValkeyValue::from(humanize_duration(&max_lifetime)));
    res.insert("lastQueriesCount".into(), ValkeyValue::from(last_queries_count));
    res.insert("minQueryDuration".into(), ValkeyValue::from(min_duration));

    let max_lifetime = chrono::Duration::from_std(max_lifetime)
        .unwrap_or_else(|_| { chrono::Duration::milliseconds(ONE_DAY_IN_MS as i64) });

    let mut items: Vec<ValkeyValue> = Vec::new();
    let top_by_count = ctx.query_stats.get_top_by_count(top_n, max_lifetime);
    for r in top_by_count.iter() {
        let mut map: HashMap<String, ValkeyValue> = HashMap::with_capacity(3);
        map.insert("query".into(), ValkeyValue::from(&r.query));
        map.insert("timeRangeSeconds".into(), r.time_range_secs.into());
        map.insert("count".into(), ValkeyValue::from(r.count as f64));
        items.push(ValkeyValue::from(map));
    }
    res.insert("topByCount".into(), ValkeyValue::from(items));


    let mut items = Vec::new();
    let top_by_avg_duration = ctx.query_stats.get_top_by_avg_duration(top_n, max_lifetime);
    for r in top_by_avg_duration.iter() {
        let mut map: HashMap<String, ValkeyValue> = HashMap::with_capacity(4);
        map.insert("query".into(), ValkeyValue::from(&r.query));
        map.insert("timeRangeSeconds".into(), r.time_range_secs.into());
        map.insert("avgDurationSeconds".into(), r.duration.num_seconds().into());
        map.insert("count".into(), ValkeyValue::from(r.count as f64));
        items.push(ValkeyValue::from(map));
    }
    res.insert("topByAvgDuration".into(), ValkeyValue::from(items));

    let mut items = Vec::new();
    let top_by_sum_duration = ctx.query_stats.get_top_by_sum_duration(top_n, max_lifetime);
    for r in top_by_sum_duration.iter() {
        let mut map: HashMap<String, ValkeyValue> = HashMap::with_capacity(4);
        map.insert("query".into(), ValkeyValue::from(&r.query));
        map.insert("timeRangeSeconds".into(), r.time_range_secs.into());
        map.insert("sumDurationSeconds".into(), r.duration.num_seconds().into());
        map.insert("count".into(), ValkeyValue::from(r.count as f64));
        items.push(ValkeyValue::from(map));
    }
    res.insert("topBySumDuration".into(), ValkeyValue::from(items));

    res.into()
}