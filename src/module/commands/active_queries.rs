use crate::globals::get_query_context;
use metricsql_runtime::types::{Timestamp, TimestampTrait};
use std::collections::HashMap;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// Returns currently running queries.
pub fn active_queries(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    // signal not more args
    if args.next().is_some() {
        return Err(ValkeyError::Str("ERR invalid argument"));
    }

    Ok(get_active_queries())
}

fn get_active_queries() -> ValkeyValue {
    let ctx = get_query_context();
    let active_queries = ctx.get_active_queries();

    let mut items: Vec<ValkeyValue> = Vec::new();
    let now = Timestamp::now();

    for aqe in active_queries.iter() {
        let duration = now - aqe.start_time;
        let duration_secs = duration / 1000;
        let mut map: HashMap<String, ValkeyValue> = HashMap::new();
        map.insert("duration".into(), ValkeyValue::from(duration));
        map.insert("duration_secs".into(), ValkeyValue::from(duration_secs));
        map.insert("id".into(), ValkeyValue::from(aqe.qid as f64));
        map.insert("remote_addr".into(), ValkeyValue::from(&aqe.quoted_remote_addr));
        map.insert("query".into(), ValkeyValue::from(&aqe.q));
        map.insert("start".into(), ValkeyValue::from(aqe.start));
        map.insert("end".into(), ValkeyValue::from(aqe.end));
        map.insert("step".into(), ValkeyValue::from(aqe.step));
        items.push(ValkeyValue::from(map));
    }

    items.into()
}