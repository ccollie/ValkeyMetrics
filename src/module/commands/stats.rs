use crate::common::METRIC_NAME_LABEL;
use crate::globals::with_timeseries_index;
use crate::index::IndexInner;
use crate::module::arg_parse::parse_integer_arg;
use std::collections::HashMap;
use std::sync::RwLockReadGuard;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const DEFAULT_LIMIT: usize = 10;

/// If limit is above of equal this threshold, we run the constituent functions in parallel.
const LIMIT_PARALLELISM_THRESHOLD: usize = 1000;

const SENTINEL_VALUE: &str = "\u{E000}";

/// https://prometheus.io/docs/prometheus/latest/querying/api/#tsdb-stats
pub fn stats(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let limit = match args.len() {
        0 => DEFAULT_LIMIT,
        1 => {
            let next = parse_integer_arg(&args[0], "limit", false)?;
            if next > usize::MAX as i64 {
                return Err(ValkeyError::Str("ERR LIMIT too large"));
            } else if next == 0 {
                return Err(ValkeyError::Str("ERR LIMIT must be greater than 0"));
            }
            next as usize
        },
        _ => {
            return Err(ValkeyError::WrongArity);
        },
    };

    with_timeseries_index(ctx, |index| {
        let inner = index.get_inner();

        let series_count = inner.label_kv_to_ts.len();

        let (series_count_by_metric_name,
            label_value_count_by_label_name,
            series_count_by_label_pairs,
            memory_in_bytes_by_label_name) = if series_count == 0 {
            (ValkeyValue::Array(vec![]), ValkeyValue::Array(vec![]), ValkeyValue::Array(vec![]), ValkeyValue::Array(vec![]))
        }
        // else if limit >= LIMIT_PARALLELISM_THRESHOLD && series_count >= LIMIT_PARALLELISM_THRESHOLD {
        //     // rayon really needs an nary join!
        //     let (series_count, label_value_counts) = rayon::join(
        //         || get_series_count_by_metric_name(&inner, limit),
        //         || get_label_value_count_by_label_name(&inner, limit));
        //     let (label_pair_count, memory_bytes) = rayon::join(
        //         || get_series_count_by_label_pair(&inner, limit),
        //         || get_memory_in_bytes_by_label_pair(&inner, limit));
        //
        //     (series_count, label_value_counts, label_pair_count, memory_bytes)
        else {
            (
                get_series_count_by_metric_name(&inner, limit),
                get_label_value_count_by_label_name(&inner, limit),
                get_series_count_by_label_pair(&inner, limit),
                get_memory_in_bytes_by_label_pair(&inner, limit),
            )
        };

        let mut data = HashMap::with_capacity(4);
        data.insert("numSeries".into(), series_count.into());
        data.insert("seriesCountByMetricName".into(), series_count_by_metric_name);
        data.insert("labelValueCountByLabelName".into(), label_value_count_by_label_name);
        data.insert("memoryInBytesByLabelPair".into(), memory_in_bytes_by_label_name);
        data.insert("seriesCountByLabelPair".into(), series_count_by_label_pairs);

        let mut res = HashMap::new();
        res.insert("status".into(), "success".into());
        res.insert("data".into(), ValkeyValue::Map(data));
        Ok(ValkeyValue::Map(res))
    })
}

fn append_key_value(map: &mut Vec<ValkeyValue>, key: &str, value: ValkeyValue) {
    let mut res: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(1);
    res.insert(key.into(), value);
    map.push(ValkeyValue::Map(res));
}


fn get_series_count_by_metric_name(inner: &RwLockReadGuard<IndexInner>, limit: usize) -> ValkeyValue {
    let prefix = format!("{METRIC_NAME_LABEL}=");
    let items: Vec<_> = inner.label_kv_to_ts
        .range(prefix..)
        .filter_map(|(key,  map)| {
            if let Some((_, value)) = key.split_once('=') {
                Some((value, map.cardinality() as usize)) // todo: can this overflow?
            } else {
                None
            }
        })
        .take(limit)
        .collect();

    let arr: Vec<ValkeyValue> = items.into_iter().map(|(name, count)| {
        let mut res: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(1);
        res.insert(name.into(), count.into());
        ValkeyValue::Map(res)
    }).collect();
    ValkeyValue::Array(arr)
}

fn get_label_value_count_by_label_name(inner: &RwLockReadGuard<IndexInner>, limit: usize) -> ValkeyValue {
    let items = inner.label_kv_to_ts
        .iter()
        .filter_map(|(key,  _)| {
            if let Some((name, _)) = key.split_once('=') {
                Some(name)
            } else {
                None
            }
        });

    let capacity = if limit < 100 { limit } else { 10 }; // ??
    let mut current = SENTINEL_VALUE;
    let mut count: usize = 0;
    let mut arr: Vec<ValkeyValue> = Vec::with_capacity(capacity);

    for name in items {
        count += 1;
        if current != name && current != SENTINEL_VALUE {
            append_key_value(&mut arr, current, count.into());
            if arr.len() >= limit {
                break;
            }
            current = name;
            count = 0;
        }
    }

    ValkeyValue::Array(arr)
}

fn get_series_count_by_label_pair(inner: &RwLockReadGuard<IndexInner>, limit: usize) -> ValkeyValue {
    let items: Vec<_> = inner.label_kv_to_ts
        .iter()
        .filter_map(|(key,  map)| {
            if let Some((name, value)) = key.split_once('=') {
                Some((ValkeyValueKey::from(format!("{}={}", name, value)), map.cardinality() as usize))
            } else {
                None
            }
        })
        .take(limit)
        .collect();

    let arr: Vec<ValkeyValue> = items.into_iter().map(|(name, count)| {
        let mut res: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(1);
        res.insert(name, count.into());
        ValkeyValue::Map(res)
    }).collect();
    ValkeyValue::Array(arr)
}

fn get_memory_in_bytes_by_label_pair(inner: &RwLockReadGuard<IndexInner>, limit: usize) -> ValkeyValue {
    let items = inner.label_kv_to_ts
        .iter()
        .filter_map(|(key,  map)| {
            if let Some((name, value)) = key.split_once('=') {
                // since we currently also store labels with the timeseries, we need to account for that
                // hopefully we can use an interner to reduce this overhead
                let series_count = map.cardinality() as usize;
                Some((name, value.len() * (1 + series_count)))
            } else {
                None
            }
        });

    let capacity = if limit < 100 { limit } else { 10 }; // ??
    let mut current = SENTINEL_VALUE;
    let mut acc: usize = 0;
    let mut arr: Vec<ValkeyValue> = Vec::with_capacity(capacity);

    for (key, sum) in items {
        acc += sum;
        if key != SENTINEL_VALUE && current != key {
            append_key_value(&mut arr, current, acc.into());
            if arr.len() >= limit {
                break;
            }
            current = key;
            acc = 0;
        }
    }

    ValkeyValue::Array(arr)
}
