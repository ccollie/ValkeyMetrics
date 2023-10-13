use std::collections::{BTreeMap, BTreeSet};
use metricsql_engine::METRIC_NAME_LABEL;
use metricsql_parser::prelude::{LabelFilter, LabelFilterOp, Matchers};
use redis_module::{Context, RedisError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use ahash::AHashMap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use roaring::{MultiOps, RoaringTreemap};
use crate::common::types::Timestamp;
use crate::module::get_timeseries;
use crate::ts::time_series::TimeSeries;
use crate::ts::{Labels};

pub type RedisContext = Context;

// Note. Unfortunately we can't use ahash here because it's RandomState uses `allocator-api`,
// which at the time of writing this is unstable. Maybee xxxhash-rust will be a good alternative.
pub type BitmapMap = BTreeMap<String, RoaringTreemap>;

/// Index for quick access to timeseries by label, label value or metric name.
pub struct TimeSeriesIndex {
    group_sequence: AtomicU64,
    /// Map from timeseries id to timeseries key.
    id_to_key: RwLock<AHashMap<u64, String>>,
    /// Map from label name to set of timeseries ids.
    label_to_ts: RwLock<BitmapMap>,
    /// Map from label name + label value to set of timeseries ids.
    label_kv_to_ts: RwLock<BitmapMap>,
    series_sequence: AtomicU64,
}

impl TimeSeriesIndex {
    pub fn new() -> TimeSeriesIndex {
        TimeSeriesIndex {
            group_sequence: AtomicU64::new(0),
            id_to_key: Default::default(),
            label_to_ts: Default::default(),
            label_kv_to_ts: Default::default(),
            series_sequence: Default::default(),
        }
    }

    pub fn clear(&self) {
        let mut id_to_key_map = self.id_to_key.write().unwrap();
        id_to_key_map.clear();

        let mut label_to_ts = self.label_to_ts.write().unwrap();
        label_to_ts.clear();

        let mut label_kv_to_ts = self.label_kv_to_ts.write().unwrap();
        label_kv_to_ts.clear();

        self.series_sequence.store(0, Ordering::Relaxed);
    }

    pub fn label_count(&self) -> usize {
        let label_to_ts = self.label_to_ts.read().unwrap();
        label_to_ts.len()
    }

    pub fn series_count(&self) -> usize {
        let id_to_key_map = self.id_to_key.read().unwrap();
        id_to_key_map.len()
    }

    pub(crate) fn next_id(&self) -> u64 {
        // wee use Relaxed here since we only need uniqueness, not monotonicity
        self.series_sequence.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn index_time_series(&self, ts: &mut TimeSeries, key: String) {
        if ts.id == 0 {
            ts.id = self.next_id();
        }
        let mut id_to_key_map = self.id_to_key.write().unwrap();
        id_to_key_map.insert(ts.id, key);
        drop(id_to_key_map);

        let mut label_to_ts = self.label_to_ts.write().unwrap();
        let mut label_kv_to_ts = self.label_kv_to_ts.write().unwrap();

        index_series_by_label_internal(
            &mut label_to_ts,
            &mut label_kv_to_ts,
            ts.id,
            METRIC_NAME_LABEL,
            &ts.metric_name,
        );

        for (label_name, label_value) in ts.labels.iter() {
            index_series_by_label_internal(
                &mut label_to_ts,
                &mut label_kv_to_ts,
                ts.id,
                label_name,
                label_value,
            );
        }
    }

    pub fn reindex_timeseries(&self, ts: &TimeSeries) {
        let mut label_to_ts = self.label_to_ts.write().unwrap();
        let mut label_kv_to_ts = self.label_kv_to_ts.write().unwrap();

        for (label_name, label_value) in ts.labels.iter() {
            index_series_by_label_internal(
                &mut label_to_ts,
                &mut label_kv_to_ts,
                ts.id,
                label_name,
                label_value,
            );
        }
        // todo !!!!
    }

    pub(crate) fn remove_timeseries(&self, ts: &TimeSeries) {
        self.remove_series_by_id(ts.id, &ts.labels);
        let mut id_to_key_map = self.id_to_key.write().unwrap();
        id_to_key_map.remove(&ts.id);
    }

    pub(crate) fn remove_series_by_id(&self, id: u64, labels: &Labels) {
        {
            let mut id_to_key_map = self.id_to_key.write().unwrap();
            id_to_key_map.remove(&id);
        }
        {
            let mut label_to_ts = self.label_to_ts.write().unwrap();
            for (label_name, _) in labels.iter() {
                if let Some(ts_by_label) = label_to_ts.get_mut(label_name) {
                    ts_by_label.remove(id);
                }
            }
        }
        {
            let mut label_kv_to_ts = self.label_kv_to_ts.write().unwrap();
            for (label_name, label_value) in labels.iter() {
                let key = format!("{}={}", label_name, label_value);
                if let Some(ts_by_label_value) = label_kv_to_ts.get_mut(&key) {
                    ts_by_label_value.remove(id);
                }
            }
        }
    }

    fn index_series_by_metric_name(&mut self, ts_id: u64, metric_name: &str) {
        self.index_series_by_label(ts_id, METRIC_NAME_LABEL, metric_name);
    }

    fn index_series_by_label(&mut self, ts_id: u64, label: &str, value: &str) {
        let mut label_to_ts = self.label_to_ts.write().unwrap();
        let mut label_kv_to_ts = self.label_kv_to_ts.write().unwrap();

        index_series_by_label_internal(&mut label_to_ts, &mut label_kv_to_ts, ts_id, label, value)
    }

    pub(crate) fn index_series_by_labels(&mut self, ts_id: u64, labels: Labels) {
        for (name, value) in labels.iter() {
            self.index_series_by_label(ts_id, name, value)
        }
    }

    pub(crate) fn remove_series_by_key(&self, key: &String) {
        todo!()
    }

    pub fn get_series_by_id<'a>(
        &'a self,
        ctx: &'a Context,
        id: u64,
    ) -> Result<Option<&TimeSeries>, RedisError> {
        let id_to_key = self.id_to_key.read().unwrap();
        get_series_by_id(ctx, &id_to_key, id)
    }

    pub(crate) fn get_series_by_metric_name<'a>(
        &'a self,
        ctx: &'a Context,
        metric: &str,
        res: &mut Vec<&'a TimeSeries>,
    ) {
        let label_kv_to_ts = self.label_kv_to_ts.read().unwrap();
        let id_to_key = self.id_to_key.read().unwrap();
        let key = format!("{}={}", METRIC_NAME_LABEL, metric);
        if let Some(ts_ids) = label_kv_to_ts.get(&key) {
            let iter = ts_ids
                .iter()
                .flat_map(|id| get_series_by_id(ctx, &id_to_key, id).unwrap_or(None));

            res.extend(iter);
        }
    }

    pub(crate) fn get_label_value_bitmap(
        &self,
        label: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> RoaringTreemap {
        let label_kv_to_ts = self.label_kv_to_ts.read().unwrap();
        get_label_value_bitmap(&label_kv_to_ts, label, predicate)
    }

    pub(crate) fn get_label_values(
        &self,
        label: &str,
    ) -> BTreeSet<String> {
        let label_kv_to_ts = self.label_kv_to_ts.read().unwrap();
        let prefix = format!("{label}=");
        label_kv_to_ts
            .range(prefix..)
            .flat_map(|(key, _)| {
                if let Some((_, value)) = key.split_once('=') {
                    Some(value.to_string())
                } else {
                    None
                }
            }).into_iter().collect()
    }

    pub(crate) fn series_by_matchers<'a>(
        &'a self,
        ctx: &'a Context,
        matchers: &Vec<Matchers>,
        start: Timestamp,
        end: Timestamp,
    ) -> Vec<&TimeSeries> {
        let id_to_key = self.id_to_key.read().unwrap();
        let label_kv_to_ts = self.label_kv_to_ts.read().unwrap();
        matchers
            .par_iter()
            .map(|filter| find_ids_by_matchers(&label_kv_to_ts, filter))
            .collect::<Vec<_>>()
            .intersection()
            .into_iter()
            .flat_map(|id| get_series_by_id(ctx, &id_to_key, id).unwrap_or(None))
            .filter(|ts| ts.overlaps(start, end))
            .collect()
    }

    pub(crate) fn labels_by_matchers<'a>(
        &'a self,
        ctx: &'a Context,
        matchers: &Vec<Matchers>,
        start: Timestamp,
        end: Timestamp,
    ) -> BTreeSet<&'a String> {
        let series = self.series_by_matchers(ctx, matchers, start, end);
        let mut result: BTreeSet<&'a String> = BTreeSet::new();

        for ts in series {
            result.extend(ts.labels.keys());
        }

        result
    }

    pub(crate) fn find_ids_by_matchers(&self, matchers: &Matchers) -> RoaringTreemap {
        let label_kv_to_ts = self.label_kv_to_ts.read().unwrap();
        find_ids_by_matchers(&label_kv_to_ts, matchers)
    }
}

fn get_label_value_bitmap(
    label_kv_to_ts: &RwLockReadGuard<BitmapMap>,
    label: &str,
    predicate: impl Fn(&str) -> bool,
) -> RoaringTreemap {
    let prefix = format!("{label}=");
    label_kv_to_ts
        .range(prefix..)
        .flat_map(|(key, map)| {
            if let Some((_, value)) = key.split_once('=') {
                return if predicate(&value) {
                    Some(map)
                } else {
                    None
                };
            }
            None
        })
        .collect::<Vec<_>>()
        .union()
}

fn index_series_by_label_internal(
    label_to_ts: &mut RwLockWriteGuard<BitmapMap>,
    label_kv_to_ts: &mut RwLockWriteGuard<BitmapMap>,
    ts_id: u64,
    label: &str,
    value: &str,
) {
    let ts_by_label = label_to_ts
        .entry(label.to_owned())
        .or_insert_with(|| RoaringTreemap::new());

    ts_by_label.insert(ts_id);

    let ts_by_label_value = label_kv_to_ts
        .entry(format!("{}={}", label, value))
        .or_insert_with(|| RoaringTreemap::new());

    ts_by_label_value.insert(ts_id);
}

#[inline]
fn get_series_by_id<'a>(
    ctx: &'a Context,
    id_to_key: &RwLockReadGuard<AHashMap<u64, String>>,
    id: u64,
) -> Result<Option<&'a TimeSeries>, RedisError> {
    if let Some(key) = id_to_key.get(&id) {
        // todo: eliminate this copy
        let rkey = ctx.create_string(key.as_str());
        return get_timeseries(ctx, &rkey, false);
    }
    Ok(None)
}

fn get_multi_series_by_id<'a>(
    ctx: &'a Context,
    id_to_key: &RwLockReadGuard<AHashMap<u64, String>>,
    ids: &[u64],
) -> Result<Vec<Option<&'a TimeSeries>>, RedisError> {
    ids.iter()
        .map(|id| get_series_by_id(ctx, &id_to_key, *id))
        .collect()
}

fn find_ids_by_label_filter<'a>(
    label_kv_to_ts: &'a RwLockReadGuard<BitmapMap>,
    filter: &LabelFilter,
) -> RoaringTreemap {
    use LabelFilterOp::*;

    match filter.op {
        Equal => {
            unreachable!("Equal should be handled by find_ids_by_matchers")
        }
        NotEqual => {
            let predicate = |value: &str| value != filter.value;
            get_label_value_bitmap(label_kv_to_ts, &filter.label, predicate)
        }
        RegexEqual => {
            // todo: return Result. However if we get an invalid regex here,
            // we have a problem with the base metricsql library.
            let regex = regex::Regex::new(&filter.value).unwrap();
            let predicate = |value: &str| regex.is_match(value);
            get_label_value_bitmap(label_kv_to_ts, &filter.label, predicate)
        }
        RegexNotEqual => {
            // todo: return Result. However if we get an invalid regex here,
            // we have a problem with the base metricsql library.
            let regex = regex::Regex::new(&filter.value).unwrap();
            let predicate = |value: &str| !regex.is_match(value);
            get_label_value_bitmap(label_kv_to_ts, &filter.label, predicate)
        }
    }
}

fn find_ids_by_matchers(
    label_kv_to_ts: &RwLockReadGuard<BitmapMap>,
    matchers: &Matchers,
) -> RoaringTreemap {
    let mut bitmaps: Vec<RoaringTreemap> = Vec::with_capacity(matchers.len());

    // special case label=value. We can use a more efficient lookup and not have to clone
    // the bitmap
    let exact = matchers.iter()
        .filter(|m| m.op == LabelFilterOp::Equal)
        .flat_map(|m| {
            let key = format!("{}={}", m.label, m.value);
            label_kv_to_ts.get(&key)
        })
        .collect::<Vec<_>>();

    if !exact.is_empty() {
        bitmaps.push(exact.intersection());
    }

    for matcher in matchers.iter() {
        if matcher.op != LabelFilterOp::Equal {
            let map = find_ids_by_label_filter(label_kv_to_ts, matcher);
            bitmaps.push(map);
        }
    }
    bitmaps.intersection()
}
#[cfg(test)]
mod tests {}
