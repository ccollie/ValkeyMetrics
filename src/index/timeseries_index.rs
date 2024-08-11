use crate::module::{with_timeseries, REDIS_PROMQL_SERIES_TYPE};
use crate::storage::time_series::TimeSeries;
use crate::storage::Label;
use metricsql_common::hash::IntMap;
use metricsql_parser::prelude::{LabelFilter, LabelFilterOp, Matchers};
use metricsql_runtime::METRIC_NAME_LABEL;
use papaya::HashMap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use redis_module::{Context, RedisString, RedisValue};
use roaring::{MultiOps, RoaringTreemap};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

pub type RedisContext = Context;

/// Map from db to TimeseriesIndex
pub type TimeSeriesIndexMap = HashMap<u32, TimeSeriesIndex>;

/// A type mapping a label name to a bitmap of ids of timeseries having that label.
/// Note that we use `BtreeMap` specifically for it's `range()` method, so a regular HashMap won't work.
pub type LabelsBitmap = BTreeMap<String, RoaringTreemap>;


#[derive(Default)]
struct IndexInner {
    /// Map from timeseries id to timeseries key.
    id_to_key: IntMap<u64, String>, // todo: have a feature to use something like compact_str
    /// Map from label name to set of timeseries ids.
    label_to_ts: LabelsBitmap,
    /// Map from label name + label value to set of timeseries ids.
    label_kv_to_ts: LabelsBitmap,
    series_sequence: AtomicU64,
}

impl IndexInner {
    pub fn new() -> IndexInner {
        IndexInner {
            id_to_key: Default::default(),
            label_to_ts: Default::default(),
            label_kv_to_ts: Default::default(),
            series_sequence: AtomicU64::new(1),
        }
    }

    fn clear(&mut self) {
        self.id_to_key.clear();
        self.label_to_ts.clear();
        self.label_kv_to_ts.clear();

        // we use Relaxed here since we only need uniqueness, not monotonicity
        self.series_sequence.store(1, Ordering::Relaxed);
    }

    fn index_time_series(&mut self, ts: &TimeSeries, key: &RedisString) {
        debug_assert!(ts.id != 0);

        self.id_to_key.insert(ts.id, key.to_string());

        if !ts.metric_name.is_empty() {
            index_series_by_label_internal(
                &mut self.label_to_ts,
                &mut self.label_kv_to_ts,
                ts.id,
                METRIC_NAME_LABEL,
                &ts.metric_name,
            );
        }

        for Label { name, value } in ts.labels.iter() {
            index_series_by_label_internal(
                &mut self.label_to_ts,
                &mut self.label_kv_to_ts,
                ts.id,
                &name,
                &value,
            );
        }
    }

    fn reindex_timeseries(&mut self, ts: &TimeSeries, key: &RedisString) {
        // todo: may cause race ?
        self.remove_series_by_id(ts.id, &ts.metric_name, &ts.labels);
        self.index_time_series(ts, key);
    }

    fn remove_series(&mut self, ts: &TimeSeries) {
        self.remove_series_by_id(ts.id, &ts.metric_name, &ts.labels);
        self.id_to_key.remove(&ts.id);
    }

    fn remove_series_by_id(&mut self, id: u64, metric_name: &str, labels: &[Label]) {
        self.id_to_key.remove(&id);
        let should_delete = !metric_name.is_empty() || !labels.is_empty();
        if !should_delete {
            return;
        }

        if !metric_name.is_empty() {
            if let Some(map) = self.label_to_ts.get_mut(METRIC_NAME_LABEL) {
                map.remove(id);
                if map.is_empty() {
                    self.label_to_ts.remove(METRIC_NAME_LABEL);
                }
            }
        }

        for Label { name, .. } in labels.iter() {
            if let Some(bitmap) = self.label_to_ts.get_mut(name) {
                bitmap.remove(id);
                if bitmap.is_empty() {
                    self.label_to_ts.remove(name);
                }
            }
        }

        if !metric_name.is_empty() {
            let key = format!("{}={}", METRIC_NAME_LABEL, metric_name);
            if let Some(bitmap) = self.label_kv_to_ts.get_mut(&key) {
                bitmap.remove(id);
                if bitmap.is_empty() {
                    self.label_kv_to_ts.remove(&key);
                }
            }
        }

        for Label { name, value} in labels.iter() {
            let key = format!("{}={}", name, value);
            if let Some(bitmap) = self.label_kv_to_ts.get_mut(&key) {
                bitmap.remove(id);
                if bitmap.is_empty() {
                    self.label_kv_to_ts.remove(&key);
                }
            }
        }

    }

    fn index_series_by_metric_name(&mut self, ts_id: u64, metric_name: &str) {
        self.index_series_by_label(ts_id, METRIC_NAME_LABEL, metric_name);
    }

    fn index_series_by_label(&mut self, ts_id: u64, label: &str, value: &str) {
        index_series_by_label_internal(&mut self.label_to_ts, &mut self.label_kv_to_ts, ts_id, label, value)
    }

    pub(super) fn get_ids_by_metric_name(&self, metric: &str) -> RoaringTreemap {
        let key = format!("{}={}", METRIC_NAME_LABEL, metric);
        if let Some(ts_ids) = self.label_kv_to_ts.get(&key) {
            return ts_ids.clone();
        }
        RoaringTreemap::new()
    }

    /// Returns a list of all series matching `matchers` while having samples in the range
    /// [`start`, `end`]
    fn series_ids_by_matchers<'ctx>(&self, matchers: &[Matchers]) -> RoaringTreemap {
        if matchers.is_empty() {
            return Default::default();
        }
        if matchers.len() == 1 {
            let filter = &matchers[0];
            return find_ids_by_matchers(&self.label_kv_to_ts, filter);
        }
        // todo: if we get a None from get_series_by_id, we should log an error
        // and remove the id from the index
        matchers
            .par_iter()
            .map(|filter| find_ids_by_matchers(&self.label_kv_to_ts, filter))
            .collect::<Vec<_>>()
            .intersection()
    }
}

/// Index for quick access to timeseries by label, label value or metric name.
/// TODO: do we need to have one per db ?
#[derive(Default)]
pub(crate) struct TimeSeriesIndex {
    inner: RwLock<IndexInner>,
}

impl TimeSeriesIndex {
    pub fn new() -> TimeSeriesIndex {
        TimeSeriesIndex {
            inner: RwLock::new(IndexInner{
                id_to_key: Default::default(),
                label_to_ts: Default::default(),
                label_kv_to_ts: Default::default(),
                series_sequence: AtomicU64::new(1)
            })
        }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.clear();
    }

    pub fn label_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.label_to_ts.len()
    }
    pub fn series_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.id_to_key.len()
    }

    pub(crate) fn next_id(&self) -> u64 {
        let inner = self.inner.read().unwrap();
        inner.series_sequence.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn index_time_series(&self, ts: &TimeSeries, key: &RedisString) {
        debug_assert!(ts.id != 0);
        let mut inner = self.inner.write().unwrap();

        inner.id_to_key.insert(ts.id, key.to_string());

        if !ts.metric_name.is_empty() {
            inner.index_series_by_label(ts.id, METRIC_NAME_LABEL, &ts.metric_name);
        }

        for Label { name, value } in ts.labels.iter() {
            inner.index_series_by_label(ts.id, &name, &value);
        }
    }

    pub fn reindex_timeseries(&self, ts: &TimeSeries, key: &RedisString) {
        let mut inner = self.inner.write().unwrap();
        // todo: may cause race ?
        inner.remove_series_by_id(ts.id, &ts.metric_name, &ts.labels);
        inner.index_time_series(ts, key);
    }

    pub(crate) fn remove_series(&self, ts: &TimeSeries) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_series_by_id(ts.id, &ts.metric_name, &ts.labels);
        inner.id_to_key.remove(&ts.id);
    }

    pub fn remove_series_by_id(&self, id: u64, metric_name: &str, labels: &[Label]) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_series_by_id(id, metric_name, labels);
    }

    fn index_series_by_labels(&self, ts_id: u64, labels: &[Label]) {
        let mut inner = self.inner.write().unwrap();
        for Label { name, value} in labels.iter() {
            inner.index_series_by_label(ts_id, name, value)
        }
    }

    pub(crate) fn remove_series_by_key(&self, ctx: &Context, key: &RedisString) -> bool {
        let mut inner = self.inner.write().unwrap();
        let redis_key = ctx.open_key(key);
        match redis_key.get_value::<TimeSeries>(&REDIS_PROMQL_SERIES_TYPE) {
            Ok(Some(ts)) => {
                inner.remove_series(ts);
                return true;
            }
            _ => {

            }
        }
        false
    }

    pub(super) fn get_ids_by_metric_name(&self, metric: &str) -> RoaringTreemap {
        let inner = self.inner.read().unwrap();
        let key = format!("{}={}", METRIC_NAME_LABEL, metric);
        if let Some(ts_ids) = inner.label_kv_to_ts.get(&key) {
            return ts_ids.clone();
        }
        RoaringTreemap::new()
    }

    pub(crate) fn rename_series(&self, ctx: &Context, new_key: &RedisString) -> bool {
        let mut inner = self.inner.write().unwrap();
        with_timeseries(ctx, new_key, | series | {
            let id = series.id;
            inner.id_to_key.insert(id, new_key.to_string());
            Ok(RedisValue::from(0i64))
        }).is_ok()
    }

    /// Return a bitmap of series ids that have the given label and pass the filter `predicate`.
    pub(crate) fn get_label_value_bitmap(
        &self,
        label: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> RoaringTreemap {
        let inner = self.inner.read().unwrap();
        get_label_value_bitmap(&inner.label_kv_to_ts, label, predicate)
    }

    /// Returns a list of all values for the given label
    pub(crate) fn get_label_values(
        &self,
        label: &str,
    ) -> BTreeSet<String> {
        let inner = self.inner.read().unwrap();
        let prefix = format!("{label}=");
        let suffix = format!("{label}={}", char::MAX);
        inner.label_kv_to_ts
            .range(prefix..suffix)
            .flat_map(|(key, _)| {
                if let Some((_, value)) = key.split_once('=') {
                    Some(value.to_string())
                } else {
                    None
                }
            }).into_iter().collect()
    }

    /// Returns a list of all series matching `matchers` while having samples in the range
    /// [`start`, `end`]
    pub(crate) fn series_ids_by_matchers<'ctx>(&self, matchers: &[Matchers]) -> RoaringTreemap {
        let inner = self.inner.read().unwrap();
        inner.series_ids_by_matchers(matchers)
    }

    /// Returns a list of all series matching `matchers` while having samples in the range
    /// [`start`, `end`]
    pub(crate) fn series_keys_by_matchers<'a>(&'a self, ctx: &Context, matchers: &[Matchers]) -> Vec<RedisString> {
        let inner = self.inner.read().unwrap();
        let bitmap = inner.series_ids_by_matchers(matchers);
        let mut result: Vec<RedisString> = Vec::with_capacity(bitmap.len() as usize);
        for id in bitmap.iter() {
            if let Some(value) = inner.id_to_key.get(&id) {
                let key = ctx.create_string(value);
                result.push(key)
            }
        }
        result
    }

    pub(crate) fn find_ids_by_matchers(&self, matchers: &Matchers) -> RoaringTreemap {
        let inner = self.inner.read().unwrap();
        find_ids_by_matchers(&inner.label_kv_to_ts, matchers)
    }
}

fn get_label_value_bitmap(
    label_kv_to_ts: &LabelsBitmap,
    label: &str,
    predicate: impl Fn(&str) -> bool,
) -> RoaringTreemap {
    let prefix = format!("{label}=");
    let suffix = format!("{label}=\u{10ffff}");
    label_kv_to_ts
        .range(prefix..suffix)
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
    label_to_ts: &mut LabelsBitmap,
    label_kv_to_ts: &mut LabelsBitmap,
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

fn find_ids_by_label_filter(
    label_kv_to_ts: &LabelsBitmap,
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

const MAX_EQUAL_MAP_SIZE: usize = 5;

fn find_ids_by_multiple_filters<'a>(
    label_kv_to_ts: &'a LabelsBitmap,
    filters: &Vec<LabelFilter>,
    bitmaps: &mut Vec<RoaringTreemap>,
    key_buf: &mut String,
) {
    //bitmaps.clear();

    let mut equal_bitmap: RoaringTreemap = RoaringTreemap::new();
    let mut has_equal = false;
    for filter in filters.iter() {
        // perform a more efficient lookup for label=value
        if filter.op == LabelFilterOp::Equal {
            // according to https://github.com/rust-lang/rust/blob/1.47.0/library/alloc/src/string.rs#L2414-L2427
            // write! will not return an Err, so the unwrap is safe
            write!(key_buf, "{}={}", filter.label, filter.value).unwrap();
            if let Some(map) = label_kv_to_ts.get(&key_buf.to_string()) {
                equal_bitmap &= map;
                has_equal = true;
            }
            key_buf.clear();
        } else {
            let map = find_ids_by_label_filter(label_kv_to_ts, filter);
            bitmaps.push(map);
        }
    }
    if has_equal {
        bitmaps.push(equal_bitmap);
    }
}

fn find_ids_by_matchers(
    label_kv_to_ts: &LabelsBitmap,
    matchers: &Matchers,
) -> RoaringTreemap {
    let mut bitmaps: Vec<RoaringTreemap> = Vec::new();
    let mut key_buf = String::with_capacity(64);

    let mut or_bitmap: Option<RoaringTreemap> = None;
    {
        if !matchers.or_matchers.is_empty() {
            for filter in matchers.or_matchers.iter() {
                find_ids_by_multiple_filters(label_kv_to_ts, filter, &mut bitmaps, &mut key_buf);
            }
            or_bitmap = Some(bitmaps.union());
        }
    }

    if !matchers.matchers.is_empty() {
        find_ids_by_multiple_filters(label_kv_to_ts, &matchers.matchers, &mut bitmaps, &mut key_buf);
        if let Some(or_bitmap) = or_bitmap {
            bitmaps.push(or_bitmap);
        }
    }

    bitmaps.intersection()
}

#[cfg(test)]
mod tests {}
