use crate::error::TsdbResult;
use crate::module::{with_timeseries, VKM_SERIES_TYPE};
use crate::storage::time_series::TimeSeries;
use crate::storage::utils::format_prometheus_metric_name;
use crate::storage::Label;
use metricsql_common::hash::IntMap;
use metricsql_parser::prelude::{LabelFilter, LabelFilterOp, Matchers};
use metricsql_runtime::METRIC_NAME_LABEL;
use papaya::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard};
use croaring::Bitmap64;
use valkey_module::{Context, ValkeyString, ValkeyValue};

/// Type for the key of the index. Use instead of `String` because Valkey keys are binary safe not utf8 safe.
pub type IndexKeyType = Box<[u8]>;

/// Map from db to TimeseriesIndex
pub type TimeSeriesIndexMap = HashMap<u32, TimeSeriesIndex>;

/// A type mapping a label name to a bitmap of ids of timeseries having that label.
/// Note that we use `BtreeMap` specifically for it's `range()` method, so a regular HashMap won't work.
pub type LabelsBitmap = BTreeMap<String, Bitmap64>;


// todo: in on_load, we need to set this to the last id + 1
static TIMESERIES_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static TIMESERIES_ID_MAX: AtomicU64 = AtomicU64::new(1);

pub fn next_timeseries_id() -> u64 {
    // we use Relaxed here since we only need uniqueness, not monotonicity
    let id = TIMESERIES_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    TIMESERIES_ID_MAX.fetch_max(id, Ordering::Relaxed);
    id
}

pub fn reset_timeseries_id_sequence() {
    TIMESERIES_ID_SEQUENCE.store(1, Ordering::SeqCst);
    TIMESERIES_ID_MAX.store(1, Ordering::SeqCst);
}

pub fn reset_timeseries_id_after_load() -> u64 {
    let mut max = TIMESERIES_ID_MAX.fetch_max(1, Ordering::SeqCst);
    max += 1;
    TIMESERIES_ID_SEQUENCE.store(max, Ordering::SeqCst);
    max
}

#[derive(Clone, Copy)]
enum SetOperation {
    Union,
    Intersection,
}

#[derive(Default, Debug)]
pub(crate) struct IndexInner {
    /// Map from timeseries id to timeseries key.
    pub id_to_key: IntMap<u64, IndexKeyType>, // todo: have a feature to use something like compact_str
    /// Map from label name to set of timeseries ids.
    pub label_to_ts: LabelsBitmap,
    /// Map from label name + label value to set of timeseries ids.
    pub label_kv_to_ts: LabelsBitmap,
}

impl IndexInner {
    pub fn new() -> IndexInner {
        IndexInner {
            id_to_key: Default::default(),
            label_to_ts: Default::default(),
            label_kv_to_ts: Default::default(),
        }
    }

    fn clear(&mut self) {
        self.id_to_key.clear();
        self.label_to_ts.clear();
        self.label_kv_to_ts.clear();
    }

    fn index_time_series(&mut self, ts: &TimeSeries, key: &[u8]) {
        debug_assert!(ts.id != 0);

        let boxed_key = key.to_vec().into_boxed_slice();
        self.id_to_key.insert(ts.id, boxed_key);

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
                name,
                value,
            );
        }
    }

    fn reindex_timeseries(&mut self, ts: &TimeSeries, key: &[u8]) {
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

    pub(super) fn get_ids_by_metric_name(&self, metric: &str) -> Option<&Bitmap64> {
        let key = format!("{}={}", METRIC_NAME_LABEL, metric);
        self.label_kv_to_ts.get(&key)
    }

    /// Returns a list of all series matching `matchers` while having samples in the range
    /// [`start`, `end`]
    fn series_ids_by_matchers(&self, matchers: &[Matchers]) -> Bitmap64 {
        if matchers.is_empty() {
            return Default::default();
        }

        let mut dest = Bitmap64::new();

        if matchers.len() == 1 {
            let filter = &matchers[0];
            find_ids_by_matchers(&self.label_kv_to_ts, filter, &mut dest);
            return dest;
        }

        // todo: if we get a None from get_series_by_id, we should log an error
        // and remove the id from the index

        // todo: determine if we should use rayon here. Some ideas
        // - look at label cardinality for each filter in matcher
        // - look at complexity of matchers (regex vs no regex)
        let mut dest = Bitmap64::new();
        let mut acc = Bitmap64::new();
        for matcher in matchers.iter() {
            find_ids_by_matchers(&self.label_kv_to_ts, matcher, &mut acc);
            dest.or_inplace(&acc);
            acc.clear();
        }

        dest
    }
}

/// Index for quick access to timeseries by label, label value or metric name.
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

    pub(crate) fn next_id() -> u64 {
        next_timeseries_id()
    }

    pub(crate) fn index_time_series(&self, ts: &TimeSeries, key: &[u8]) {
        debug_assert!(ts.id != 0);
        let mut inner = self.inner.write().unwrap();
        inner.index_time_series(ts, key);
    }

    pub fn reindex_timeseries(&self, ts: &TimeSeries, key: &[u8]) {
        let mut inner = self.inner.write().unwrap();
        inner.reindex_timeseries(ts, key);
    }

    pub(crate) fn remove_series(&self, ts: &TimeSeries) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_series(ts);
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

    pub(crate) fn remove_series_by_key(&self, ctx: &Context, key: &ValkeyString) -> bool {
        let mut inner = self.inner.write().unwrap();
        let valkey_key = ctx.open_key(key);

        if let Ok(Some(ts)) = valkey_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE) {
            inner.remove_series(ts);
            return true;
        }
        false
    }

    /// This exists primarily to ensure that we disallow duplicate metric names, since the
    /// metric name and valkey key are distinct. IE we can have the metric http_requests_total{status="200"}
    /// stored at requests:http:total:200
    pub fn get_id_by_name_and_labels(&self, metric: &str, labels: &[Label]) -> TsdbResult<Option<u64>> {
        let inner = self.inner.read().unwrap();
        let key = format!("{}={}", METRIC_NAME_LABEL, metric);
        if let Some(bmp) = inner.label_kv_to_ts.get(&key) {
            let mut key: String = String::new();
            let mut acc: Bitmap64 = bmp.clone(); // wish we didn't have to clone here
            for label in labels.iter() {
                write!(key,"{}={}", label.name, label.value).unwrap();
                if let Some(bmp) = inner.label_kv_to_ts.get(&key) {
                    acc.and_inplace(bmp);
                }
                key.clear();
            }
            match acc.cardinality() {
                0 => Ok(None),
                1 => Ok(Some(acc.iter().next().unwrap())),
                _ => {
                    let metric_name = format_prometheus_metric_name(metric, labels);
                    // todo: show keys in the error message ?
                    let msg = format!("Multiple series with the same metric: {}", metric_name);
                    Err(msg.into())
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn prometheus_name_exists(&self, metric: &str, labels: &[Label]) -> bool {
        matches!(self.get_id_by_name_and_labels(metric, labels), Ok(Some(_)))
    }

    pub fn get_key_by_name_and_labels(&self, metric: &str, labels: &[Label]) -> TsdbResult<Option<IndexKeyType>> {
        let possible_id = self.get_id_by_name_and_labels(metric, labels)?;
        match possible_id {
            Some(id) => {
                let inner = self.inner.read().unwrap();
                Ok(inner.id_to_key.get(&id).cloned())
            }
            None => Ok(None)
        }
    }

    // todo: store Arc<Bitmap64> in the index
    pub(super) fn get_ids_by_metric_name(&self, metric: &str) -> Bitmap64 {
        let inner = self.inner.read().unwrap();
        let key = format!("{}={}", METRIC_NAME_LABEL, metric);
        if let Some(bmp) = inner.label_kv_to_ts.get(&key) {
            bmp.clone()
        } else {
            Bitmap64::new()
        }
    }

    pub(crate) fn rename_series(&self, ctx: &Context, new_key: &ValkeyString) -> bool {
        let mut inner = self.inner.write().unwrap();
        with_timeseries(ctx, new_key, | series | {
            let id = series.id;
            // slow, but we don't expect this to be called often
            let key = new_key.as_slice().to_vec().into_boxed_slice();
            inner.id_to_key.insert(id, key);
            Ok(ValkeyValue::from(0i64))
        }).is_ok()
    }

    /// Return a bitmap of series ids that have the given label and pass the filter `predicate`.
    pub(crate) fn get_label_value_bitmap(
        &self,
        label: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> Bitmap64 {
        let inner = self.inner.read().unwrap();
        get_label_value_bitmap(&inner.label_kv_to_ts, label, predicate)
    }

    /// Returns a list of all values for the given label
    pub fn get_label_values(
        &self,
        label: &str,
    ) -> BTreeSet<String> {
        let inner = self.inner.read().unwrap();
        let prefix = format!("{label}=");
        let suffix = format!("{label}={}", char::MAX);
        let split_pos = prefix.len();
        inner.label_kv_to_ts
            .range(prefix..suffix)
            .map(|(key, _)| { key[split_pos..].to_string() })
            .collect()
    }

    pub fn is_series_indexed(&self, id: u64) -> bool {
        let inner = self.inner.read().unwrap();
        inner.id_to_key.contains_key(&id)
    }
    pub fn is_key_indexed(&self, key: &str) -> bool {
        let inner = self.inner.read().unwrap();
        let key = format!("{}={}", METRIC_NAME_LABEL, key);
        inner.label_kv_to_ts.contains_key(&key)
    }

    /// Returns a list of all series matching `matchers` while having samples in the range
    /// [`start`, `end`]
    pub(crate) fn series_ids_by_matchers(&self, matchers: &[Matchers]) -> Bitmap64 {
        let inner = self.inner.read().unwrap();
        inner.series_ids_by_matchers(matchers)
    }

    /// Returns a list of all series matching `matchers` while having samples in the range
    /// [`start`, `end`]
    pub(crate) fn series_keys_by_matchers(&self, ctx: &Context, matchers: &[Matchers]) -> Vec<ValkeyString> {
        let inner = self.inner.read().unwrap();
        let bitmap = inner.series_ids_by_matchers(matchers);
        let mut result: Vec<ValkeyString> = Vec::with_capacity(bitmap.cardinality() as usize);
        for id in bitmap.iter() {
            if let Some(value) = inner.id_to_key.get(&id) {
                let key = ctx.create_string(&value[0..]);
                result.push(key)
            }
        }
        result
    }

    pub(crate) fn find_ids_by_matchers(&self, matchers: &Matchers) -> Bitmap64 {
        let inner = self.inner.read().unwrap();
        let mut dest = Bitmap64::new();
        find_ids_by_matchers(&inner.label_kv_to_ts, matchers, &mut dest);
        dest
    }

    pub(crate) fn get_inner(&self) -> RwLockReadGuard<IndexInner> {
        self.inner.read().unwrap()
    }
}

pub struct LabelNameIter<'a> {
    inner: std::collections::btree_map::Iter<'a, String, Bitmap64>,
}

impl<'a> LabelNameIter<'a> {
    pub fn new(inner: std::collections::btree_map::Iter<'a, String, Bitmap64>) -> LabelNameIter<'a> {
        LabelNameIter { inner }
    }
}

impl<'a> Iterator for LabelNameIter<'a> {
    type Item = (&'a str, &'a Bitmap64);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key, map)) = self.inner.next() {
            return Some((key, map));
        }
        None
    }
}


fn filter_by_label_value_predicate(
    label_kv_to_ts: &LabelsBitmap,
    dest: &mut Bitmap64,
    op: SetOperation,
    label: &str,
    predicate: impl Fn(&str) -> bool,
) {
    let prefix = format!("{label}=");
    let suffix = format!("{label}=\u{10ffff}");
    let start_pos = prefix.len();
    let mut iter = label_kv_to_ts
        .range(prefix..suffix)
        .filter_map(|(key, map)| {
            let value = &key[start_pos..];
            if predicate(value) {
                Some(map)
            } else {
                None
            }
        });

    match op {
        SetOperation::Union => {
            for map in iter {
                dest.or_inplace(map);
            }
        }
        SetOperation::Intersection => {
            if dest.is_empty() {
                // we need at least one set to intersect with
                if let Some(first) = iter.next() {
                    dest.or_inplace(first);
                } else {
                    return;
                }
            }
            for map in iter {
                dest.and_inplace(map);
            }
        }
    }
}

fn get_label_value_bitmap(
    label_kv_to_ts: &LabelsBitmap,
    label: &str,
    predicate: impl Fn(&str) -> bool,
) -> Bitmap64 {
    let prefix = format!("{label}=");
    let suffix = format!("{label}=\u{10ffff}");
    let start_pos = prefix.len();
    let mut bitmap = Bitmap64::new();

    let iter = label_kv_to_ts
        .range(prefix..suffix)
        .filter_map(|(key, map)| {
            let value = &key[start_pos..];
            if predicate(value) {
                Some(map)
            } else {
                None
            }
        });

    for map in iter {
        bitmap.or_inplace(map);
    }
    bitmap
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
        .or_default();

    ts_by_label.add(ts_id);

    let ts_by_label_value = label_kv_to_ts
        .entry(format!("{}={}", label, value))
        .or_default();

    ts_by_label_value.add(ts_id);
}



fn find_ids_by_label_filter(
    label_kv_to_ts: &LabelsBitmap,
    filter: &LabelFilter,
    dest: &mut Bitmap64,
    op: SetOperation,
    key_buf: &mut String,
) {
    use LabelFilterOp::*;

    match filter.op {
        Equal => {
            key_buf.clear();
            // according to https://github.com/rust-lang/rust/blob/1.47.0/library/alloc/src/string.rs#L2414-L2427
            // write! will not return an Err, so the unwrap is safe
            write!(key_buf, "{}={}", filter.label, filter.value).unwrap();
            if let Some(map) = label_kv_to_ts.get(key_buf.as_str()) {
                match op {
                    SetOperation::Union => {
                        dest.or_inplace(map);
                    }
                    SetOperation::Intersection => {
                        if dest.is_empty() {
                            // we need at least one set to intersect with
                            dest.or_inplace(map);
                        } else {
                            dest.and_inplace(map);
                        }
                    }
                }
            } else if matches!(op, SetOperation::Intersection) {
                // if we are intersecting, and we don't have a match, we can return early
                dest.clear();
                return;
            }
        }
        NotEqual => {
            let predicate = |value: &str| value != filter.value;
            filter_by_label_value_predicate(label_kv_to_ts, dest, op, &filter.label, predicate)
        }
        RegexEqual => {
            // todo: return Result. However if we get an invalid regex here,
            // we have a problem with the base metricsql library.
            let regex = regex::Regex::new(&filter.value).unwrap();
            let predicate = |value: &str| regex.is_match(value);
            filter_by_label_value_predicate(label_kv_to_ts, dest, op, &filter.label, predicate)
        }
        RegexNotEqual => {
            // todo: return Result. However if we get an invalid regex here,
            // we have a problem with the base metricsql library.
            let regex = regex::Regex::new(&filter.value).unwrap();
            let predicate = |value: &str| !regex.is_match(value);
            filter_by_label_value_predicate(label_kv_to_ts, dest, op, &filter.label, predicate)
        }
    }
}

fn find_ids_by_exact_match<'a>(
    label_kv_to_ts: &'a LabelsBitmap,
    label: &str,
    value: &str,
) -> Option<&'a Bitmap64> {
    let key = format!("{}={}", label, value);
    label_kv_to_ts.get(&key)
}

fn find_ids_by_multiple_filters(
    label_kv_to_ts: &LabelsBitmap,
    filters: &[LabelFilter],
    dest: &mut Bitmap64,
    operation: SetOperation,
    key_buf: &mut String, // used to minimize allocations
) {
    for filter in filters.iter() {
        find_ids_by_label_filter(label_kv_to_ts, filter, dest, operation, key_buf);
    }
}

fn find_ids_by_matchers(
    label_kv_to_ts: &LabelsBitmap,
    matchers: &Matchers,
    dest: &mut Bitmap64
) {
    let mut key_buf = String::with_capacity(64);

    if !matchers.or_matchers.is_empty() {
        for filter in matchers.or_matchers.iter() {
            find_ids_by_multiple_filters(label_kv_to_ts, filter, dest, SetOperation::Union, &mut key_buf);
        }
    }

    if !matchers.matchers.is_empty() {
        find_ids_by_multiple_filters(label_kv_to_ts, &matchers.matchers, dest, SetOperation::Intersection, &mut key_buf);
    }
}

#[cfg(test)]
mod tests {}
