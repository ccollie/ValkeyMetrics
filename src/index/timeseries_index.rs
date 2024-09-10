use super::index_key::*;
use crate::common::types::{Label, Timestamp};
use crate::error::TsdbResult;
use crate::index::filters::{get_ids_by_matchers_optimized, process_equals_match, process_iterator};
use crate::module::{with_timeseries, VKM_SERIES_TYPE};
use crate::storage::time_series::TimeSeries;
use crate::storage::utils::format_prometheus_metric_name;
use croaring::Bitmap64;
use metricsql_common::hash::IntMap;
use metricsql_parser::prelude::{LabelFilter, LabelFilterOp, Matchers};
use metricsql_runtime::types::METRIC_NAME_LABEL;
use papaya::HashMap;
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::ops::ControlFlow::Continue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard};
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};

/// Type for the key of the index. Use instead of `String` because Valkey keys are binary safe not utf8 safe.
pub type KeyType = Box<[u8]>;

/// Map from db to TimeseriesIndex
pub type TimeSeriesIndexMap = HashMap<u32, TimeSeriesIndex>;

// label
// label=value
pub type ARTBitmap = blart::TreeMap<IndexKey, Bitmap64>;

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
pub(super) enum SetOperation {
    Union,
    Intersection,
}

impl PartialEq for SetOperation {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SetOperation::Union, SetOperation::Union) => true,
            (SetOperation::Intersection, SetOperation::Intersection) => true,
            _ => false,
        }
    }
}

#[derive(Default, Debug)]
pub(crate) struct IndexInner {
    /// Map from timeseries id to timeseries key.
    pub id_to_key: IntMap<u64, KeyType>,
    /// Map from label name and (label name,  label value) to set of timeseries ids.
    pub label_index: ARTBitmap,
    pub label_count: usize,
}

impl IndexInner {
    pub fn new() -> IndexInner {
        IndexInner {
            id_to_key: Default::default(),
            label_index: Default::default(),
            label_count: 0,
        }
    }

    fn clear(&mut self) {
        self.id_to_key.clear();
        self.label_index.clear();
        self.label_count = 0;
    }

    fn index_time_series(&mut self, ts: &TimeSeries, key: &[u8]) {
        debug_assert!(ts.id != 0);

        let boxed_key = key.to_vec().into_boxed_slice();
        self.id_to_key.insert(ts.id, boxed_key);

        if !ts.metric_name.is_empty() {
            self.index_series_by_label(ts.id, METRIC_NAME_LABEL, &ts.metric_name);
        }

        for Label { name, value } in ts.labels.iter() {
            self.index_series_by_label(ts.id, name, value);
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
        // should never happen, but just in case
        if metric_name.is_empty() && labels.is_empty() {
            return;
        }

        if !metric_name.is_empty() {
            self.remove_label_value(METRIC_NAME_LABEL, metric_name, id);
        }

        for Label { name, value} in labels.iter() {
            self.remove_label_value(name, value, id);
        }
    }

    fn index_series_by_metric_name(&mut self, ts_id: u64, metric_name: &str) {
        self.index_series_by_label(ts_id, METRIC_NAME_LABEL, metric_name);
    }

    fn has_label(&self, label: &str) -> bool {
        let prefix = get_key_for_label_prefix(label);
        self.label_index.prefix(prefix.as_bytes()).next().is_some()
    }

    fn add_or_insert(&mut self, label: &str, value: &str, ts_id: u64) -> bool {
        let key = IndexKey::for_label_value(label, value);
        if let Some(bmp) = self.label_index.get_mut(&key) {
            bmp.add(ts_id);
            false
        } else {
            let mut bmp = Bitmap64::new();
            bmp.add(ts_id);
            let has_label = self.has_label(label);
            // TODO: !!!!!! handle error
            self.label_index.try_insert(key, bmp).unwrap();
            if has_label {
                self.label_count += 1;
            }
            true
        }
    }

    fn index_series_by_label(&mut self, ts_id: u64, label: &str, value: &str) {
        self.add_or_insert(label, value, ts_id);
    }

    fn remove_label_value(&mut self, label: &str, value: &str, ts_id: u64) {
        let key = IndexKey::for_label_value(label, value);
        if let Some(bmp) = self.label_index.get_mut(&key) {
            bmp.remove(ts_id);
            if bmp.is_empty() {
                self.label_index.remove(&key);
                if !self.has_label(label) {
                    self.label_count -= 1;
                }
            }
        }
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
            find_ids_by_matchers(&self.label_index, filter, &mut dest);
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
            find_ids_by_matchers(&self.label_index, matcher, &mut acc);
            dest.or_inplace(&acc);
            acc.clear();
        }

        dest
    }

    fn process_label_values<T, CONTEXT, F, PRED>(
        &self,
        label: &str,
        ctx: &mut CONTEXT,
        predicate: PRED,
        f: F
    ) -> Option<T>
    where F: Fn(&mut CONTEXT, &str, &Bitmap64) -> ControlFlow<Option<T>>,
        PRED: Fn(&str) -> bool
    {
        let prefix = get_key_for_label_prefix(label);
        let start_pos = prefix.len();
        for (key, map) in self.label_index.prefix(prefix.as_bytes()) {
            let value = key.sub_string(start_pos);
            if predicate(value) {
                match f(ctx, value, map) {
                    ControlFlow::Break(v) => {
                        return v;
                    },
                    Continue(_) => continue,
                }
            }
        }
        None
    }
}

/// Index for quick access to timeseries by label, label value or metric name.
#[derive(Default)]
pub(crate) struct TimeSeriesIndex {
    inner: RwLock<IndexInner>,
}

impl TimeSeriesIndex {
    pub fn new() -> Self {
        TimeSeriesIndex {
            inner: RwLock::new(IndexInner::new())
        }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.clear();
    }

    pub fn label_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.label_count
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

    pub fn remove_series(&self, ts: &TimeSeries) {
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

    pub fn remove_series_by_key(&self, ctx: &Context, key: &ValkeyString) -> bool {
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
        let mut key: String = String::new();
        format_key_for_metric_name(&mut key, metric);
        if let Some(measurement_bmp) = inner.label_index.get(key.as_bytes()) {
            let mut first = true;
            let mut acc = Bitmap64::new();
            for label in labels.iter() {
                format_key_for_label_value(&mut key, &label.name, &label.value);
                if let Some(bmp) = inner.label_index.get(key.as_bytes()) {
                    if bmp.is_empty() {
                        break;
                    }
                    if first {
                        acc = measurement_bmp.and(bmp);
                        first = false;
                    } else {
                        acc.and_inplace(bmp);
                    }
                }
            }
            match acc.cardinality() {
                0 => Ok(None),
                1 => Ok(Some(acc.iter().next().unwrap())),
                _ => {
                    let metric_name = format_prometheus_metric_name(metric, labels);
                    // todo: show keys in the error message ?
                    let msg = format!("Multiple series with the same metric: {metric_name}");
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

    pub fn get_key_by_name_and_labels(&self, metric: &str, labels: &[Label]) -> TsdbResult<Option<KeyType>> {
        let possible_id = self.get_id_by_name_and_labels(metric, labels)?;
        match possible_id {
            Some(id) => {
                let inner = self.inner.read().unwrap();
                Ok(inner.id_to_key.get(&id).cloned())
            }
            None => Ok(None)
        }
    }

    pub(super) fn get_ids_by_metric_name(&self, metric: &str) -> Bitmap64 {
        let inner = self.inner.read().unwrap();
        let key = get_key_for_metric_name(metric);
        if let Some(bmp) = inner.label_index.get(key.as_bytes()) {
            bmp.clone()
        } else {
            Bitmap64::new()
        }
    }

    pub fn rename_series(&self, ctx: &Context, new_key: &ValkeyString) -> bool {
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
    pub(crate) fn get_label_value_bitmap<F>(
        &self,
        label: &str,
        predicate: F,
    ) -> Bitmap64
    where F: Fn(&str) -> bool
    {
        let mut bitmap = Bitmap64::new();
        self.process_label_values(label, &mut bitmap, predicate, |ctx, _value, map| {
            ctx.or_inplace(map);
            Continue::<Option<()>>(())
        });
        bitmap
    }

    /// Returns a list of all values for the given label
    pub fn get_label_values(&self, label: &str) -> BTreeSet<String> {
        let inner = self.inner.read().unwrap();
        let prefix = get_key_for_label_prefix(label);
        let split_pos = prefix.len();
        let mut result: BTreeSet<String> = BTreeSet::new();

        for value in inner.label_index.prefix(prefix.as_bytes())
            .map(|(key, _)| key.sub_string(split_pos)) {
            result.insert(value.to_string());
        }

        result
    }

    pub fn is_series_indexed(&self, id: u64) -> bool {
        let inner = self.inner.read().unwrap();
        inner.id_to_key.contains_key(&id)
    }

    pub fn is_key_indexed(&self, key: &str) -> bool {
        let inner = self.inner.read().unwrap();
        let key = get_key_for_metric_name(key);
        inner.label_index.contains_key(key.as_bytes())
    }

    /// Returns a list of all series matching `matchers` while having samples in the range
    /// [`start`, `end`]
    pub(crate) fn series_ids_by_matchers(&self, matchers: &[Matchers]) -> Bitmap64 {
        let inner = self.inner.read().unwrap();
        inner.series_ids_by_matchers(matchers)
    }

    /// Returns a list of all series matching `matchers`
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

    pub fn with_series_by_matchers<F, STATE>(
        &self,
        ctx: &Context,
        matcher: Matchers,
        start_ts: Timestamp,
        end_ts: Timestamp,
        state: &mut STATE,
        f: F,
    ) -> ValkeyResult<()>
    where F: Fn(&mut STATE, &mut TimeSeries),  // todo: return ControlFLow
    {
        let keys = self.series_keys_by_matchers(ctx, &[matcher]);
        for key in keys.iter() {
            let redis_key = ctx.open_key_writable(key);
            let series = redis_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)?;
            if let Some(series) = series {
                if series.overlaps(start_ts, end_ts) {
                    f(state, series);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn find_ids_by_matchers(&self, matchers: &Matchers) -> Bitmap64 {
        let inner = self.inner.read().unwrap();
        let mut dest = Bitmap64::new();
        find_ids_by_matchers(&inner.label_index, matchers, &mut dest);
        dest
    }

    // Compiles the given matchers to optimized matchers. Incurs some setup overhead, so use this in the following cases: s
    // * the queries are complex.
    // * the labels being matched have high cardinality
    pub fn get_ids_by_matchers_optimized(&self, matchers: &Matchers) -> Bitmap64 {
        let inner = self.inner.read().unwrap();
        let mut dest = Bitmap64::new();
        get_ids_by_matchers_optimized(&inner.label_index, matchers, &mut dest);
        dest
    }

    pub fn get_series_count_by_metric_name(&self, limit: usize, start: Option<&str>) -> Vec<(ValkeyValueKey, usize)> {
        let inner = self.inner.read().unwrap();
        let prefix = get_key_for_label_value(METRIC_NAME_LABEL, start.unwrap_or(""));
        let prefix_len = prefix.len();
        inner.label_index
            .prefix(prefix.as_bytes())
            .filter_map(|(key,  map)| {
                // keys and values are expected to be utf-8. If we panic, we have bigger issues
                let key = key.sub_string(prefix_len);
                let k = ValkeyValueKey::from(key);
                Some((k, map.cardinality() as usize))
            })
            .take(limit)
            .collect()
    }

    pub fn process_label_values<T, CONTEXT, PRED, F>(
        &self,
        label: &str,
        ctx: &mut CONTEXT,
        predicate: PRED,
        f: F
    ) -> Option<T>
    where F: Fn(&mut CONTEXT, &str, &Bitmap64) -> ControlFlow<Option<T>>,
        PRED: Fn(&str) -> bool
    {
        let inner = self.inner.read().unwrap();
        inner.process_label_values(label, ctx, predicate, f)
    }

    pub(crate) fn get_inner(&self) -> RwLockReadGuard<IndexInner> {
        self.inner.read().unwrap()
    }
}


fn filter_by_label_value_predicate(
    label_index: &ARTBitmap,
    dest: &mut Bitmap64,
    op: SetOperation,
    label: &str,
    predicate: impl Fn(&str) -> bool,
) {
    let prefix = get_key_for_label_prefix(label);
    let start_pos = prefix.len();
    let iter = label_index
        .prefix(prefix.as_bytes())
        .filter_map(|(key, map)| {
            let value = key.sub_string(start_pos);
            if predicate(&value) {
                Some(map)
            } else {
                None
            }
        });

    process_iterator(iter, dest, op);
}

fn find_ids_by_label_filter(
    label_index: &ARTBitmap,
    filter: &LabelFilter,
    dest: &mut Bitmap64,
    op: SetOperation,
    key_buf: &mut String,
) {
    use LabelFilterOp::*;

    match filter.op {
        Equal => {
            key_buf.clear();
            format_key_for_label_value(key_buf, &filter.label, &filter.value);
            process_equals_match(label_index, key_buf, dest, op);
        }
        NotEqual => {
            let predicate = |value: &str| value != filter.value;
            filter_by_label_value_predicate(label_index, dest, op, &filter.label, predicate)
        }
        RegexEqual => {
            // todo: return Result. However if we get an invalid regex here,
            // we have a problem with the base metricsql library.
            let regex = regex::Regex::new(&filter.value).unwrap();
            let predicate = |value: &str| regex.is_match(value);
            filter_by_label_value_predicate(label_index, dest, op, &filter.label, predicate)
        }
        RegexNotEqual => {
            // todo: return Result. However if we get an invalid regex here,
            // we have a problem with the base metricsql library.
            let regex = regex::Regex::new(&filter.value).unwrap();
            let predicate = |value: &str| !regex.is_match(value);
            filter_by_label_value_predicate(label_index, dest, op, &filter.label, predicate)
        }
    }
}

fn find_ids_by_multiple_filters(
    label_index: &ARTBitmap,
    filters: &[LabelFilter],
    dest: &mut Bitmap64,
    operation: SetOperation,
    key_buf: &mut String, // used to minimize allocations
) {
    for filter in filters.iter() {
        find_ids_by_label_filter(label_index, filter, dest, operation, key_buf);
    }
}

fn find_ids_by_matchers(
    label_index: &ARTBitmap,
    matchers: &Matchers,
    dest: &mut Bitmap64
) {
    let mut key_buf = String::with_capacity(64);

    if !matchers.matchers.is_empty() {
        find_ids_by_multiple_filters(label_index, &matchers.matchers, dest, SetOperation::Intersection, &mut key_buf);
    }

    if !matchers.or_matchers.is_empty() {
        for filter in matchers.or_matchers.iter() {
            find_ids_by_multiple_filters(label_index, filter, dest, SetOperation::Union, &mut key_buf);
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Label;
    use crate::storage::time_series::TimeSeries;
    use metricsql_parser::prelude::parse_metric_name;

    fn create_series_from_metric_name(prometheus_name: &str) -> TimeSeries {
        let mut ts = TimeSeries::new();
        let labels = parse_metric_name(prometheus_name).unwrap();
        for label in labels.into_iter() {
            if label.name == METRIC_NAME_LABEL {
                ts.metric_name = label.value;
            } else {
                ts.labels.push(label);
            }
        }
        ts.id = TimeSeriesIndex::next_id();
        ts
    }

    fn create_series(metric_name: &str, labels: Vec<Label>) -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.id = TimeSeriesIndex::next_id();
        ts.metric_name = metric_name.to_string();
        ts.labels = labels;
        ts
    }

    #[test]
    fn test_index_time_series() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_time_series(&ts, b"time-series-1");

        assert_eq!(index.series_count(), 1);
        assert_eq!(index.label_count(), 3); // metric_name + region + env
    }

    #[test]
    fn test_reindex_time_series() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_time_series(&ts, b"time-series-1");

        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="prod"}"#);
        index.reindex_timeseries(&ts, b"time-series-1");

        assert_eq!(index.series_count(), 2);
        assert_eq!(index.label_count(), 3); // metric_name + region + env
    }

    #[test]
    fn test_remove_time_series() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_time_series(&ts, b"time-series-1");
        assert_eq!(index.series_count(), 1);

        index.remove_series(&ts);

        assert_eq!(index.series_count(), 0);
        assert_eq!(index.label_count(), 0);
    }

    #[test]
    fn test_get_label_values() {
        let index = TimeSeriesIndex::new();
        let ts1 = create_series("latency", vec![
            Label { name: "region".to_string(), value: "us-east1".to_string() },
            Label { name: "env".to_string(), value: "dev".to_string() },
        ]);
        let ts2 = create_series("latency", vec![
            Label { name: "region".to_string(), value: "us-east2".to_string() },
            Label { name: "env".to_string(), value: "qa".to_string() },
        ]);

        index.index_time_series(&ts1, b"time-series-1");
        index.index_time_series(&ts2, b"time-series-2");

        let values = index.get_label_values("region");
        assert_eq!(values.len(), 2);
        assert!(values.contains("us-east1"));
        assert!(values.contains("us-east2"));

        let values = index.get_label_values("env");
        assert_eq!(values.len(), 2);
        assert!(values.contains("dev"));
        assert!(values.contains("qa"));
    }

    #[test]
    fn test_get_id_by_name_and_labels() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_time_series(&ts, b"time-series-1");

        let id = index.get_id_by_name_and_labels("latency", &ts.labels).unwrap();
        assert_eq!(id, Some(ts.id));
    }

    #[test]
    fn test_prometheus_name_exists() {
        let index = TimeSeriesIndex::new();
        let ts = create_series("latency", vec![
            Label { name: "region".to_string(), value: "us-east1".to_string() },
            Label { name: "env".to_string(), value: "qa".to_string() },
        ]);

        index.index_time_series(&ts, b"time-series-1");

        assert!(index.prometheus_name_exists("latency", &ts.labels));
    }
}
