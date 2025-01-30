use super::index_key::*;
use super::postings::Postings;
use super::{IdBitmap, IdHasher, KeyType, TimeSeries};
use crate::common::types::{IntMap, Label, Matchers};
use crate::common::METRIC_NAME_LABEL;
use crate::error::{TsdbError, TsdbResult};
use crate::module::{with_timeseries, VKM_SERIES_TYPE};
use crate::series::chunks::utils::format_prometheus_metric_name;
use crate::series::SeriesRef;
use rand::{rng, Rng};
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::atomic::AtomicU64;
use std::sync::{RwLock, RwLockReadGuard};
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{logging, Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// Index for quick access to timeseries by label, label value or metric name.
#[derive(Default)]
pub(crate) struct TimeSeriesIndex {
    pub(super) inner: RwLock<Postings>,
    pub(super) last_id: AtomicU64,
}

impl Clone for TimeSeriesIndex {
    fn clone(&self) -> Self {
        let inner = self.inner.read().unwrap().clone();
        let id = self.last_id.load(std::sync::atomic::Ordering::Relaxed);
        TimeSeriesIndex {
            inner: RwLock::new(inner),
            last_id: AtomicU64::new(id),
        }
    }
}

impl TimeSeriesIndex {
    pub fn new() -> Self {
        TimeSeriesIndex {
            inner: RwLock::new(Postings::new()),
            last_id: AtomicU64::new(0),
        }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.clear();
        self.last_id.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    // swap the inner value with some other value
    // this is specifically to handle the `swapdb` event callback
    // todo: can this deadlock ?
    pub fn swap(&mut self, other: &mut TimeSeriesIndex) {
        std::mem::swap(&mut self.inner, &mut other.inner);
        std::mem::swap(&mut self.last_id, &mut other.last_id);
    }

    pub fn label_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.label_count
    }
    pub fn series_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.id_to_key.len()
    }

    pub fn next_id(&self) -> SeriesRef {
        let inner = self.inner.read().unwrap();
        const MAX_RETRIES: usize = 64;
        let mut counter = 0;
        loop {
            if counter >= MAX_RETRIES {
                return 0;
            }
            let current = self.last_id.load(std::sync::atomic::Ordering::Relaxed) as SeriesRef;
            if inner.id_to_key.contains_key(&current) {
                counter += 1;
                continue;
            } else {
                if current == 0 {
                    self.last_id.store(1, std::sync::atomic::Ordering::Relaxed);
                }
                return current;
            }
        }
    }

    pub(crate) fn index_time_series(&self, ts: &mut TimeSeries, key: &[u8]) -> TsdbResult<()> {
        let mut inner = self.inner.write().unwrap();

        if ts.id == 0 {
            ts.id = generate_unique_id(ts, &inner.id_to_key)
                .map_err(|e| TsdbError::General(e.to_string()))?;
        }

        inner.index_time_series(ts, key);
        Ok(())
    }

    pub fn reindex_timeseries(&self, ts: &TimeSeries, key: &[u8]) {
        let mut inner = self.inner.write().unwrap();
        inner.reindex_timeseries(ts, key);
    }

    pub fn remove_series(&self, ts: &TimeSeries) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_series(ts);
    }

    pub fn remove_series_by_id(&self, id: SeriesRef, metric_name: &str, labels: &[Label]) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_series_by_id(id, metric_name, labels);
    }

    fn index_series_by_labels(&self, ts_id: SeriesRef, labels: &[Label]) {
        let mut inner = self.inner.write().unwrap();
        for Label { name, value } in labels.iter() {
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
    pub fn get_id_by_name_and_labels(
        &self,
        metric: &str,
        labels: &[Label],
    ) -> ValkeyResult<Option<SeriesRef>> {
        let inner = self.inner.read().map_err(|_| {
            logging::log_debug("Possible lock poison error reading timeseries index");
            ValkeyError::Str("Error reading index")
        })?;

        let mut key: String = String::new();
        format_key_for_metric_name(&mut key, metric);
        if let Some(measurement_bmp) = inner.label_index.get(key.as_bytes()) {
            let mut first = true;
            let mut acc = IdBitmap::new();
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
                1 => Ok(acc.iter().next()),
                _ => {
                    let metric_name = format_prometheus_metric_name(metric, labels);
                    Err(ValkeyError::String(format!(
                        "Multiple series with the same metric: {metric_name}"
                    )))
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn prometheus_name_exists(&self, metric: &str, labels: &[Label]) -> bool {
        matches!(self.get_id_by_name_and_labels(metric, labels), Ok(Some(_)))
    }

    pub fn get_key_by_name_and_labels(
        &self,
        metric: &str,
        labels: &[Label],
    ) -> ValkeyResult<Option<KeyType>> {
        if let Some(id) = self.get_id_by_name_and_labels(metric, labels)? {
            let inner = self.inner.read()?;
            return Ok(inner.id_to_key.get(&id).cloned());
        }
        Ok(None)
    }

    pub(crate) fn get_ids_by_metric_name(&self, metric: &str) -> IdBitmap {
        let inner = self.inner.read().unwrap();
        let key = get_key_for_metric_name(metric);
        if let Some(bmp) = inner.label_index.get(key.as_bytes()) {
            bmp.clone()
        } else {
            IdBitmap::new()
        }
    }

    pub fn rename_series(&self, ctx: &Context, old_key: &[u8], new_key: &[u8]) -> bool {
        let mut inner = self.inner.write().unwrap();
        let old = ctx.create_string(old_key);
        with_timeseries(ctx, &old, |series| {
            let id = series.id;
            // slow, but we don't expect this to be called often
            let key = new_key.to_vec().into_boxed_slice();
            inner.id_to_key.insert(id, key);
            Ok(ValkeyValue::from(0i64))
        })
        .is_ok()
    }

    /// Return a bitmap of series ids that have the given label and pass the filter `predicate`.
    pub(crate) fn get_label_value_bitmap<F>(&self, label: &str, predicate: F) -> IdBitmap
    where
        F: Fn(&str) -> bool,
    {
        let mut bitmap = IdBitmap::new();
        self.process_label_values(label, &mut bitmap, predicate, |ctx, _value, map| {
            ctx.or_inplace(map);
            ControlFlow::Continue::<Option<()>>(())
        });
        bitmap
    }

    /// Returns a list of all values for the given label
    pub fn get_label_values(&self, label: &str) -> BTreeSet<String> {
        let inner = self.inner.read().unwrap();
        let prefix = get_key_for_label_prefix(label);
        let split_pos = prefix.len();
        let mut result: BTreeSet<String> = BTreeSet::new();

        for value in inner
            .label_index
            .prefix(prefix.as_bytes())
            .map(|(key, _)| key.sub_string(split_pos))
        {
            result.insert(value.to_string());
        }

        result
    }

    pub fn is_series_indexed(&self, id: SeriesRef) -> bool {
        let inner = self.inner.read().unwrap();
        inner.id_to_key.contains_key(&id)
    }

    pub fn is_key_indexed(&self, key: &str) -> bool {
        let inner = self.inner.read().unwrap();
        let key = get_key_for_metric_name(key);
        inner.label_index.contains_key(key.as_bytes())
    }

    /// Returns a list of all series keys matching `matchers`
    pub(crate) fn series_keys_by_matchers(
        &self,
        ctx: &Context,
        matchers: &Matchers,
    ) -> TsdbResult<Vec<ValkeyString>> {
        let inner = self.inner.read().unwrap();
        let bitmap = inner.series_refs_by_matchers(matchers)?;
        let mut result: Vec<ValkeyString> = Vec::with_capacity(bitmap.cardinality() as usize);
        for id in bitmap.iter() {
            if let Some(value) = inner.id_to_key.get(&id) {
                let key = ctx.create_string(&value[0..]);
                result.push(key)
            }
        }
        Ok(result)
    }

    pub fn label_values_with_matchers(
        &self,
        name: &str,
        matchers: &Matchers,
    ) -> TsdbResult<Vec<String>> {
        let inner = self.inner.read().unwrap();
        if !matchers.matchers.is_empty() {
            return inner.label_values_with_matchers(name, &matchers.matchers);
        }

        if !matchers.or_matchers.is_empty() {
            let mut set = BTreeSet::new();
            for filter in matchers.or_matchers.iter() {
                let result = inner.label_values_with_matchers(name, filter)?;
                set.extend(result);
            }
            let result = set.into_iter().collect();
            Ok(result)
        } else {
            Ok(vec![])
        }
    }

    /// Returns a list of all series matching `matchers` while having samples in the range
    /// Primarily for unit testing outside valkey contexts
    pub(crate) fn series_keys_by_matchers_internal(
        &self,
        matchers: &Matchers,
    ) -> TsdbResult<Vec<KeyType>> {
        let inner = self.inner.read().unwrap();
        let bitmap = inner.series_refs_by_matchers(matchers)?;
        let mut result: Vec<KeyType> = Vec::with_capacity(bitmap.cardinality() as usize);
        for id in bitmap.iter() {
            if let Some(value) = inner.id_to_key.get(&id) {
                result.push(value.clone())
            }
        }
        Ok(result)
    }

    pub fn get_series_count_by_metric_name(
        &self,
        limit: usize,
        start: Option<&str>,
    ) -> Vec<(ValkeyValueKey, usize)> {
        let inner = self.inner.read().unwrap();
        let prefix = get_key_for_label_value(METRIC_NAME_LABEL, start.unwrap_or(""));
        let prefix_len = prefix.len();
        inner
            .label_index
            .prefix(prefix.as_bytes())
            .map(|(key, map)| {
                // keys and values are expected to be utf-8. If we panic, we have bigger issues
                let key = key.sub_string(prefix_len);
                let k = ValkeyValueKey::from(key);
                (k, map.cardinality() as usize)
            })
            .take(limit)
            .collect()
    }

    pub fn process_label_values<T, CONTEXT, PRED, F>(
        &self,
        label: &str,
        ctx: &mut CONTEXT,
        predicate: PRED,
        f: F,
    ) -> Option<T>
    where
        F: Fn(&mut CONTEXT, &str, &IdBitmap) -> ControlFlow<Option<T>>,
        PRED: Fn(&str) -> bool,
    {
        let inner = self.inner.read().unwrap();
        inner.process_label_values(label, ctx, predicate, f)
    }

    pub(crate) fn get_inner(&self) -> RwLockReadGuard<Postings> {
        self.inner.read().unwrap()
    }

    pub(crate) fn optimize(&self, force: bool) {
        let mut inner = self.inner.write().unwrap();
        inner.optimize(force);
    }
}

fn hash_timeseries(ts: &TimeSeries, state: &mut IdHasher, counter: usize) -> SeriesRef {
    #[cfg(not(feature = "id64"))]
    state.reset(0);

    #[cfg(feature = "id64")]
    state.reset();

    state.update(ts.metric_name.as_bytes());
    for Label { name, value } in &ts.labels {
        state.update(name.as_bytes());
        state.update(value.as_bytes());
    }
    state.update(counter.to_be_bytes().as_slice());

    state.digest() as SeriesRef
}

// todo: why not just use a snowflake id generator ?
fn generate_unique_id(
    ts: &TimeSeries,
    id_to_key: &IntMap<SeriesRef, KeyType>,
) -> ValkeyResult<SeriesRef> {
    const MAX_RETRIES: usize = 64;

    let mut hasher: IdHasher = Default::default();

    let mut counter: usize = 0;
    let mut id = hash_timeseries(ts, &mut hasher, counter);

    if !id_to_key.contains_key(&id) {
        return Ok(id);
    }

    let mut rng = rng();
    loop {
        id = hash_timeseries(ts, &mut hasher, counter);
        if id_to_key.contains_key(&id) {
            if counter >= MAX_RETRIES {
                return Err(ValkeyError::Str(
                    "Err - failed to generate unique id for time series",
                ));
            }
            let ex: usize = rng.random_range(1..64);
            counter = counter.wrapping_add(ex);
            continue;
        }
        return Ok(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Label;
    use crate::series::time_series::TimeSeries;
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
        ts
    }

    fn create_series(metric_name: &str, labels: Vec<Label>) -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.metric_name = metric_name.to_string();
        ts.labels = labels;
        ts
    }

    #[test]
    fn test_index_time_series() {
        let index = TimeSeriesIndex::new();
        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_time_series(&mut ts, b"time-series-1").unwrap();

        assert_eq!(index.series_count(), 1);
        assert_eq!(index.label_count(), 3); // metric_name + region + env
    }

    #[test]
    fn test_reindex_time_series() {
        let index = TimeSeriesIndex::new();
        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_time_series(&mut ts, b"time-series-1").unwrap();

        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="prod"}"#);
        index.reindex_timeseries(&ts, b"time-series-1");

        assert_eq!(index.series_count(), 2);
        assert_eq!(index.label_count(), 3); // metric_name + region + env
    }

    #[test]
    fn test_remove_time_series() {
        let index = TimeSeriesIndex::new();
        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_time_series(&mut ts, b"time-series-1").unwrap();
        assert_eq!(index.series_count(), 1);

        index.remove_series(&ts);

        assert_eq!(index.series_count(), 0);
        assert_eq!(index.label_count(), 0);
    }

    #[test]
    fn test_get_label_values() {
        let index = TimeSeriesIndex::new();
        let mut ts1 = create_series(
            "latency",
            vec![
                Label {
                    name: "region".to_string(),
                    value: "us-east1".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "dev".to_string(),
                },
            ],
        );
        let mut ts2 = create_series(
            "latency",
            vec![
                Label {
                    name: "region".to_string(),
                    value: "us-east2".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "qa".to_string(),
                },
            ],
        );

        index.index_time_series(&mut ts1, b"time-series-1").unwrap();
        index.index_time_series(&mut ts2, b"time-series-2").unwrap();

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
        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_time_series(&mut ts, b"time-series-1").unwrap();

        let id = index
            .get_id_by_name_and_labels("latency", &ts.labels)
            .unwrap();
        assert_eq!(id, Some(ts.id));
    }

    #[test]
    fn test_prometheus_name_exists() {
        let index = TimeSeriesIndex::new();
        let mut ts = create_series(
            "latency",
            vec![
                Label {
                    name: "region".to_string(),
                    value: "us-east1".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "qa".to_string(),
                },
            ],
        );

        index.index_time_series(&mut ts, b"time-series-1").unwrap();

        assert!(index.prometheus_name_exists("latency", &ts.labels));
    }
}
