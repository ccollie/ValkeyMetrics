use super::index_key::*;
use crate::common::types::{IntMap, Label, LabelFilter, LabelFilterOp, Matchers};
use crate::common::METRIC_NAME_LABEL;
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::module::{with_timeseries, VKM_SERIES_TYPE};
use crate::series::chunks::utils::format_prometheus_metric_name;
use crate::series::index::querier::Postings;
use crate::series::time_series::{TimeSeries, TimeseriesId};
use cfg_if::cfg_if;
use metricsql_common::hash::FastHashSet;
use metricsql_parser::label::Matcher;
use rand::Rng;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::atomic::AtomicU64;
use std::sync::{LazyLock, RwLock, RwLockReadGuard};
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{logging, Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

// todo: move to config
pub const OPTIMIZE_CHANGE_THRESHOLD: usize = 1000;

cfg_if! {
    if #[cfg(feature = "id64")] {
        use xxhash_rust::xxh3::Xxh3 as IdHasher;
        pub(crate) use croaring::Bitmap64 as IdBitmap;
    } else {
        use xxhash_rust::xxh32::Xxh32 as IdHasher;
        pub(crate) use croaring::Bitmap as IdBitmap;
    }
}

/// Type for the key of the index. Use instead of `String` because Valkey keys are binary safe not utf8 safe.
pub type KeyType = Box<[u8]>;


// label
// label=value
pub type ARTBitmap = blart::TreeMap<IndexKey, IdBitmap>;

const EMPTY_BITMAP: LazyLock<IdBitmap> = LazyLock::new(|| IdBitmap::new());

#[derive(Clone, Copy)]
pub(crate) enum SetOperation {
    Union,
    Intersection,
}

impl PartialEq for SetOperation {
    fn eq(&self, other: &Self) -> bool {
        matches!((self, other), (SetOperation::Union, SetOperation::Union) | (SetOperation::Intersection, SetOperation::Intersection))
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct IndexInner {
    /// Map from timeseries id to timeseries key.
    pub id_to_key: IntMap<TimeseriesId, KeyType>,
    /// Map from label name and (label name,  label value) to set of timeseries ids.
    pub label_index: ARTBitmap,
    pub label_count: usize,
    pub changes_since_last_optimize: usize,
}

impl IndexInner {
    pub fn new() -> IndexInner {
        IndexInner {
            id_to_key: Default::default(),
            label_index: Default::default(),
            label_count: 0,
            changes_since_last_optimize: 0,
        }
    }

    fn clear(&mut self) {
        self.id_to_key.clear();
        self.label_index.clear();
        self.label_count = 0;
        self.changes_since_last_optimize = 0;
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

    fn remove_series_by_id(&mut self, id: TimeseriesId, metric_name: &str, labels: &[Label]) {
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

    fn index_series_by_metric_name(&mut self, ts_id: TimeseriesId, metric_name: &str) {
        self.index_series_by_label(ts_id, METRIC_NAME_LABEL, metric_name);
    }

    fn has_label(&self, label: &str) -> bool {
        let prefix = get_key_for_label_prefix(label);
        self.label_index.prefix(prefix.as_bytes()).next().is_some()
    }

    pub fn add_label_value(&mut self, label: &str, value: &str, ts_id: TimeseriesId) -> bool {
        let key = IndexKey::for_label_value(label, value);
        let result = if let Some(bmp) = self.label_index.get_mut(&key) {
            bmp.add(ts_id);
            false
        } else {
            let mut bmp = IdBitmap::new();
            bmp.add(ts_id);
            // TODO: !!!!!! handle error
            match self.label_index.try_insert(key, bmp).unwrap() {
                None => {
                    self.label_count += 1;
                    true
                },
                _ => false
            }
        };
        self.changes_since_last_optimize += 1;
        result
    }

    pub fn index_series_by_label(&mut self, ts_id: TimeseriesId, label: &str, value: &str) {
        self.add_label_value(label, value, ts_id);
    }

    fn remove_label_value(&mut self, label: &str, value: &str, ts_id: TimeseriesId) {
        let key = IndexKey::for_label_value(label, value);
        if let Some(bmp) = self.label_index.get_mut(&key) {
            bmp.remove(ts_id);
            if bmp.is_empty() {
                self.label_index.remove(&key);
                if !self.has_label(label) {
                    self.label_count -= 1;
                }
            }
            self.changes_since_last_optimize += 1;
        }
    }

    /// Returns a list of all series matching `matchers`
    fn series_ids_by_matchers(&self, matchers: &Matchers) -> TsdbResult<Cow<IdBitmap>> {
        if !matchers.matchers.is_empty() {
            return self.postings_for_matchers(&matchers.matchers);
        }

        if !matchers.or_matchers.is_empty() {
            let parallelize = should_parallelize_matchers(matchers);
            if parallelize {
                run_or_matchers_parallel(self, &matchers.or_matchers)
            } else {
                let mut acc = IdBitmap::new();
                for filter in matchers.or_matchers.iter() {
                    let postings = self.postings_for_matchers(filter)?;
                    acc.or_inplace(&*postings);
                }
                Ok(Cow::Owned(acc))
            }
        } else {
            Ok(Cow::Owned(IdBitmap::new()))
        }
    }


    /// Optimize the bitmap indexes
    fn optimize(&mut self, force: bool) {
        if force || self.changes_since_last_optimize > OPTIMIZE_CHANGE_THRESHOLD {
            for (_, bmp) in self.label_index.iter_mut() {
                bmp.run_optimize();
                let _ = bmp.shrink_to_fit();
            }
            self.changes_since_last_optimize = 0;
        }
        // todo: rayon ??
    }

    // `postings_for_matchers` assembles a single postings iterator against the index
    // based on the given matchers. The resulting postings are not ordered by series.
    pub fn postings_for_matchers(&self, ms: &[Matcher]) -> TsdbResult<Cow<IdBitmap>> {
        if ms.len() == 1 {
            let m = &ms[0];
            if m.label.is_empty() && m.label.is_empty() {
                return Ok(Cow::Owned(self.all_postings()));
            }
        }

        let mut sorted_matchers: SmallVec<(&Matcher, bool, bool), 4> = SmallVec::new();
        let mut not_its= Postings::new();

        let mut has_subtracting_matchers = false;
        let mut has_intersecting_matchers = false;

        // See which label must be non-empty.
        // Optimization for case like {l=~".", l!="1"}.
        let mut label_must_be_set: FastHashSet<String> = FastHashSet::with_capacity(ms.len());
        for m in ms {
            let matches_empty = m.is_match("");
            if !matches_empty {
                label_must_be_set.insert(m.label.clone());
            }
            let is_subtracting = is_subtracting_matcher(m, &label_must_be_set);

            has_subtracting_matchers |= is_subtracting;
            has_intersecting_matchers |= !is_subtracting;

            sorted_matchers.push((&m, matches_empty, is_subtracting))
        }

        let mut its = if has_subtracting_matchers && !has_intersecting_matchers {
            // If there's nothing to subtract from, add in everything and remove the not_its later.
            // We prefer to get AllPostings so that the base of subtraction (i.e. all_postings)
            // doesn't include series that may be added to the index reader during this function call.
            self.all_postings()
        } else {
            IdBitmap::new()
        };

        // Sort matchers to have the intersecting matchers first.
        // This way the base for subtraction is smaller and there is no chance that the set we subtract
        // from contains postings of series that didn't exist when we constructed the set we subtract by.
        sorted_matchers.sort_by(|i, j|-> Ordering {
            let is_i_subtracting = i.2;
            let is_j_subtracting = j.2;
            if !is_i_subtracting && is_j_subtracting {
                return Ordering::Less;
            }

            // i.cmp(&j)
            return Ordering::Greater;
        });

        for (m, matches_empty, _is_subtracting) in sorted_matchers {
            let value = &m.value;
            let name = &m.label;
            let typ = m.op;

            if name.is_empty() && value.is_empty() {
                // If the matchers for a label name selects an empty value, it selects all
                // the series which don't have the label name set too. See:
                //
                return Err(TsdbError::General(error_consts::MISSING_FILTER.into())) // todo: better error
            }

            if typ == LabelFilterOp::RegexEqual && value == ".*" {
                // .* regexp matches any string: do nothing.
                continue;
            }

            if typ == LabelFilterOp::RegexNotEqual && value == ".*" {
                return Ok(Cow::Owned(IdBitmap::default()))
            }

            if typ == LabelFilterOp::RegexEqual && value == ".+" {
                // .+ regexp matches any non-empty string: get postings for all label values.
                let it = self.postings_for_all_label_values(&m.label);
                if it.is_empty() {
                    return Ok(Cow::Owned(it))
                }
                its &= it;
            } else if typ == LabelFilterOp::RegexNotEqual && value == ".+" {
                // .+ regexp matches any non-empty string: get postings for all label values and remove them.
                let it = self.postings_for_all_label_values(name);
                not_its |= it;
                //its = append(not_its, it)
            } else if label_must_be_set.contains(name) {
                // If this matcher must be non-empty, we can be smarter.
                let is_not = typ == LabelFilterOp::NotEqual || m.op == LabelFilterOp::RegexNotEqual;

                if is_not {
                    // a failure here should probably panic
                    let inverse = m.inverse()
                        .map_err(|_| TsdbError::General(error_consts::INVALID_MATCHER.to_string()))?;

                    // If the label can't be empty and is a Not, then subtract it out at the end.
                    if matches_empty { // l!="foo"
                        // If the label can't be empty and is a Not and the inner matcher
                        // doesn't match empty, then subtract it out at the end.
                        let it = self.postings_for_matcher(&inverse);
                        not_its.or_inplace(&*it);
                    } else {
                        // l!=""
                        // If the label can't be empty and is a Not, but the inner matcher can
                        // be empty we need to use inverse_postings_for_matcher.
                        let it = self.inverse_postings_for_matcher(&inverse);
                        if it.is_empty() {
                            return Ok(it);
                        }
                        intersect(&mut its, &it);
                    }
                } else {
                    // l="a", l=~"a|b", l=~"a.b", etc.
                    // Non-Not matcher, use normal `postings_for_matcher`.
                    let it = self.postings_for_matcher(m);
                    if it.is_empty() {
                        return Ok(it);
                    }
                    intersect(&mut its, &it);
                }

            } else { // l=""
                // If the matchers for a label name selects an empty value, it selects all
                // the series which don't have the label name set too. See:
                // https://github.com/prometheus/prometheus/issues/3575 and
                // https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
                let it = self.inverse_postings_for_matcher(m);
                not_its.or_inplace(&*it);
            }
        }

        its -= &not_its;
        Ok(Cow::Owned(its))
    }

    pub fn postings_for_all_label_values(&self, label_name: &str) -> IdBitmap {
        let prefix = get_key_for_label_prefix(label_name);
        let mut result = IdBitmap::new();
        for (_, map) in self.label_index.prefix(prefix.as_bytes()) {
            result.or_inplace(map);
        }
        result
    }

    pub fn all_postings(&self) -> IdBitmap {
        const BUFFER_SIZE: usize = 64;
        let mut result = IdBitmap::new();
        // use chunks to minimize ffi calls
        let mut id_chunk: [TimeseriesId; BUFFER_SIZE] = [0; BUFFER_SIZE];
        let mut len = 0;
        for id in self.id_to_key.keys().copied() {
            id_chunk[len] = id;
            len += 1;
            if len % BUFFER_SIZE == 0 {
                result.add_many(&id_chunk);
                len = 0;
            }
        }
        if len > 0 {
            result.add_many(&id_chunk[0..len]);
        }
        result
    }

    /// `postings` returns the postings list iterator for the label pairs.
    /// The postings here contain the ids to the series inside the index.
    /// Found IDs are not strictly required to point to a valid Series, e.g.
    /// during background garbage collections.
    pub fn postings(&self, name: &str, values: &[String]) -> IdBitmap {
        let mut result = IdBitmap::new();
        for value in values {
            let key = IndexKey::for_label_value(name, value);
            if let Some(bmp) = self.label_index.get(&key) {
                result.or_inplace(bmp);
            }
        }
        result
    }

    pub fn postings_for_label_value<'a>(&'a self, name: &str, value: &str) -> Cow<'a, IdBitmap> {
        let key = IndexKey::for_label_value(name, value);
        if let Some(bmp) = self.label_index.get(&key) {
            Cow::Borrowed(bmp)
        } else {
            Cow::Owned(IdBitmap::default())
        }
    }

    pub fn postings_for_label_matching(&self, name: &str, match_fn: fn(&str) -> bool) -> IdBitmap {
        let prefix = get_key_for_label_prefix(name);
        let start_pos = prefix.len();
        let mut result = IdBitmap::new();
        for (key, map) in self.label_index.prefix(prefix.as_bytes()) {
            let value = key.sub_string(start_pos);
            if match_fn(value) {
                result.or_inplace(map);
            }
        }
        result
    }

    fn postings_for_matcher_internal(&self, matcher: &Matcher) -> IdBitmap {
        let mut result = IdBitmap::new();
        let prefix = get_key_for_label_prefix(&matcher.label);
        let start_pos = prefix.len();
        for (key, map) in self.label_index.prefix(prefix.as_bytes()) {
            let value = key.sub_string(start_pos);
            if matcher.is_match(value) {
                result.or_inplace(map);
            }
        }
        result
    }

    pub fn postings_for_matcher(&self, m: &Matcher) -> Cow<IdBitmap> {
        if m.label.is_empty() && m.value.is_empty() {
            return Cow::Owned(self.all_postings());
        }
        if m.op == LabelFilterOp::Equal {
            return self.postings_for_label_value(&m.label, &m.value);
        }
        if m.op == LabelFilterOp::RegexEqual {
            let set_matches = m.set_matches();
            if !set_matches.is_empty() {
                if set_matches.len() == 1 {
                    return self.postings_for_label_value(&m.label, &set_matches[0]);
                }
                return Cow::Owned(self.postings(&m.label, &set_matches));
            }
        }

        Cow::Owned(self.postings_for_matcher_internal(m))
    }

    fn inverse_postings_for_matcher(&self, m: &Matcher) -> Cow<IdBitmap> {
        if m.op == LabelFilterOp::RegexNotEqual {
            let set_matches = m.set_matches();
            if !set_matches.is_empty() {
                return Cow::Owned(self.postings(&m.label, &set_matches));
            }
        }

        if m.op == LabelFilterOp::NotEqual {
            return self.postings_for_label_value(&m.label, &m.value);
        }

        if m.value.is_empty() && (m.op == LabelFilterOp::RegexEqual || m.op == LabelFilterOp::Equal) {
            return Cow::Owned(self.postings_for_all_label_values(&m.label));
        }

        Cow::Owned(self.postings_for_matcher_internal(m))
    }

    pub fn process_label_values<T, CONTEXT, F, PRED>(
        &self,
        label: &str,
        ctx: &mut CONTEXT,
        predicate: PRED,
        f: F
    ) -> Option<T>
    where F: Fn(&mut CONTEXT, &str, &IdBitmap) -> ControlFlow<Option<T>>,
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
                    ControlFlow::Continue(_) => continue,
                }
            }
        }
        None
    }
}

#[inline]
fn intersect(dest: &mut IdBitmap, other: &IdBitmap) {
    if dest.is_empty() {
        dest.or_inplace(other);
    } else {
        dest.and_inplace(other);
    }
}

fn is_subtracting_matcher(m: &Matcher, label_must_be_set: &FastHashSet<String>) -> bool {
    if !label_must_be_set.contains(&m.label) {
        return true;
    }
    matches!(m.op, LabelFilterOp::NotEqual | LabelFilterOp::RegexNotEqual if m.is_match(""))
}

/// Index for quick access to timeseries by label, label value or metric name.
#[derive(Default)]
pub(crate) struct TimeSeriesIndex {
    pub(super) inner: RwLock<IndexInner>,
    pub(super) last_id: AtomicU64
}

impl Clone for TimeSeriesIndex {
    fn clone(&self) -> Self {
        let inner = self.inner.read().unwrap().clone();
        let id = self.last_id.load(std::sync::atomic::Ordering::Relaxed);
        TimeSeriesIndex {
            inner: RwLock::new(inner),
            last_id: AtomicU64::new(id)
        }
    }
}

impl TimeSeriesIndex {
    pub fn new() -> Self {
        TimeSeriesIndex {
            inner: RwLock::new(IndexInner::new()),
            last_id: AtomicU64::new(0)
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

    pub fn next_id(&self) -> TimeseriesId {
        let inner = self.inner.read().unwrap();
        const MAX_RETRIES: usize = 64;
        let mut counter = 0;
        loop {
            if counter >= MAX_RETRIES {
                return 0;
            }
            let current = self.last_id.load(std::sync::atomic::Ordering::Relaxed) as TimeseriesId;
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

    pub fn remove_series_by_id(&self, id: TimeseriesId, metric_name: &str, labels: &[Label]) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_series_by_id(id, metric_name, labels);
    }

    fn index_series_by_labels(&self, ts_id: TimeseriesId, labels: &[Label]) {
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
    pub fn get_id_by_name_and_labels(&self, metric: &str, labels: &[Label]) -> ValkeyResult<Option<TimeseriesId>> {
        let inner = self.inner.read()
            .map_err(|_| {
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
                    Err(ValkeyError::String(format!("Multiple series with the same metric: {metric_name}")))
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn prometheus_name_exists(&self, metric: &str, labels: &[Label]) -> bool {
        matches!(self.get_id_by_name_and_labels(metric, labels), Ok(Some(_)))
    }

    pub fn get_key_by_name_and_labels(&self, metric: &str, labels: &[Label]) -> ValkeyResult<Option<KeyType>> {
        if let Some(id)  = self.get_id_by_name_and_labels(metric, labels)? {
            let inner = self.inner.read()?;
            return Ok(inner.id_to_key.get(&id).cloned())
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
        with_timeseries(ctx, &old, | series | {
            let id = series.id;
            // slow, but we don't expect this to be called often
            let key = new_key.to_vec().into_boxed_slice();
            inner.id_to_key.insert(id, key);
            Ok(ValkeyValue::from(0i64))
        }).is_ok()
    }

    /// Return a bitmap of series ids that have the given label and pass the filter `predicate`.
    pub(crate) fn get_label_value_bitmap<F>(
        &self,
        label: &str,
        predicate: F,
    ) -> IdBitmap
    where F: Fn(&str) -> bool
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

        for value in inner.label_index.prefix(prefix.as_bytes())
            .map(|(key, _)| key.sub_string(split_pos)) {
            result.insert(value.to_string());
        }

        result
    }

    pub fn is_series_indexed(&self, id: TimeseriesId) -> bool {
        let inner = self.inner.read().unwrap();
        inner.id_to_key.contains_key(&id)
    }

    pub fn is_key_indexed(&self, key: &str) -> bool {
        let inner = self.inner.read().unwrap();
        let key = get_key_for_metric_name(key);
        inner.label_index.contains_key(key.as_bytes())
    }

    /// Returns a list of all series matching `matchers`
    pub(crate) fn series_keys_by_matchers(&self, ctx: &Context, matchers: &Matchers) -> TsdbResult<Vec<ValkeyString>> {
        let inner = self.inner.read().unwrap();
        let bitmap = inner.series_ids_by_matchers(matchers)?;
        let mut result: Vec<ValkeyString> = Vec::with_capacity(bitmap.cardinality() as usize);
        for id in bitmap.iter() {
            if let Some(value) = inner.id_to_key.get(&id) {
                let key = ctx.create_string(&value[0..]);
                result.push(key)
            }
        }
        Ok(result)
    }

    /// Returns a list of all series matching `matchers` while having samples in the range
    /// Primarily for unit testing outside valkey contexts
    pub(crate) fn series_keys_by_matchers_internal(&self, matchers: &Matchers) -> TsdbResult<Vec<KeyType>> {
        let inner = self.inner.read().unwrap();
        let bitmap = inner.series_ids_by_matchers(matchers)?;
        let mut result: Vec<KeyType> = Vec::with_capacity(bitmap.cardinality() as usize);
        for id in bitmap.iter() {
            if let Some(value) = inner.id_to_key.get(&id) {
                result.push(value.clone())
            }
        }
        Ok(result)
    }

    pub fn get_series_count_by_metric_name(&self, limit: usize, start: Option<&str>) -> Vec<(ValkeyValueKey, usize)> {
        let inner = self.inner.read().unwrap();
        let prefix = get_key_for_label_value(METRIC_NAME_LABEL, start.unwrap_or(""));
        let prefix_len = prefix.len();
        inner.label_index
            .prefix(prefix.as_bytes())
            .map(|(key,  map)| {
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
        f: F
    ) -> Option<T>
    where F: Fn(&mut CONTEXT, &str, &IdBitmap) -> ControlFlow<Option<T>>,
        PRED: Fn(&str) -> bool
    {
        let inner = self.inner.read().unwrap();
        inner.process_label_values(label, ctx, predicate, f)
    }

    pub(crate) fn get_inner(&self) -> RwLockReadGuard<IndexInner> {
        self.inner.read().unwrap()
    }

    pub(crate) fn optimize(&self, force: bool) {
        let mut inner = self.inner.write().unwrap();
        inner.optimize(force);
    }
}


fn run_or_matchers_parallel<'a>(label_index: &'a IndexInner,
                            matchers: &[Vec<LabelFilter>]) -> TsdbResult<Cow<'a, IdBitmap>> {
    let mut scope = chili::Scope::global();
    match matchers {
        [] => Ok(Cow::Owned(IdBitmap::new())),
        [matchers] => label_index.postings_for_matchers(&matchers),
        [m1, m2] => {
            let (r1, r2) = scope.join(
                |_| label_index.postings_for_matchers(&m1),
                |_| label_index.postings_for_matchers(&m2),
            );
            let mut r1 = r1?.into_owned();
            let r2 = r2?;
            r1.or_inplace(&*r2);
            Ok(Cow::Owned(r1))
        }
        [m1, m2, m3] => {
            let (x, (y, z)) = scope.join(
                |_| label_index.postings_for_matchers(&m1),
                |s2| s2.join(
                    |_| label_index.postings_for_matchers(&m2),
                    |_| label_index.postings_for_matchers(&m3),
                )
            );
            let mut x = x?.into_owned();
            let y = y?;
            let z = z?;
            x.or_inplace(&*y);
            x.or_inplace(&*z);
            Ok(Cow::Owned(x))
        }
        _ => {
            let mid = matchers.len() / 2;
            let (left, right) = matchers.split_at(mid);
            let (left_results, right_results) = scope.join(
                |_| run_or_matchers_parallel(label_index, left),
                |_| run_or_matchers_parallel(label_index, right)
            );
            let right_results = right_results?;
            let mut left_results = left_results?.into_owned();
            left_results.or_inplace(&*right_results);
            Ok(Cow::Owned(left_results))
        }
    }
}

// Placeholder for more reasonable heuristics
// e.g. if we have a filter that matches all postings, we should not parallelize
// and instead rely on set operations to optimize the query at each iteration
fn should_parallelize_matchers(matchers: &Matchers) -> bool {
    if !matchers.matchers.is_empty() {
        return matchers.matchers.len() > 1
    }
    if!matchers.or_matchers.is_empty() {
        return matchers.or_matchers.iter()
            .any(|m| m.len() > 3)
    }
    false
}


fn hash_timeseries(ts: &TimeSeries, state: &mut IdHasher, counter: usize) -> TimeseriesId {
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

    state.digest() as TimeseriesId
}

// todo: why not just use a snowflake id generator ?
fn generate_unique_id(ts: &TimeSeries, id_to_key: &IntMap<TimeseriesId, KeyType>) -> ValkeyResult<TimeseriesId> {
    const MAX_RETRIES: usize = 64;

    let mut hasher: IdHasher = Default::default();

    let mut counter: usize = 0;
    let mut id = hash_timeseries(ts, &mut hasher, counter);

    if !id_to_key.contains_key(&id) {
        return Ok(id);
    }

    let mut rng = rand::thread_rng();
    loop {
        id = hash_timeseries(ts, &mut hasher, counter);
        if id_to_key.contains_key(&id) {
            if counter >= MAX_RETRIES {
                return Err(ValkeyError::Str("Err - failed to generate unique id for time series"));
            }
            let ex: usize = rng.gen_range(1..64);
            counter = counter.wrapping_add(ex);
            continue;
        }
        return Ok(id)
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
        let mut ts1 = create_series("latency", vec![
            Label { name: "region".to_string(), value: "us-east1".to_string() },
            Label { name: "env".to_string(), value: "dev".to_string() },
        ]);
        let mut ts2 = create_series("latency", vec![
            Label { name: "region".to_string(), value: "us-east2".to_string() },
            Label { name: "env".to_string(), value: "qa".to_string() },
        ]);

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

        let id = index.get_id_by_name_and_labels("latency", &ts.labels).unwrap();
        assert_eq!(id, Some(ts.id));
    }

    #[test]
    fn test_prometheus_name_exists() {
        let index = TimeSeriesIndex::new();
        let mut ts = create_series("latency", vec![
            Label { name: "region".to_string(), value: "us-east1".to_string() },
            Label { name: "env".to_string(), value: "qa".to_string() },
        ]);

        index.index_time_series(&mut ts, b"time-series-1").unwrap();

        assert!(index.prometheus_name_exists("latency", &ts.labels));
    }
}
