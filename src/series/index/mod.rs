
mod timeseries_index;
mod index_key;
pub mod serialization;
#[cfg(test)]
mod index_tests;
#[cfg(test)]
mod postings_tests;
mod postings;

use crate::common::get_current_db;
use crate::module::VKM_SERIES_TYPE;
use crate::series::TimeSeries;
use papaya::{Guard, HashMap};
use rayon::iter::{ParallelBridge, ParallelIterator};
use std::sync::LazyLock;
use ahash::AHashSet;
pub use timeseries_index::*;
pub use postings::*;
pub use metricsql_parser::label::{Matcher, Matchers};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

/// Map from db to TimeseriesIndex
pub type TimeSeriesIndexMap = HashMap<i32, TimeSeriesIndex>;

pub(crate) static TIMESERIES_INDEX: LazyLock<TimeSeriesIndexMap> = LazyLock::new(TimeSeriesIndexMap::new);

#[inline]
pub fn get_timeseries_index_for_db(db: i32, guard: &impl Guard) -> &TimeSeriesIndex {
    TIMESERIES_INDEX.get_or_insert_with(db, TimeSeriesIndex::new, guard)
}

pub fn with_timeseries_index<F, R>(ctx: &Context, f: F) -> R
where
    F: FnOnce(&TimeSeriesIndex) -> R,
{
    let db = get_current_db(ctx);
    let guard = TIMESERIES_INDEX.guard();
    let index = get_timeseries_index_for_db(db, &guard);
    let res = f(index);
    drop(guard);
    res
}

pub(crate) fn with_matched_series<F, STATE>(ctx: &Context, acc: &mut STATE, matchers: &[Matchers], mut f: F) -> ValkeyResult<()>
where
    F: FnMut(&mut STATE, &TimeSeries, ValkeyString) -> ValkeyResult<()>,
{
    with_timeseries_index(ctx, move |index| {
        let keys = series_keys_by_matchers(ctx, index, matchers)?;
        if keys.is_empty() {
            return Err(ValkeyError::Str("ERR no series found"));
        }
        for key in keys {
            let db_key = ctx.open_key(&key);
            if let Some(series) = db_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)? {
                f(acc, series, key)?
            }
        }
        Ok(())
    })
}

pub fn series_keys_by_matchers(ctx: &Context,
                               ts_index: &TimeSeriesIndex,
                               matchers: &[Matchers]) -> ValkeyResult<AHashSet<ValkeyString>> {

    // todo: rayon ?
    let mut key_set = AHashSet::new();
    for matcher in matchers {
        let keys = ts_index.series_keys_by_matchers(ctx, matcher)?;
        key_set.extend(keys);
    }

    Ok(key_set)
}

// todo: move elsewhere
pub fn clear_timeseries_index(ctx: &Context) {
    let db = get_current_db(ctx);
    TIMESERIES_INDEX.pin().remove(&db);
}

pub fn clear_all_timeseries_indexes() {
    TIMESERIES_INDEX.pin().clear();
}

pub fn swap_timeseries_index_dbs(from_db: i32, to_db: i32) {
    let map = TIMESERIES_INDEX.pin();
    let from = map.remove(&from_db);
    let to = map.remove(&from_db);

    // change this if https://github.com/ibraheemdev/papaya/issues/29 is resolved
    if let Some(to) = to {
        map.insert(from_db, to.clone());
    }
    if let Some(from) = from {
        map.insert(to_db, from.clone());
    }
}

pub fn optimize_all_timeseries_indexes() {
    let guard = TIMESERIES_INDEX.guard();
    let values: Vec<_> = TIMESERIES_INDEX.values(&guard).collect();
    values.into_iter().par_bridge().for_each(|index| {
        index.optimize(false);
    });
    guard.flush();
}