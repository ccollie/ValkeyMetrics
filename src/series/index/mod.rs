
mod timeseries_index;
#[cfg(test)]
mod index_tests;
mod filters;
mod index_key;
pub mod serialization;

use crate::common::get_current_db;
use crate::module::VKM_SERIES_TYPE;
use crate::series::TimeSeries;
use metricsql_parser::label::Matchers;
use papaya::{Guard, HashMap};
use rayon::iter::{ParallelBridge, ParallelIterator};
use smallvec::SmallVec;
use std::sync::LazyLock;
pub use timeseries_index::*;
use valkey_module::{Context, ValkeyString};

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

pub fn with_matched_series<F, R>(ctx: &Context, matchers: &[Matchers], f: F) -> R
where
    F: FnOnce(&[&TimeSeries]) -> R,
{
    let db = get_current_db(ctx);
    let guard = TIMESERIES_INDEX.guard();
    let index = get_timeseries_index_for_db(db, &guard);

    let keys = index.series_keys_by_matchers(ctx, matchers);

    if keys.is_empty() {
        return f(&[]);
    }

    // needed to keep valkey keys alive below
    let db_keys = keys
        .iter()
        .map(|key| ctx.open_key(key))
        .collect::<Vec<_>>();

    let mut time_series: SmallVec<&TimeSeries, 10> = SmallVec::new();

    for key in db_keys.iter() {
        if let Ok(Some(series)) = key.get_value::<TimeSeries>(&VKM_SERIES_TYPE) {
            time_series.push(series);
        }
    }

    let res = f(time_series.as_slice());

    drop(guard);
    res
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