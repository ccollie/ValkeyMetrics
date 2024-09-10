//! # Utilities for ints
use std::cmp;

pub type MergeAsOfCompareFn = Box<dyn Fn(&i64, &i64, &i64) -> (cmp::Ordering, i64)>;

fn merge_asof_prior_impl(this: &i64, other: &i64, other_prior: &i64, lookback_ms: i64) -> (cmp::Ordering, i64) {
    let diff = this - other_prior;
    match diff {
        d if d < 0 && this != other => (cmp::Ordering::Less, 0),
        d if d > lookback_ms && this != other => (cmp::Ordering::Greater, 0),
        d if d <= lookback_ms && this != other => (cmp::Ordering::Equal, -1),
        _ => (cmp::Ordering::Equal, 0)
    }
}

fn merge_asof_fwd_impl(this: &i64, other: &i64, other_peak: &i64, lookback_ms: i64) -> (cmp::Ordering, i64) {
    let diff1 = other_peak - this;
    let diff2 = other - this;
    let diff = cmp::min(diff1, cmp::max(diff2, 0));
    let offset: i64 = if diff == diff2 { 0 } else { 1 };
    match diff {
        d if d < 0 && this != other => (cmp::Ordering::Greater, 0),
        d if d > lookback_ms && this != other => (cmp::Ordering::Less, 0),
        d if d <= lookback_ms && this != other => (cmp::Ordering::Equal, offset),
        _ => (cmp::Ordering::Equal, 0)
    }
}

// todo: Nearest

fn merge_asof_frontend(free_param: i64, func: fn(&i64, &i64, &i64, i64) -> (cmp::Ordering, i64)) -> Box<dyn Fn(&i64, &i64, &i64) -> (cmp::Ordering, i64)> {
    Box::new(move |this: &i64, other: &i64, other_peak: &i64| func(this, other, other_peak, free_param))
}

/// Implementation for mergeasof for a given duration lookback for a pair of Timeseries that has a HashableIndex<i64>
pub fn merge_asof_prior(look_back: i64) -> Box<dyn Fn(&i64, &i64, &i64) -> (cmp::Ordering, i64)> {
    merge_asof_frontend(look_back, merge_asof_prior_impl)
}
/// Implementation for mergeasof for a given duration look-forward for a pair of Timeseries that has a HashableIndex<i64>
pub fn merge_asof_fwd(look_fwd: i64) -> Box<dyn Fn(&i64, &i64, &i64) -> (cmp::Ordering, i64)> {
    merge_asof_frontend(look_fwd, merge_asof_fwd_impl)
}