/// The code in this file is largely copied from
// https://github.com/beignetbytes/tsxlib-rs
// License: Apache-2.0/MIT
use crate::common::types::{SampleLike, Timestamp};
use joinkit::EitherOrBoth;
use std::cmp;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use valkey_module::ValkeyError;

/// MergeAsOfMode describes the roll behavior of the asof merge
#[derive(Clone, Copy)]
pub enum MergeAsOfMode{ RollPrior, RollFollowing }

#[derive(Debug, Default, Clone, Copy)]
pub enum AsOfJoinStrategy {
    #[default]
    Prior,
    Next,
}

impl Display for AsOfJoinStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AsOfJoinStrategy::Next => write!(f, "Next"),
            AsOfJoinStrategy::Prior => write!(f, "Prior"),
        }
    }
}
impl FromStr for AsOfJoinStrategy {
    type Err = ValkeyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("forward") => Ok(AsOfJoinStrategy::Next),
            s if s.eq_ignore_ascii_case("next") => Ok(AsOfJoinStrategy::Next),
            s if s.eq_ignore_ascii_case("prior") => Ok(AsOfJoinStrategy::Prior),
            s if s.eq_ignore_ascii_case("backward") => Ok(AsOfJoinStrategy::Prior),
            _ => Err(ValkeyError::Str("invalid join direction")),
        }
    }
}

impl TryFrom<&str> for AsOfJoinStrategy {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let direction = value.to_lowercase();
        match direction.as_str() {
            "next" => Ok(AsOfJoinStrategy::Next),
            "prior" => Ok(AsOfJoinStrategy::Prior),
            _ => Err(ValkeyError::Str("invalid join direction")),
        }
    }
}

pub fn merge_apply_asof<'a, TSample: SampleLike + Clone + Eq + Ord>(
    left_samples: &'a [TSample], // todo: take impl Iterator<Item=Sample>
    other_samples: &'a [TSample],
    tolerance: &Duration,
    merge_mode: MergeAsOfMode) -> Vec<EitherOrBoth<&'a TSample, &'a TSample>>
{
    let tolerance_ms = tolerance.as_millis() as i64;
    let compare_func = match merge_mode {
        MergeAsOfMode::RollFollowing => Some(merge_asof_fwd(tolerance_ms)),
        MergeAsOfMode::RollPrior => Some(merge_asof_prior(tolerance_ms)),
    };

    let other_idx_func:Option<Box<dyn Fn(usize)->usize>> = match merge_mode {
        MergeAsOfMode::RollFollowing => {
            let other_len = other_samples.len();
            Some(Box::new(move |idx: usize| fwd_func(idx, other_len)))
        },
        MergeAsOfMode::RollPrior => Some(Box::new(|idx: usize| prior_func(idx))),
    };

    get_asof_merge_joined(
        left_samples,
        other_samples,
        compare_func,
        other_idx_func
    )
}

fn prior_func(idx: usize) -> usize {
    if idx == 0 {
        0
    } else {
        idx - 1
    }
}

fn fwd_func(idx: usize, other_len: usize) -> usize {
    if idx >= other_len - 1 {
        other_len - 1
    } else {
        idx + 1
    }
}

/// as of join. this is a variation of merge join that allows for timestamps to be equal based on a custom comparator func
fn get_asof_merge_joined<'a, TSample: SampleLike + Clone>(
    left: &'a [TSample],
    right: &'a [TSample],
    compare_func: Option<Box<dyn Fn(&Timestamp, &Timestamp, &Timestamp) -> (cmp::Ordering, i64)>>,
    other_idx_func: Option<Box<dyn Fn(usize) -> usize>>) -> Vec<EitherOrBoth<&'a TSample, &'a TSample>>
{
    #![allow(clippy::type_complexity)]
    let mut output: Vec<EitherOrBoth<&'a TSample, &'a TSample>> = Vec::new();
    let mut pos1: usize = 0;
    let mut pos2: usize = 0;

    let comp_func = match compare_func {
        Some(func) => func,
        None => Box::new(|this: &Timestamp, other: &Timestamp, _other_prior: &Timestamp| (this.cmp(other), 0)) // use built in ordinal compare if no override
    };

    let cand_idx_func = match other_idx_func {
        Some(func) => func,
        None => Box::new(|idx| idx)
    };

    while pos1 < left.len() {
        let first = &left[pos1];
        let second = &right[pos2];
        let first_ts = first.timestamp();
        let second_ts = second.timestamp();
        let comp_res = comp_func(
            &first_ts,
            &second_ts,
            &right[cand_idx_func(pos2)].timestamp()
        );
        let offset = comp_res.1;
        match comp_res.0 {
            // (Evaluated as,  but is actually)
            cmp::Ordering::Greater => {
                output.push(EitherOrBoth::Left(first));
                if pos2 < (right.len() - 1) {
                    pos1 += 1; //the first index might still be longer so we gotta keep rolling it forward even though we are out of space on the other index
                }
            }
            cmp::Ordering::Less => {
                output.push(EitherOrBoth::Left(first));
                pos1 += 1;
            }
            cmp::Ordering::Equal => {
                let pas64: i64 = pos2.try_into().unwrap();
                let idx0: i64 = pas64 + offset;
                if let Some(other) = right.get(idx0 as usize) {
                    output.push(EitherOrBoth::Both(first, other));
                } else {
                    output.push(EitherOrBoth::Left(first));
                }
                if first_ts.eq(&second_ts) && pos2 < (right.len() - 1) { // only incr if things are actually equal and you have room to run
                    pos2 += 1;
                }
                pos1 += 1;
            }
        }
    }
    output
}

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

fn merge_asof_frontend(free_param: i64, func: fn(&i64, &i64, &i64, i64) -> (cmp::Ordering, i64)) -> MergeAsOfCompareFn {
    Box::new(move |this: &i64, other: &i64, other_peak: &i64| func(this, other, other_peak, free_param))
}

/// Implementation for mergeasof for a given duration lookback for a pair of Timeseries that has a HashableIndex<i64>
fn merge_asof_prior(look_back: i64) -> MergeAsOfCompareFn {
    merge_asof_frontend(look_back, merge_asof_prior_impl)
}
/// Implementation for mergeasof for a given duration look-forward for a pair of Timeseries that has a HashableIndex<i64>
fn merge_asof_fwd(look_fwd: i64) -> MergeAsOfCompareFn {
    merge_asof_frontend(look_fwd, merge_asof_fwd_impl)
}

#[cfg(test)]
mod tests {
    use super::{merge_apply_asof, MergeAsOfMode};
    use crate::common::types::Timestamp;
    use joinkit::EitherOrBoth;
    use metricsql_runtime::types::Sample;
    use std::time::Duration;
    use crate::join::JoinValue;

    fn create_samples(timestamps: &[Timestamp], values: &[f64]) -> Vec<Sample> {
        timestamps.iter()
            .zip(values.iter())
            .map(|(t, v)| Sample::new(*t, *v))
            .collect()
    }

    fn convert_pair(value: &EitherOrBoth<&Sample, &Sample>) -> JoinValue {
        value.into()
    }

    #[test]
    fn test_merge_asof_prior() {

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let ts = create_samples(&index, &values);

        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let index2 = vec![2, 4, 5, 7, 8, 10];

        let ts_join = create_samples(&index2, &values2);

        let tolerance = Duration::from_secs(1);
        let actual = merge_apply_asof(&ts, &ts_join, &tolerance, MergeAsOfMode::RollPrior)
            .iter()
            .map(convert_pair)
            .collect::<Vec<_>>();

        let expected = vec![
            JoinValue::left(1, 1.0),
            JoinValue::both(2, 1.0, 1.0),
            JoinValue::both(3, 1.0, 1.0),
            JoinValue::both(4, 1.0, 2.0),
            JoinValue::both(5, 1.0, 3.0),
            JoinValue::both(6, 1.0, 3.0),
            JoinValue::both(7, 1.0, 4.0),
            JoinValue::both(8, 1.0, 5.0),
            JoinValue::both(9, 1.0, 5.0),
            JoinValue::both(10, 1.0, 6.0),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_merge_asof_forward(){

        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let ts = create_samples(&index, &values);

        let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let index2 = vec![2, 5, 6, 8, 10];

        let ts_join = create_samples(&index2, &values2);

        let tolerance = Duration::from_millis(1);

        let custom: Vec<_> = merge_apply_asof(&ts, &ts_join, &tolerance, MergeAsOfMode::RollFollowing)
            .iter()
            .map(convert_pair)
            .collect();

        let tolerance = Duration::from_millis(2);
        let custom2: Vec<_> = merge_apply_asof(&ts, &ts_join, &tolerance, MergeAsOfMode::RollFollowing)
            .iter()
            .map(convert_pair)
            .collect();

        let expected = vec![
            JoinValue::both(1, 1.0, 1.0),
            JoinValue::both(2, 1.0, 1.0),
            JoinValue::left(3, 1.0),
            JoinValue::both(4, 1.0, 2.0),
            JoinValue::both(5, 1.0, 2.0),
            JoinValue::both(6, 1.0, 3.0),
            JoinValue::both(7, 1.0, 4.0),
            JoinValue::both(8, 1.0, 4.0),
            JoinValue::both(9, 1.0, 5.0),
            JoinValue::both(10, 1.0, 5.0),
        ];

        let expected1 = vec![
            JoinValue::both(1, 1.0, 1.0),
            JoinValue::both(2, 1.0, 1.0),
            JoinValue::both(3, 1.0, 2.0),
            JoinValue::both(4, 1.0, 2.0),
            JoinValue::both(5, 1.0, 2.0),
            JoinValue::both(6, 1.0, 3.0),
            JoinValue::both(7, 1.0, 4.0),
            JoinValue::both(8, 1.0, 4.0),
            JoinValue::both(9, 1.0, 5.0),
            JoinValue::both(10, 1.0, 5.0),
        ];

        assert_eq!(custom, expected);
        assert_eq!(custom2, expected1);
    }
}