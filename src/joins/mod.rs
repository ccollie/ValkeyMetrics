mod int_utils;
mod index;
mod data_elements;
mod time_utils;
mod joins;


/// MergeAsOfMode describes the roll behavior of the asof merge
pub enum MergeAsOfMode{ RollPrior, RollFollowing, NoRoll }

use crate::common::types::{Sample, Timestamp};
pub use data_elements::*;
pub use index::*;
pub use joins::*;
pub use int_utils::*;

use std::cmp;

pub type TDate = Timestamp;

/// This is similar to a left join except that it match on nearest key rather than equal keys similar to
/// <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.merge_asof.html>
///
/// # Example
///
/// ```
/// use super::MergeAsOfMode;
/// use super::TimeSeriesDataPoint;
/// let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
/// let index = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
/// let ts = TimeSeries::from_vecs(index.iter().map(|x| *x).collect(), values).unwrap();
/// let values2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
/// let index2 = vec![2, 4, 5, 7, 8, 10];
/// let ts_join = TimeSeries::from_vecs(index2.iter().map(|x| *x).collect(), values2).unwrap();
///
/// let result = ts.merge_apply_asof(&ts_join,Some(chrono_utils::merge_asof_prior(Duration::seconds(1))),|a,b| (*a, match b {
///     Some(x) => Some(*x),
///     None => None
/// }), MergeAsOfMode::RollPrior);
///
/// let expected = vec![
///     TimeSeriesDataPoint { timestamp: 1, value: (1.00, None) },
///     TimeSeriesDataPoint { timestamp: 2, value: (1.00, Some(1.00)) },
///     TimeSeriesDataPoint { timestamp: 3, value: (1.00, Some(1.00)) },
///     TimeSeriesDataPoint { timestamp: 4, value: (1.00, Some(2.00)) },
///     TimeSeriesDataPoint { timestamp: 5, value: (1.00, Some(3.00)) },
///     TimeSeriesDataPoint { timestamp: 6, value: (1.00, Some(3.00)) },
///     TimeSeriesDataPoint { timestamp: 7, value: (1.00, Some(4.00)) },
///     TimeSeriesDataPoint { timestamp: 8, value: (1.00, Some(5.00)) },
///     TimeSeriesDataPoint { timestamp: 9, value: (1.00, Some(5.00)) },
///     TimeSeriesDataPoint { timestamp: 10, value: (1.00, Some(6.00)) },
/// ];
///
/// let ts_expected = TimeSeries::from_tsdatapoints(expected).unwrap();
///
/// assert_eq!(result, ts_expected);
/// ```
pub fn merge_apply_asof<T>(left_samples: &Vec<Sample>,
                           other_samples: &Vec<Sample>,
                           compare_func: Option<Box<dyn Fn(&TDate,&TDate,&TDate)->(cmp::Ordering,i64)>>,
                           apply_func: fn(&f64, Option<&f64>) -> T,
                           merge_mode: MergeAsOfMode) -> Vec<TimeSeriesDataPoint<TDate, T>>
{
    #![allow(clippy::type_complexity)]
    #![allow(clippy::redundant_closure)]
    match merge_mode {
        MergeAsOfMode::NoRoll if compare_func.is_some() => panic!("you cannot have a roll function if you do not set a merge as of mode"),
        _ => ()
    };

    let left_indices: HashableIndex<Timestamp> = get_hashable_index(left_samples);
    let other_indices: HashableIndex<Timestamp> = get_hashable_index(other_samples);

    let je = JoinEngine{idx_this : &left_indices, idx_other : &other_indices};

    let other_idx_func:Option<Box<dyn Fn(usize)->usize>> = match merge_mode {
        MergeAsOfMode::RollFollowing => {
            let other_len = other_indices.len();
            Some(Box::new(move |idx: usize| fwd_func(idx, other_len)))
        },
        MergeAsOfMode::RollPrior => Some(Box::new(|idx: usize| prior_func(idx))),
        MergeAsOfMode::NoRoll => None
    };
    let indexes = je.get_asof_merge_joined_indices(compare_func, other_idx_func);
    //can make this parallel if you want...
    indexes.iter().map(|x|
        TimeSeriesDataPoint {
            timestamp: left_indices[x.this_idx],
            value : apply_func(
                &left_samples[x.this_idx].value,
                match x.other_idx {
                    Some(idx) => Some(&other_samples[idx].value),
                    None => None
                }
            )} )
        .collect()
}

fn get_hashable_index(samples: &Vec<Sample>) -> HashableIndex<Timestamp> {
    HashableIndex::new(samples.iter().map(|x| x.timestamp).collect())
}
