use crate::common::types::{Label, Sample, Timestamp};
use enquote::enquote;
use crate::series::types::ValueFilter;

#[inline]
pub(crate) fn filter_samples_by_date_range(samples: &mut Vec<Sample>, start: Timestamp, end: Timestamp) {
    samples.retain(|s| s.timestamp >= start && s.timestamp <= end)
}

#[inline]
pub(crate) fn filter_samples_by_value(samples: &mut Vec<Sample>, value_filter: &ValueFilter) {
    samples.retain(|s| s.value >= value_filter.min && s.value <= value_filter.max)
}

/// Find the index of the first element of `arr` that is greater
/// or equal to `val`.
/// Assumes that `arr` is sorted.
pub fn find_first_ge_index<T>(arr: &[T], val: &T) -> usize
where
    T: Ord,
{
    if arr.len() <= 16 {
        // If the vectors are small, perform a linear search.
        return arr.iter().position(|x| x >= val).unwrap_or(arr.len());
    }
    arr.binary_search(val).unwrap_or_else(|x| x)
}

/// Find the index of the first element of `arr` that is greater
/// than `val`.
/// Assumes that `arr` is sorted.
pub fn find_first_gt_index<T>(arr: &[T], val: T) -> usize
where
    T: Ord,
{
    match arr.binary_search(&val) {
        Ok(x) => x + 1,
        Err(x) => x,
    }
}

pub fn find_last_ge_index<T: Ord>(arr: &[T], val: &T) -> usize {
    if arr.len() <= 16 {
        return match arr.iter().rposition(|x| val >= x) {
            Some(idx) => {
                if arr[idx] > *val {
                    idx.saturating_sub(1)
                } else {
                    idx
                }
            },
            None => 0,
        }
    }
    arr.binary_search(val).unwrap_or_else(|x| x.saturating_sub(1))
}

/// Finds the start and end indices of timestamps within a specified range.
///
/// This function searches for the indices of timestamps that fall within the given
/// start and end timestamps (inclusive).
///
/// # Parameters
///
/// * `timestamps`: A slice of i64 values representing timestamps, expected to be sorted.
/// * `start_ts`: The lower bound of the timestamp range to search for (inclusive).
/// * `end_ts`: The upper bound of the timestamp range to search for (inclusive).
///
/// # Returns
///
/// Returns `Option<(usize, usize)>`:
/// * `Some((start_index, end_index))` if valid indices are found within the range.
/// * `None` if the input `timestamps` slice is empty.
///
/// The returned indices can be used to slice the original `timestamps` array
/// to get the subset of timestamps within the specified range.
pub(crate) fn get_timestamp_index_bounds(timestamps: &[i64], start_ts: Timestamp, end_ts: Timestamp) -> Option<(usize, usize)> {
    get_index_bounds(timestamps, &start_ts, &end_ts)
}

/// Finds the start and end indices (inclusive) of a range within a sorted slice.
///
/// # Parameters
///
/// * `values`: A slice of ordered elements to search within.
/// * `start`: The lower bound of the range to search for.
/// * `end`: The upper bound of the range to search for.
///
/// # Returns
///
/// Returns `Option<(usize, usize)>`:
/// * `Some((start_idx, end_idx))` if valid indices are found within the range.
/// * `None` if the `values` slice is empty, if all samples are less than `start`,
///   or if `start` and `end` are equal and greater than the sample at the found index.
///
/// Used to get an inclusive bounds for the slice (all elements in slice[start_index...=end_index]
/// satisfy the condition x >= start &&  <= end).
pub(crate) fn get_index_bounds<T: Ord>(values: &[T], start: &T, end: &T) -> Option<(usize, usize)> {
    if values.is_empty() {
        return None;
    }

    let len = values.len();

    let start_idx = find_first_ge_index(values, start);
    if start_idx >= len {
        return None;
    }

    let right = &values[start_idx..];
    let idx = find_last_ge_index(right, end);
    let end_idx = start_idx + idx;

    // imagine this scenario:
    // samples = &[10, 20, 30, 40]
    // start = 25, end = 25
    // we have a situation where start_index == end_index (2), yet samples[2] is greater than end,
    if start_idx == end_idx {
        // todo: get_unchecked
        if values[start_idx] > *end {
            return None;
        }
    }

    Some((start_idx, end_idx))
}


pub(crate) fn get_sample_index_bounds(samples: &[Sample], start_ts: Timestamp, end_ts: Timestamp) -> Option<(usize, usize)> {

    let start_sample = Sample { timestamp: start_ts, value: 0.0 };
    let end_sample = Sample { timestamp: end_ts, value: 0.0 };

    get_index_bounds(samples, &start_sample, &end_sample)
}

pub fn trim_to_range_inclusive(timestamps: &mut Vec<i64>, values: &mut Vec<f64>, start_ts: Timestamp, end_ts: Timestamp) {
    if let Some((start_idx, end_idx)) = get_timestamp_index_bounds(timestamps, start_ts, end_ts) {
        if start_idx == end_idx {
            // todo: get_unchecked
            let ts = timestamps[start_idx];
            let value = values[start_idx];
            timestamps.clear();
            values.clear();
            timestamps.push(ts);
            values.push(value);
        }
        if start_idx > 0 {
            timestamps.drain(..start_idx);
            values.drain(..start_idx);
        }
        let new_len = end_idx - start_idx + 1;
        timestamps.truncate(new_len);
        values.truncate(new_len);
    } else {
        timestamps.clear();
        values.clear();
    }
}

pub fn format_prometheus_metric_name_into(full_name: &mut String, name: &str, labels: &[Label]) {
    full_name.push_str(name);
    if !labels.is_empty() {
        full_name.push('{');
        for (i, label) in labels.iter().enumerate() {
            full_name.push_str(&label.name);
            full_name.push_str("=\"");
            // avoid allocation if possible
            if label.value.contains('"') {
                let quoted_value = enquote('\"', &label.value);
                full_name.push_str(&quoted_value);
            } else {
                full_name.push_str(&label.value);
            }
            full_name.push('"');
            if i < labels.len() - 1 {
                full_name.push(',');
            }
        }
        full_name.push('}');
    }
}

pub fn format_prometheus_metric_name(name: &str, labels: &[Label]) -> String {
    let size_hint = name.len() + labels.iter()
        .map(|l| l.name.len() + l.value.len() + 3).sum::<usize>();
    let mut full_name: String = String::with_capacity(size_hint);
    format_prometheus_metric_name_into(&mut full_name, name, labels);
    full_name
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_timestamp_index_empty() {
        let timestamps = vec![];
        assert_eq!(get_timestamp_index(&timestamps, 0), None);
        assert_eq!(get_timestamp_index(&timestamps, 1), None);
        assert_eq!(get_timestamp_index(&timestamps, 100), None);
    }

    #[test]
    fn get_timestamp_index_found() {
        let timestamps = vec![1, 2, 3, 4, 5];
        assert_eq!(get_timestamp_index(&timestamps, 1), Some(0));
        assert_eq!(get_timestamp_index(&timestamps, 2), Some(1));
        assert_eq!(get_timestamp_index(&timestamps, 3), Some(2));
        assert_eq!(get_timestamp_index(&timestamps, 4), Some(3));
        assert_eq!(get_timestamp_index(&timestamps, 5), Some(4));
    }

    #[test]
    fn get_timestamp_index_not_found() {
        let timestamps = vec![1, 2, 3, 4, 5, 10];
        assert_eq!(get_timestamp_index(&timestamps, 0), Some(0));
        assert_eq!(get_timestamp_index(&timestamps, 6), Some(5));
        assert_eq!(get_timestamp_index(&timestamps, 100), None);
    }

    #[test]
    fn trim_to_range_inclusive_all_before_start_ts() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 10;
        let end_ts = 20;

        trim_to_range_inclusive(&mut timestamps, &mut values, start_ts, end_ts);

        assert!(timestamps.is_empty());
        assert!(values.is_empty());
    }

    #[test]
    fn trim_to_range_inclusive_within_range() {
        let mut timestamps = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let start_ts = 3;
        let end_ts = 8;

        trim_to_range_inclusive(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![3, 4, 5, 6, 7, 8]);
        assert_eq!(values, vec![3.0, 4.0, 5.0, 6.0, 7.0, 8.0]);
    }

    #[test]
    fn trim_to_range_inclusive_all_within_range() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 0;
        let end_ts = 6;

        trim_to_range_inclusive(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![1, 2, 3, 4, 5]);
        assert_eq!(values, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    }

    #[test]
    fn trim_to_range_inclusive_start_ts_equals_end_ts() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 3;
        let end_ts = 3;

        trim_to_range_inclusive(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![3]);
        assert_eq!(values, vec![3.0]);
    }

}