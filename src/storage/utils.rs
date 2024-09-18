use enquote::enquote;
use rand_distr::num_traits::Zero;
use crate::common::types::{Label, Timestamp};

trait ModuloSignedExt {
    fn modulo(&self, n: Self) -> Self;
}
#[macro_export]
macro_rules! modulo_signed_ext_impl {
    ($($t:ty)*) => ($(
        impl ModuloSignedExt for $t {
            #[inline]
            fn modulo(&self, n: Self) -> Self {
                (self % n + n) % n
            }
        }
    )*)
}
modulo_signed_ext_impl! { i8 i16 i32 i64 i128 }

/// Returns the index of the first timestamp that is greater than or equal to `start_ts`.
pub(crate) fn get_timestamp_index(timestamps: &[i64], start_ts: Timestamp) -> Option<usize> {
    if timestamps.is_empty() {
        return None;
    }

    let stamps = &timestamps[0..];
    let min_timestamp = stamps[0];
    let max_timestamp = stamps[stamps.len() - 1];
    if max_timestamp < start_ts {
        // Out of range.
        return None
    }

    let idx = if start_ts <= min_timestamp {
        0
    } else {
        stamps.binary_search(&start_ts).unwrap_or_else(|i| i)
    };

    Some(idx)
}

// TODO: test
pub fn round_to_significant_digits(x: f64, n: u32) -> f64 {
    if x.is_zero() || x.is_infinite() || x.is_nan() {
        return x;
    }
    if n == 0 || n >= 18 {
        return x;
    }
    let magnitude = x.abs().log10().floor();
    let power = 10.0_f64.powi(n as i32 - 1 - magnitude as i32);
    (x * power).round() / power
}

pub(crate) fn get_timestamp_index_bounds(timestamps: &[i64], start_ts: Timestamp, end_ts: Timestamp) -> Option<(usize, usize)> {
    if timestamps.is_empty() {
        return None;
    }

    let stamps = &timestamps[0..];

    let min_timestamp = stamps[0];
    let max_timestamp = stamps[stamps.len() - 1];
    if min_timestamp > end_ts || max_timestamp < start_ts {
        // Out of range.
        return None
    }

    let start_idx = if start_ts <= min_timestamp {
        0
    } else {
        stamps.binary_search(&start_ts).unwrap_or_else(|i| i)
    };

    let end_idx = if end_ts >= max_timestamp {
        stamps.len()
    } else {
        // todo: optimize by searching only stamps[start_idx..]
        stamps.binary_search(&end_ts).unwrap_or_else(|i| i)
    };


    Some((start_idx, end_idx))
}

// todo: needs test
// todo: this looks slow : need to optimize
pub fn trim_vec_data(timestamps: &mut Vec<i64>, values: &mut Vec<f64>, start_ts: Timestamp, end_ts: Timestamp) {
    if timestamps.is_empty() {
        return;
    }
    let last = timestamps[timestamps.len() - 1];
    if last < start_ts {
        timestamps.clear();
        values.clear();
        return;
    }

    if let Some((start_idx, end_idx)) = get_timestamp_index_bounds(timestamps, start_ts, end_ts) {
        let mut idx = 0;
        timestamps.retain(|_| {
            let keep = idx >= start_idx && idx < end_idx;
            idx += 1;
            keep
        });

        let mut idx = 0;
        values.retain(|_| {
            let keep = idx >= start_idx && idx < end_idx;
            idx += 1;
            keep
        });
    }
}


// returns the number of matches
pub(crate) fn filter_samples_by_ts<'a>(
    timestamps: &mut [i64],
    values: &mut [f64],
    by_ts_args: &'a [Timestamp]
) -> (usize, &'a [Timestamp]) {
    let mut count = 0;

    if by_ts_args.is_empty() {
        return (0, by_ts_args);
    }

    let last_ts = timestamps[timestamps.len() - 1];
    let first_ts = timestamps[0];

    let filters_len = by_ts_args.len();
    let last_ts_filter = by_ts_args[filters_len - 1];

    if first_ts > last_ts_filter {
        return (0, &by_ts_args[filters_len - 1..]);
    }

    if last_ts < by_ts_args[0] {
        return (0, by_ts_args);
    }

    let mut ts_filter_index = by_ts_args.binary_search(&first_ts).unwrap_or_else(|i| i);

    let mut i = 0;
    while i < timestamps.len() && ts_filter_index < filters_len {
        let stamps = &timestamps[i..];
        let filter_ts = by_ts_args[ts_filter_index];
        if stamps[stamps.len() - 1] > filter_ts {
            break;
        }
        match stamps.binary_search(&filter_ts) {
            Ok(idx) => {
                i += idx;
                timestamps[count] = timestamps[idx];
                values[count] = values[idx];
                count += 1;
            },
            Err(idx) => {
                i += idx;
            }
        }
        ts_filter_index += 1;
    }

    (count, &by_ts_args[ts_filter_index..])
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
    #[test]
    fn get_timestamp_index_empty() {
        let timestamps = vec![];
        assert_eq!(super::get_timestamp_index(&timestamps, 0), None);
        assert_eq!(super::get_timestamp_index(&timestamps, 1), None);
        assert_eq!(super::get_timestamp_index(&timestamps, 100), None);
    }

    #[test]
    fn get_timestamp_index_found() {
        let timestamps = vec![1, 2, 3, 4, 5];
        assert_eq!(super::get_timestamp_index(&timestamps, 1), Some(0));
        assert_eq!(super::get_timestamp_index(&timestamps, 2), Some(1));
        assert_eq!(super::get_timestamp_index(&timestamps, 3), Some(2));
        assert_eq!(super::get_timestamp_index(&timestamps, 4), Some(3));
        assert_eq!(super::get_timestamp_index(&timestamps, 5), Some(4));
    }

    #[test]
    fn get_timestamp_index_not_found() {
        let timestamps = vec![1, 2, 3, 4, 5, 10];
        assert_eq!(super::get_timestamp_index(&timestamps, 0), Some(0));
        assert_eq!(super::get_timestamp_index(&timestamps, 6), Some(5));
        assert_eq!(super::get_timestamp_index(&timestamps, 100), None);
    }
}