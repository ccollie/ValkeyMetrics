use crate::common::types::Timestamp;

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

/// Retyrns the index of the first timestamp that is greater than or equal to `start_ts`.
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

pub fn trim_data<'a>(timestamps: &'a [i64], values: &'a [f64], start_ts: Timestamp, end_ts: Timestamp) -> (&'a [i64], &'a [f64]) {
    if let Some((start_idx, end_idx)) = get_timestamp_index_bounds(timestamps, start_ts, end_ts) {
        let stamps = &timestamps[0..];
        let timestamps = &stamps[start_idx..end_idx];
        let values = &values[start_idx..end_idx];
        (timestamps, values)
    } else {
        return (&[], &[]);
    }
}

// todo: needs test

pub fn trim_vec_data<'a>(timestamps: &mut Vec<i64>, values: &mut Vec<f64>, start_ts: Timestamp, end_ts: Timestamp) {
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
    } else {
        return;
    }
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