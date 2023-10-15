use crate::ts::Timestamp;

pub(crate) fn get_timestamp_index_bounds(timestamps: &[i64], start_ts: Timestamp, end_ts: Timestamp) -> (usize, usize) {
    let stamps = &timestamps[0..];
    let start_idx = match stamps.binary_search(&start_ts) {
        Ok(idx) => idx,
        Err(idx) => idx,
    };

    let end_idx = match stamps.binary_search(&end_ts) {
        Ok(idx) => idx,
        Err(idx) => idx,
    };

    (start_idx, end_idx)
}

pub fn trim_data<'a>(timestamps: &'a [i64], values: &'a [f64], start_ts: Timestamp, end_ts: Timestamp) -> (&'a [i64], &'a [f64]) {
    let (start_idx, end_idx) = get_timestamp_index_bounds(timestamps, start_ts, end_ts);

    if start_idx > end_idx {
        return (&[], &[]);
    }

    let stamps = &timestamps[0..];
    let timestamps = &stamps[start_idx..end_idx];
    let values = &values[start_idx..end_idx];
    (timestamps, values)
}

// todo: needs test

pub fn trim_vec_data<'a>(timestamps: &mut Vec<i64>, values: &mut Vec<f64>, start_ts: Timestamp, end_ts: Timestamp) {
    let (start_idx, end_idx) = get_timestamp_index_bounds(timestamps, start_ts, end_ts);

    if start_idx > end_idx {
        return;
    }

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

pub struct Interval {
    start: i32,
    end: i32,
}

fn find_non_overlapping_intervals(intervals: &mut [Interval]) -> Vec<Interval> {
    // Sort the intervals by start time
    intervals.sort_by(|a, b| a.start.cmp(&b.start));

    let mut non_overlapping_intervals = Vec::new();

    // Traverse the intervals
    for i in 1..intervals.len() {
        // Previous interval end
        let prev_end = intervals[i - 1].end;

        // Current interval start
        let curr_start = intervals[i].start;

        // If ending index of previous is less than starting index of current,
        // then it is a non-overlapping interval
        if prev_end < curr_start {
            non_overlapping_intervals.push(Interval {
                start: prev_end,
                end: curr_start,
            });
        }
    }

    non_overlapping_intervals
}