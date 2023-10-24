use std::time::Duration;
use crate::common::types::Timestamp;
use crate::ts::utils::get_timestamp_index_bounds;

/// removes samples from src* if they are closer to each other than dedup_interval in milliseconds.
pub fn deduplicate_samples(
    src_timestamps: &mut Vec<Timestamp>,
    src_values: &mut Vec<f64>,
    start_ts: Timestamp,
    end_ts: Timestamp,
    dedup_interval: Duration,
) -> Option<Timestamp> {
    if src_timestamps.len() < 2 {
        return None;
    }
    let dedup_interval = dedup_interval.as_millis() as i64;

    let bounds = get_timestamp_index_bounds(src_timestamps, start_ts, end_ts);
    if bounds.is_none() {
        return None;
    }

    let (start_idx, end_idx) = bounds.unwrap();

    let src_ts = &mut src_timestamps[start_idx..];
    if src_ts.len() < 2 {
        return None;
    }

    if !needs_dedup(src_ts, dedup_interval) {
        // Fast path - nothing to deduplicate
        return None;
    }

    let mut ts_next = src_timestamps[start_idx] + dedup_interval - 1;
    ts_next = ts_next - (ts_next % dedup_interval);
    let mut j: usize = 0;
    let mut count = 0;
    let last_timestamp: Option<Timestamp> = None;

    // todo: eliminate bounds checks
    for i in start_idx + 1 ..end_idx {
        let ts = src_timestamps[i];
        if ts <= ts_next {
            continue;
        }

        src_timestamps[j] = ts;
        src_values.swap(j, i);
        j += 1;
        count += 1;

        ts_next += dedup_interval;
        if ts_next < ts {
            ts_next = ts + dedup_interval - 1;
            ts_next -= ts_next % dedup_interval
        }
    }

    let last = src_timestamps.len() - 1;
    src_timestamps.swap(count - 1, last);
    src_values.swap(count - 1, last);

    src_timestamps.truncate(count);
    src_values.truncate(count);

    last_timestamp
}

fn needs_dedup(timestamps: &[Timestamp], dedup_interval: i64) -> bool {
    if timestamps.len() < 2 || dedup_interval <= 0 {
        return false;
    }
    let mut ts_next = timestamps[0] + dedup_interval - 1;
    ts_next = ts_next - (ts_next % dedup_interval);
    for ts in &timestamps[1..] {
        let ts = *ts;
        if ts <= ts_next {
            return true;
        }
        ts_next += dedup_interval;
        if ts_next < ts {
            ts_next = ts + dedup_interval - 1;
            ts_next -= ts_next % dedup_interval
        }
    }
    false
}
