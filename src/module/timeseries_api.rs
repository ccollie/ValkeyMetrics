use valkey_module::{ValkeyError, ValkeyResult};
use crate::common::types::Timestamp;
use crate::storage::time_series::TimeSeries;

pub fn validate_sample_timestamp_for_insert(series: &TimeSeries, ts: Timestamp) -> ValkeyResult<()> {
    let last_ts = series.last_timestamp;
    // ensure inside retention period.
    if series.is_older_than_retention(ts) {
        return Err(ValkeyError::Str("TSDB: Timestamp is older than retention"));
    }

    if let Some(dedup_interval) = series.dedupe_interval {
        if !series.is_empty() {
            let millis = dedup_interval.as_millis() as i64;
            if millis > 0 && (ts - last_ts) < millis {
                return Err(ValkeyError::Str(
                    "TSDB: new sample in less than dedupe interval",
                ));
            }
        }
    }
    Ok(())
}