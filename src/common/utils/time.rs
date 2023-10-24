use chrono::Duration;

/// Returns the time duration since UNIX_EPOCH in milliseconds.
pub fn current_time_millis() -> i64 {
    // todo: specifically get from redis
    chrono::Utc::now().timestamp_millis()
}
pub fn duration_to_chrono(duration: std::time::Duration) -> Duration {
    Duration::milliseconds(duration.as_millis() as i64)
}
