/// Returns the time duration since UNIX_EPOCH in milliseconds.
pub fn current_time_millis() -> i64 {
    // todo: specifically get from redis
    // see https://redis.io/docs/reference/modules/modules-api-ref/#section-module-information-and-time-measurement
    chrono::Utc::now().timestamp_millis()
}