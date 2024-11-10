/// `ALERT_METRIC_NAME` is the metric name for synthetic alert timeseries.
pub static ALERT_METRIC_NAME: &str = "ALERTS";

/// `ALERT_FOR_STATE_METRIC_NAME` is the metric name for 'for' state of alert.
pub static ALERT_FOR_STATE_METRIC_NAME: &str = "ALERTS_FOR_STATE";

/// `ALERT_NAME_LABEL` is the label name indicating the name of an alert.
pub static ALERT_NAME_LABEL: &str = "alertname";
/// ALERT_STATE_LABEL is the label name indicating the state of an alert.
pub static ALERT_STATE_LABEL: &str = "alertstate";

/// `ALERT_GROUP_NAME_LABEL` defines the label name attached for generated time series.
/// attaching this label may be disabled via `-disableAlertgroupLabel` flag.
pub static ALERT_GROUP_NAME_LABEL: &str = "alertgroup";

/// STALE_NAN_BITS is bit representation of Prometheus staleness mark (aka stale NaN).
/// This mark is put by Prometheus at the end of time series for improving staleness detection.
/// See https://www.robustperception.io/staleness-and-promql
/// StaleNaN is a special NaN value, which is used as Prometheus staleness mark.
pub const STALE_NAN_BITS: u64 = 0x7ff0000000000002;

// todo: have global configurable key prefix
pub const KEY_PREFIX: &'static str = "__VM__";

// todo: better name
// todo: have global configurable key prefix
pub const STREAM_NOTIFIER_KEY_PREFIX: &'static str = "x-alert-stream"; 