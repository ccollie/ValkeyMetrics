/// ALERT_METRIC_NAME is the metric name for synthetic alert timeseries.
pub static ALERT_METRIC_NAME: &str = "ALERTS";

/// ALERT_FOR_STATE_METRIC_NAME is the metric name for 'for' state of alert.
pub static ALERT_FOR_STATE_METRIC_NAME: &str = "ALERTS_FOR_STATE";

/// ALERT_NAME_LABEL is the label name indicating the name of an alert.
pub static ALERT_NAME_LABEL: &str = "alertname";
/// ALERT_STATE_LABEL is the label name indicating the state of an alert.
pub static ALERT_STATE_LABEL: &str = "alertstate";

/// ALERT_GROUP_NAME_LABEL defines the label name attached for generated time series.
/// attaching this label may be disabled via `-disableAlertgroupLabel` flag.
pub static ALERT_GROUP_NAME_LABEL: &str = "alertgroup";
