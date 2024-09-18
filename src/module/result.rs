use crate::common::types::{Label, Timestamp};
use crate::storage::time_series::TimeSeries;
use metricsql_runtime::types::{MetricName, METRIC_NAME_LABEL};
use std::collections::HashMap;
use std::fmt::Display;
use metricsql_runtime::QueryResult;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{ValkeyString, ValkeyValue};

pub static META_KEY_LABEL: &str = "__meta:key__";

pub enum ResultType {
    Matrix,
    Vector,
    Scalar,
    String,
}

impl ResultType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResultType::Matrix => "matrix",
            ResultType::Vector => "vector",
            ResultType::Scalar => "scalar",
            ResultType::String => "string",
        }
    }
}
impl Display for ResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub(crate) fn metric_name_to_valkey_value(
    metric_name: &MetricName,
    key: Option<&str>,
) -> ValkeyValue {
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> =
        HashMap::with_capacity(metric_name.labels.len() + 1);
    if !metric_name.measurement.is_empty() {
        map.insert(
            ValkeyValueKey::from(METRIC_NAME_LABEL),
            metric_name.measurement.clone().into(),
        );
    }
    if let Some(key) = key {
        map.insert(ValkeyValueKey::from(META_KEY_LABEL), ValkeyValue::from(key));
    }
    for Label { name, value } in metric_name.labels.iter() {
        map.insert(ValkeyValueKey::from(name), value.into());
    }

    ValkeyValue::Map(map)
}

pub(super) fn sample_to_result(timestamp: Timestamp, value: f64) -> ValkeyValue {
    let epoch = ValkeyValue::Integer(timestamp);
    let value = ValkeyValue::SimpleString(value.to_string());
    vec![epoch, value].into()
}

pub(super) fn samples_to_result(timestamps: &[i64], values: &[f64]) -> ValkeyValue {
    timestamps
        .iter()
        .zip(values.iter())
        .map(|(ts, val)| sample_to_result(*ts, *val))
        .collect::<Vec<ValkeyValue>>()
        .into()
}

/// `To Prometheus Range Vector output
/// https://prometheus.io/docs/prometheus/latest/querying/api/#range-vectors
/// ``` json
/// {
///     "status" : "success",
///     "data" : {
///         "resultType" : "matrix",
///         "result" : [
///             {
///                 "metric" : {
///                     "__name__" : "up",
///                     "job" : "prometheus",
///                     "instance" : "localhost:9090"
///                 },
///                 "values" : [
///                     [ 1435781430.781, "1" ],
///                     [ 1435781445.781, "1" ],
///                     [ 1435781460.781, "1" ]
///                 ]
///             },
///             {
///                 "metric" : {
///                     "__name__" : "up",
///                     "job" : "node",
///                     "instance" : "localhost:9091"
///                 },
///                 "values" : [
///                     [ 1435781430.781, "0" ],
///                     [ 1435781445.781, "0" ],
///                     [ 1435781460.781, "1" ]
///                 ]
///             }
///         ]
///     }
/// }
/// ```
pub fn to_matrix_result(vals: Vec<QueryResult>) -> ValkeyValue {
    let map: Vec<ValkeyValue> = vals
        .into_iter()
        .map(|val| {
            let metric_name = metric_name_to_valkey_value(&val.metric, None);
            let samples = samples_to_result(&val.timestamps, &val.values);
            let map: HashMap<ValkeyValueKey, ValkeyValue> = vec![
                (ValkeyValueKey::from("metric"), metric_name),
                (ValkeyValueKey::from("values"), samples),
            ]
            .into_iter()
            .collect();
            ValkeyValue::Map(map)
        })
        .into_iter()
        .collect();

    to_success_result(map.into(), ResultType::Matrix)
}

/// Convert to Prometheus Instant Vector output format
/// https://prometheus.io/docs/prometheus/latest/querying/api/#instant-vectors
/// ``` json
/// {
///     "status" : "success",
///     "data" : {
///         "resultType" : "vector",
///         "result" : [
///             {
///                 "metric" : {
///                     "__name__" : "up",
///                     "job" : "prometheus",
///                     "instance" : "localhost:9090"
///                 },
///                 "value": [ 1435781451.781, "1" ]
///             },
///             {
///                 "metric" : {
///                     "__name__" : "up",
///                     "job" : "node",
///                     "instance" : "localhost:9100"
///                 },
///                 "value" : [ 1435781451.781, "0" ]
///             }
///         ]
///     }
/// }
/// ```
pub fn to_instant_vector_result(metric: &MetricName, ts: Timestamp, value: f64) -> ValkeyValue {
    let metric_name = metric_name_to_valkey_value(metric, None);
    let sample = sample_to_result(ts, value);
    let map: HashMap<ValkeyValueKey, ValkeyValue> = vec![
        (ValkeyValueKey::from("metric"), metric_name),
        (ValkeyValueKey::from("value"), sample),
    ]
    .into_iter()
    .collect();

    ValkeyValue::Map(map)
}

fn to_single_vector_result(metric: &MetricName, ts: Timestamp, value: f64) -> ValkeyValue {
    let metric_name = metric_name_to_valkey_value(metric, None);
    let sample = sample_to_result(ts, value);
    let map: HashMap<ValkeyValueKey, ValkeyValue> = vec![
        (ValkeyValueKey::from("metric"), metric_name),
        (ValkeyValueKey::from("value"), sample),
    ]
    .into_iter()
    .collect();

    ValkeyValue::Map(map)
}

pub fn to_success_result(data: ValkeyValue, response_type: ResultType) -> ValkeyValue {
    let data_map: HashMap<ValkeyValueKey, ValkeyValue> = vec![
        (
            ValkeyValueKey::from("resultType"),
            ValkeyValue::SimpleStringStatic(response_type.as_str()),
        ),
        (ValkeyValueKey::from("result"), data),
    ]
    .into_iter()
    .collect();

    let map: HashMap<ValkeyValueKey, ValkeyValue> = vec![
        status_element(true),
        (
            ValkeyValueKey::from("data"),
            ValkeyValue::Map(data_map),
        ),
    ]
    .into_iter()
    .collect();

    ValkeyValue::Map(map)
}

pub fn format_string_array_result(arr: &[String]) -> ValkeyValue {
    let converted = arr.iter().map(ValkeyValue::from).collect();
    format_array_result(converted)
}

pub fn format_array_result(arr: Vec<ValkeyValue>) -> ValkeyValue {
    let map: HashMap<ValkeyValueKey, ValkeyValue> = [
        status_element(true),
        (
            ValkeyValueKey::from("data"),
            ValkeyValue::Array(arr),
        ),
    ]
        .into_iter()
        .collect();

    ValkeyValue::Map(map)
}

fn status_element(success: bool) -> (ValkeyValueKey, ValkeyValue) {
    let status = if success { "success" } else { "error" };
    (
        ValkeyValueKey::from("status"),
        ValkeyValue::SimpleStringStatic(status),
    )
}

pub fn std_duration_to_redis_value(duration: &std::time::Duration) -> ValkeyValue {
    ValkeyValue::Integer(duration.as_secs() as i64 * 1000 + duration.subsec_millis() as i64)
}
pub fn string_hash_map_to_redis_value(map: &HashMap<String, String>) -> ValkeyValue {
    ValkeyValue::from(map.clone())
}
pub(super) fn get_ts_metric_selector(ts: &TimeSeries, key: Option<&ValkeyString>) -> ValkeyValue {
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(ts.labels.len() + 1);
    map.insert(
        ValkeyValueKey::String(METRIC_NAME_LABEL.into()),
        ValkeyValue::from(&ts.metric_name),
    );
    if let Some(key) = key {
        map.insert(ValkeyValueKey::String(META_KEY_LABEL.into()), ValkeyValue::from(key));
    }
    for Label { name, value } in ts.labels.iter() {
        map.insert(ValkeyValueKey::String(name.into()), ValkeyValue::from(value));
    }
    ValkeyValue::Map(map)
}
