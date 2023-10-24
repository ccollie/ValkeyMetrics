use crate::common::types::Timestamp;
use crate::ts::time_series::TimeSeries;
use metricsql_engine::{MetricName, QueryResult, Tag, METRIC_NAME_LABEL};
use redis_module::redisvalue::RedisValueKey;
use redis_module::RedisValue;
use std::collections::HashMap;
use std::fmt::Display;

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

pub(crate) fn metric_name_to_redis_value(
    metric_name: &MetricName,
    key: Option<&str>,
) -> RedisValue {
    let mut map: HashMap<RedisValueKey, RedisValue> =
        HashMap::with_capacity(metric_name.tags.len() + 1);
    if !metric_name.metric_group.is_empty() {
        map.insert(
            RedisValueKey::String(METRIC_NAME_LABEL.to_string()),
            metric_name.metric_group.clone().into(),
        );
    }
    if let Some(key) = key {
        map.insert(RedisValueKey::from(META_KEY_LABEL), RedisValue::from(key));
    }
    for Tag { key, value } in metric_name.tags.iter() {
        map.insert(RedisValueKey::String(key.into()), value.into());
    }

    RedisValue::Map(map)
}

pub(super) fn sample_to_result(timestamp: Timestamp, value: f64) -> RedisValue {
    let epoch = RedisValue::Integer(timestamp);
    let value = RedisValue::SimpleString(value.to_string());
    vec![epoch, value].into()
}

pub(super) fn samples_to_result(timestamps: &[i64], values: &[f64]) -> RedisValue {
    timestamps
        .iter()
        .zip(values.iter())
        .map(|(ts, val)| sample_to_result(*ts, *val))
        .collect::<Vec<RedisValue>>()
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
pub fn to_matrix_result(vals: Vec<QueryResult>) -> RedisValue {
    let map: Vec<RedisValue> = vals
        .into_iter()
        .map(|val| {
            let metric_name = metric_name_to_redis_value(&val.metric, None);
            let samples = samples_to_result(&val.timestamps, &val.values);
            let map: HashMap<RedisValueKey, RedisValue> = vec![
                (RedisValueKey::String("metric".to_string()), metric_name),
                (RedisValueKey::String("values".to_string()), samples),
            ]
            .into_iter()
            .collect();
            RedisValue::Map(map)
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
pub fn to_instant_vector_result(metric: &MetricName, ts: Timestamp, value: f64) -> RedisValue {
    let metric_name = metric_name_to_redis_value(metric, None);
    let sample = sample_to_result(ts, value);
    let map: HashMap<RedisValueKey, RedisValue> = vec![
        (RedisValueKey::String("metric".to_string()), metric_name),
        (RedisValueKey::String("value".to_string()), sample),
    ]
    .into_iter()
    .collect();

    RedisValue::Map(map)
}

fn to_single_vector_result(metric: &MetricName, ts: Timestamp, value: f64) -> RedisValue {
    let metric_name = metric_name_to_redis_value(metric, None);
    let sample = sample_to_result(ts, value);
    let map: HashMap<RedisValueKey, RedisValue> = vec![
        (RedisValueKey::String("metric".to_string()), metric_name),
        (RedisValueKey::String("value".to_string()), sample),
    ]
    .into_iter()
    .collect();

    RedisValue::Map(map)
}

pub fn to_success_result(data: RedisValue, response_type: ResultType) -> RedisValue {
    let data_map: HashMap<RedisValueKey, RedisValue> = vec![
        (
            RedisValueKey::String("resultType".to_string()),
            RedisValue::SimpleStringStatic(response_type.as_str()),
        ),
        (RedisValueKey::String("result".to_string()), data),
    ]
    .into_iter()
    .collect();

    let map: HashMap<RedisValueKey, RedisValue> = vec![
        (
            RedisValueKey::String("status".to_string()),
            RedisValue::SimpleStringStatic("success"),
        ),
        (
            RedisValueKey::String("data".to_string()),
            RedisValue::Map(data_map),
        ),
    ]
    .into_iter()
    .collect();

    RedisValue::Map(map)
}

pub fn std_duration_to_redis_value(duration: &std::time::Duration) -> RedisValue {
    return RedisValue::Integer(duration.as_secs() as i64 * 1000 + duration.subsec_millis() as i64);
}
pub fn string_hash_map_to_redis_value(map: &HashMap<String, String>) -> RedisValue {
    RedisValue::from(map.clone())
}
pub(super) fn get_ts_metric_selector(ts: &TimeSeries) -> RedisValue {
    let mut map: HashMap<RedisValueKey, RedisValue> = HashMap::with_capacity(ts.labels.len() + 1);
    map.insert(
        RedisValueKey::String(METRIC_NAME_LABEL.into()),
        RedisValue::from(&ts.metric_name),
    );
    for (k, v) in ts.labels.iter() {
        map.insert(RedisValueKey::String(k.into()), RedisValue::from(v));
    }
    RedisValue::Map(map)
}
