use crate::rules::template::utils::{
    btree_map_to_template_value,
    get_hash_array_value,
    get_hash_float_value,
    get_hash_string_value
};
use crate::storage::Label;
use chrono::{DateTime, Utc};
use gtmpl_value::{FuncError, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;

/// metric is private copy of provider.Metric,
/// it is used for templating annotations,
/// Labels as map simplifies templates evaluation.
#[derive(Clone, Default)]
pub(crate) struct Metric {
    pub(crate) labels: Vec<Label>,
    pub(crate) timestamp: i64,
    pub(crate) value: f64,
}

impl Metric {
    fn new(labels: Vec<Label>, timestamp: i64, value: f64) -> Self {
        Metric {
            labels,
            timestamp,
            value
        }
    }

    fn get_label(&self, key: &str) -> &str {
        self.labels.iter().find(|x| x.name == key)
            .map_or("", |l| l.value.as_str())
    }
}

pub fn get_value_from_label(label: &Label) -> Value {
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String(label.name.to_string()));
    m.insert("value".to_string(), Value::String(label.value.to_string()));
    Value::Object(m)
}

impl From<&Metric> for Value {
    fn from(metric: &Metric) -> Value {
        let mut m = HashMap::new();
        let labels = metric.labels.iter().map(|l| l.into()).collect();
        m.insert("labels".to_string(), Value::Array(labels));
        m.insert("timestamp".to_string(), Value::from(metric.timestamp));
        m.insert("value".to_string(), Value::from(metric.value));
        Value::Object(m)
    }
}

impl From<Metric> for Value {
    fn from(metric: Metric) -> Value {
        metric.into()
    }
}

impl TryFrom<&Value> for Metric {
    type Error = FuncError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Map(_) | Value::Object(_) => {
                let label_values = get_hash_array_value(&value, "labels", true)?.unwrap();
                let mut labels: Vec<Label> = vec![];
                for label_value in label_values {
                    labels.push(label_value.try_into()?);
                }
                let timestamp = get_hash_float_value(&value, "timestamp", true)?.unwrap();
                let value = get_hash_float_value(&value, "value", true)?.unwrap();
                Ok(Metric::new(labels, timestamp as i64, value))
            }
            _ => Err(FuncError::Generic(format!("expected object for metric, got {}", value)))
        }
    }
}

impl TryFrom<Value> for Metric {
    type Error = FuncError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Metric::try_from(&value)
    }
}


pub type KV = BTreeMap<String, String>;

// Alert holds one alert for notification templates.
#[derive(Serialize, Deserialize, Clone)]
pub struct Alert {
    status: String,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
    starts_at: DateTime<Utc>,
    ends_at: DateTime<Utc>,
    generator_url: String,
    fingerprint: String,
}

impl From<&Alert> for Value {
    fn from(s: &Alert) -> Value {
        #[warn(unused_mut)]
        let mut m: HashMap<String, Value> = HashMap::new();
        m.insert("status".to_owned(), Value::from(&s.status));
        m.insert("labels".to_owned(), btree_map_to_template_value(&s.labels));
        m.insert("annotations".to_owned(), btree_map_to_template_value(&s.annotations));
        m.insert("starts_at".to_owned(), DateTimeModel(s.starts_at).into());
        m.insert("ends_at".to_owned(), DateTimeModel(s.starts_at).into());
        m.insert("generator_url".to_owned(), Value::from(&s.generator_url));
        m.insert("fingerprint".to_owned(), Value::from(&s.fingerprint));
        Value::Object(m)
    }
}

pub(super) struct DateTimeModel(DateTime<Utc>);
impl DateTimeModel {
    pub(super) fn new(at: DateTime<Utc>) -> Self {
        Self(at)
    }
}

impl Deref for DateTimeModel {
    type Target = DateTime<Utc>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&DateTimeModel> for Value {
    fn from(d: &DateTimeModel) -> Self {
        let mut result: HashMap<String, Value> = HashMap::new();
        result.insert("timestamp".to_owned(), Value::from(d.timestamp()));
        result.insert("timestamp_millis".to_owned(), Value::from(d.timestamp_millis()));
        result.insert("rfc3339".to_owned(), Value::from(d.to_rfc3339()));
        Value::Object(result)
    }
}

impl From<DateTimeModel> for Value {
    fn from(value: DateTimeModel) -> Self {
        value.into()
    }
}


// Data is the data passed to notification templates.
//
// End-users should not be exposed to Go's type system, as this will confuse them and prevent
// simple things like simple equality checks to fail. Map everything to float64/string.
#[derive(Serialize, Deserialize)]
pub struct Data {
    receiver: String,
    status: String,
    alerts: Vec<Alert>,
    group_labels: KV,
    common_labels: KV,
    common_annotations: KV,
}

pub fn label_to_template_value(label: &Label) -> Value {
    let mut m = HashMap::new();
    m.insert("name".to_string(), Value::String(label.name.to_string()));
    m.insert("value".to_string(), Value::String(label.value.to_string()));
    Value::Object(m)
}

pub fn template_value_to_label(value: &Value) -> Result<Label, FuncError> {
    match value {
        Value::Map(_) |
        Value::Object(_) => {
            let name = get_hash_string_value(value, "name", true)?.unwrap();
            let value = get_hash_string_value(value, "value", true)?.unwrap();
            Ok(Label::new(name.to_string(), value.to_string()))
        }
        _ => Err(FuncError::Generic(format!("expected object for label, got {}", value)))
    }
}

pub fn metric_to_template_value(metric: &Metric) -> Value {
    let mut m = HashMap::new();
    let labels = metric.labels.iter().map(|l| label_to_template_value).into();
    m.insert("labels".to_string(), Value::Array(labels));
    m.insert("timestamp".to_string(), Value::from(metric.timestamp));
    m.insert("value".to_string(), Value::from(metric.value));
    Value::Object(m)
}

pub fn template_value_to_metric(value: &Value) -> Result<Metric, FuncError> {
    match value {
        Value::Object(_map) => {
            let label_values = get_hash_array_value(value, "labels", true)?.unwrap();
            let labels = label_values.iter()
                .map(template_value_to_label)
                .collect::<Result<Vec<Label>, FuncError>>()?;
            let timestamp = get_hash_float_value(value, "timestamp", true)?.unwrap();
            let value = get_hash_float_value(value, "value", true)?.unwrap();
            Ok(Metric::new(labels, timestamp as i64, value))
        }
        _ => Err(FuncError::Generic(format!("expected object for metric, got {}", value)))
    }
}