use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::Hasher;
use std::str::FromStr;
use std::time::Duration;

use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3;

use crate::config::DEFAULT_RULE_UPDATE_ENTRIES_LIMIT;
use crate::rules::alerts::{AlertsError, AlertsResult};
use crate::rules::RuleType;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DataSourceType {
    #[default]
    Redis,
    Prometheus,
    Graphite,
    OpenTSDB,
    InfluxDB,
}

impl Display for DataSourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataSourceType::Redis => write!(f, "redis"),
            DataSourceType::Prometheus => write!(f, "prometheus"),
            DataSourceType::Graphite => write!(f, "graphite"),
            DataSourceType::OpenTSDB => write!(f, "opentsdb"),
            DataSourceType::InfluxDB => write!(f, "influxdb"),
        }
    }
}

impl FromStr for DataSourceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "redis" => Ok(DataSourceType::Redis),
            "prometheus" => Ok(DataSourceType::Prometheus),
            "graphite" => Ok(DataSourceType::Graphite),
            "opentsdb" => Ok(DataSourceType::OpenTSDB),
            "influxdb" => Ok(DataSourceType::InfluxDB),
            _ => Err(format!("Unknown data source type: {}", s))
        }
    }
}

/// ValidateTplFn must validate the given annotations
pub type ValidateTplFn = fn(annotations: &AHashMap<String, String>) -> AlertsResult<()>;

/// Rule describes entity that represent either recording rule or alerting rule.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RuleConfig {
    #[serde(skip)]
    pub id: u64,
    pub record: String,
    pub alert: String,
    pub expr: String,
    pub r#for: Duration,
    /// Alert will continue firing for this long even when the alerting expression no longer has results.
    pub keep_firing_for: Duration,
    pub labels: AHashMap<String, String>,
    pub annotations: AHashMap<String, String>,
    pub debug: bool,
    /// update_entries_limit defines max number of rule's state updates stored in memory.
    /// Overrides `-rule.updateEntriesLimit`.
    pub update_entries_limit: Option<usize>,
}

impl RuleConfig {
    /// Hash returns unique hash of the RuleConfig
    pub fn hash(&self) -> u64 {
        hash_rule_config(&self)
    }

    /// returns Rule name according to its type
    pub fn name(&self) -> &str {
        if !self.record.is_empty() {
            &self.record
        } else {
            &self.alert
        }
    }

    pub fn rule_type(&self) -> RuleType {
        if !self.record.is_empty() {
            RuleType::Recording
        } else {
            RuleType::Alerting
        }
    }

    pub fn update_entries_limit(&self) -> usize {
        // todo; this is a placeholder. use global config
        self.update_entries_limit.unwrap_or(DEFAULT_RULE_UPDATE_ENTRIES_LIMIT)
    }

    pub fn validate(&self) -> AlertsResult<()> {
        let name = self.name();

        let err = |msg: &str| -> AlertsResult<()> {
            return Err(AlertsError::InvalidRule(msg.to_string()));
        };

        if self.record.is_empty() && self.alert.is_empty() {
            let msg = format!("rule \"{name}\" must have either record or alert field set");
            return err(&msg);
        }
        if !self.record.is_empty() && !self.alert.is_empty() {
            let msg = format!("rule \"{name}\" should have either record or alert field set, not both");
            return err(&msg);
        }
        if self.expr.is_empty() {
            let msg = format!("rule \"{name}\" must have expression set");
            return err(&msg);
        }
        if self.r#for.as_millis() < 0 {
            let msg = format!("rule \"{name}\" for duration should not be negative");
            return err(&msg);
        }
        if self.keep_firing_for.as_millis() < 0 {
            return Err(AlertsError::InvalidRule("rule keep_firing_for duration shouldn't be negative".to_string()));
        }
        Ok(())
    }
}

impl Display for RuleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut rule_type = "recording";
        if !self.alert.is_empty() {
            rule_type = "alerting"
        }
        write!(f, "{} rule {}; expr: {}", rule_type, self.name(), self.expr)?;
        let mut keys = self.labels.keys().collect::<Vec<_>>();
        keys.sort();

        for (i, key) in keys.iter().enumerate() {
            let value = self.labels.get(key).unwrap();
            if i == 0 {
                write!(f, "; labels:")?;
            }
            write!(f, " ")?;
            write!(f, "{}={}", key, value)?;
            if i < keys.len() - 1 {
                write!(f, ",")?;
            }
        }
        Ok(())
    }
}

/// Group contains list of Rules grouped into entity with one name and evaluation interval
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GroupConfig {
    #[serde(rename = "type")]
    pub datasource_type: DataSourceType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub interval: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub eval_offset: Option<Duration>,
    pub limit: usize,
    pub rules: Vec<RuleConfig>,
    pub concurrency: usize,
    /// Labels is a set of label value pairs, that will be added to every rule.
    /// It has priority over the external labels.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub labels: AHashMap<String, String>,
    /// Checksum stores the hash of yaml definition for this group.
    /// May be used to detect any changes like rules re-ordering etc.
    pub checksum: String,
    /// Optional HTTP URL parameters added to each rule request
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub params: Option<AHashMap<String, String>>,
    /// Headers contains optional HTTP headers added to each rule request
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) headers: Headers,
    /// optional HTTP headers sent to notifiers for generated notifications
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub notifier_headers: Vec<Header>,
    /// eval_alignment will make the timestamp of group query requests be aligned with interval
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eval_alignment: Option<bool>,
}

/// Header is a Key - Value struct for holding an HTTP header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Header {
    pub(crate) key: String,
    pub(crate) value: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Headers(pub Vec<Header>);

impl Headers {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Header> {
        self.0.iter()
    }
}

impl Into<HashMap<String, String>> for Headers {
    fn into(self) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        for header in self.0 {
            headers.insert(header.key, header.value);
        }
        headers
    }
}

impl Default for Headers {
    fn default() -> Self {
        Headers(Vec::new())
    }
}

impl GroupConfig {
    pub fn validate(&self, validate_tpl_fn: ValidateTplFn, validate_expressions: bool) -> AlertsResult<()> {
        fn err(msg: &str) -> AlertsResult<()> {
            Err(AlertsError::InvalidConfiguration(msg.to_string()))
        }

        if self.name.is_empty() {
            return err("group name must be set");
        }
        if let Some(interval) = &self.interval {
            if interval.as_millis() < 0 {
                return err("interval shouldn't be lower than 0");
            }
        }
        if let Some(offset) = &self.eval_offset {
            if offset.as_millis() < 0 {
                return err("eval_offset shouldn't be lower than 0");
            }
            if let Some(interval) = &self.interval {
                // if `eval_offset` is set, interval won't use global evaluationInterval flag and
                // must be bigger than offset.
                if offset > interval {
                    let msg = format!("eval_offset should be smaller than interval; now eval_offset: {}, interval: {}",
                                      offset.as_millis(), interval.as_millis());
                    return err(&msg);
                }
            }
        }
        if self.concurrency < 0 {
            return Err(AlertsError::InvalidConfiguration(
                format!("invalid concurrency {}, shouldn't be less than 0", self.concurrency)));
        }
        let mut unique_rules = HashSet::with_capacity(self.rules.len());

        for r in self.rules {
            let rule_name = r.name();
            let id = r.id();
            if unique_rules.contains(&id) {
                return Err(AlertsError::InvalidConfiguration(format!("{} is a duplicate in group", r)));
            }
            unique_rules.insert(id);
            r.validate()?;

            if validate_expressions {
                validate_expr(&r.expr)
                    .map_err(|err| {
                        let msg = format!("invalid expression for rule {}: {:?}", rule_name, err);
                        Err(AlertsError::InvalidRule(msg))
                    })?;
            }

            validate_tpl_fn(&r.annotations)
                .map_err(|err| {
                    let msg = format!("invalid annotations for rule {}: {:?}", rule_name, err);
                    Err(AlertsError::InvalidRule(msg))
                })?;

            validate_tpl_fn(&r.labels)
                .map_err(|err| {
                    let msg = format!("invalid labels for rule {}: {:?}", rule_name, err);
                    Err(AlertsError::InvalidRule(msg))
                })?;
        }
        Ok(())
    }

    pub fn from_yaml(yaml: &str) -> AlertsResult<Self> {
        serde_yaml::from_str(yaml)
            .map_err(|err| AlertsError::InvalidConfiguration(format!("invalid yaml: {:?}", err)))
    }

    pub fn to_yaml(&self) -> AlertsResult<String> {
        serde_yaml::to_string(self)
            .map_err(|err| AlertsError::CannotSerialize(format!("yaml serialization error : {:?}", err)))
    }
}

fn validate_expr(expr: &str) -> AlertsResult<()> {
    match metricsql_parser::parser::parse(expr) {
        Ok(_) => Ok(()),
        Err(err) => Err(AlertsError::InvalidConfiguration(format!("invalid expression: {:?}", err)))
    }
}

/// HashRule hashes significant Rule fields into unique hash that supposed to define Rule uniqueness
fn hash_rule_config(r: &RuleConfig) -> u64 {
    let mut h = Xxh3::new();
    h.write(r.expr.as_bytes());
    if !r.record.is_empty() {
        h.write("recording".as_bytes());
        h.write(r.record.as_bytes());
    } else {
        h.write("alerting".as_bytes());
        h.write(r.alert.as_bytes());
    }
    let mut keys = r.labels.keys().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        let v = r.labels.get(key).unwrap();
        h.write(key.as_bytes());
        h.write(v.as_bytes());
        h.Write("\0xff");
    }
    h.digest()
}

