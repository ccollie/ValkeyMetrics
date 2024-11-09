use crate::alerts::rules::{AlertingRule, RecordingRule};
use crate::alerts::types::RawTimeSeries;
use crate::alerts::{AlertDatasource, AlertsError, AlertsResult};
use crate::common::types::Timestamp;
use get_size::GetSize;
use metricsql_common::hash::FastHasher;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::time::Duration;
use crate::config::GLOBAL_SETTINGS;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum RuleType {
    Recording,
    Alerting,
}

impl RuleType {
    pub fn name(&self) -> &'static str {
        match self {
            RuleType::Recording => "recording",
            RuleType::Alerting => "alerting",
        }
    }
}


impl Display for RuleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for RuleType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            value if value.eq_ignore_ascii_case("recording_rule") => Ok(RuleType::Recording),
            value if value.eq_ignore_ascii_case(RuleType::Recording.name()) => Ok(RuleType::Recording),
            value if value.eq_ignore_ascii_case(RuleType::Alerting.name()) => Ok(RuleType::Alerting),
            _ => Err(format!("unknown rules type: {}", s)),
        }
    }
}


/// Rule represents alerting or recording rules that has unique id, can be executed
/// and updated with other Rule.
pub trait Rule: Debug + Any {
    /// id returns unique id that may be used for identifying this Rule among others.
    fn id(&self) -> u64;
    
    fn name(&self) -> &str;
    
    fn rule_type(&self) -> RuleType;
    
    fn expr(&self) -> &str;
    
    /// exec executes the rules with given context at the given timestamp and limit.
    /// returns an err if number of resulting time series exceeds the limit.
    fn exec(&mut self, querier: &AlertDatasource, ts: Timestamp, limit: usize) -> AlertsResult<Vec<RawTimeSeries>>;
    /// exec_range executes the rules on the given time range.
    fn exec_range(&mut self, querier: &AlertDatasource, start: Timestamp, end: Timestamp) -> AlertsResult<Vec<RawTimeSeries>>;
    
    fn update_with(&mut self, other: &dyn Rule) -> AlertsResult<()>;
    
    fn get_last_entry(&self) -> Option<&RuleStateEntry>;
    
    fn get_rule_state_count(&self) -> usize;
    fn get_all_entries(&self) -> Vec<RuleStateEntry>;
    
    fn as_any(&self) -> &dyn Any;
    
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[derive(GetSize)]
pub struct RuleStateEntry {
    /// stores last moment of time rules.exec() was called
    pub time: Timestamp,
    /// stores the timestamp with which rules.exec() was called
    pub at: Timestamp,
    /// stores the duration of the last rules.exec() call
    pub duration: Duration,
    /// stores last error that happened in exec func resets on every successful exec
    /// may be used as Health ruleState
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<AlertsError>,    // todo: error type
    /// stores the number of samples returned during the last evaluation
    pub samples: usize,
    /// stores the number of time series fetched during the last evaluation.
    pub series_fetched: Option<usize>
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(GetSize)]
pub enum MetricRule {
    AlertingRule(AlertingRule), // possibly box this to conserve space
    RecordingRule(RecordingRule),
}

impl Default for MetricRule {
    fn default() -> Self {
        MetricRule::RecordingRule(RecordingRule::default())
    }
}

impl Rule for MetricRule {
    fn id(&self) -> u64 {
        match self {
            MetricRule::AlertingRule(rule) => rule.id(),
            MetricRule::RecordingRule(rule) => rule.id(),
        }
    }

    fn name(&self) -> &str {
        match self {
            MetricRule::AlertingRule(rule) => rule.name(),
            MetricRule::RecordingRule(rule) => rule.name(),
        }
    }
    
    fn rule_type(&self) -> RuleType {
        match self {
            MetricRule::AlertingRule(rule) => rule.rule_type(),
            MetricRule::RecordingRule(rule) => rule.rule_type(),
        }
    }

    fn expr(&self) -> &str {
        match self {
            MetricRule::AlertingRule(rule) => rule.expr(),
            MetricRule::RecordingRule(rule) => rule.expr(),
        }
    }

    fn exec(&mut self, querier: &AlertDatasource, ts: Timestamp, limit: usize) -> AlertsResult<Vec<RawTimeSeries>> {
        match self {
            MetricRule::AlertingRule(rule) => rule.exec(querier, ts, limit),
            MetricRule::RecordingRule(rule) => rule.exec(querier, ts, limit),
        }
    }

    fn exec_range(&mut self, querier: &AlertDatasource, start: Timestamp, end: Timestamp) -> AlertsResult<Vec<RawTimeSeries>> {
        match self {
            MetricRule::AlertingRule(rule) => rule.exec_range(querier, start, end),
            MetricRule::RecordingRule(rule) => rule.exec_range(querier, start, end),
        }
    }

    fn update_with(&mut self, other: &dyn Rule) -> AlertsResult<()> {
        match self {
            MetricRule::AlertingRule(ref mut a) => a.update_with(other),
            MetricRule::RecordingRule(ref mut a) => a.update_with(other),
        }
    }

    fn get_last_entry(&self) -> Option<&RuleStateEntry> {
        match self {
            MetricRule::AlertingRule(rule) => rule.get_last_entry(),
            MetricRule::RecordingRule(rule) => rule.get_last_entry(),
        }
    }

    fn get_rule_state_count(&self) -> usize {
        match self {
            MetricRule::AlertingRule(rule) => rule.get_rule_state_count(),
            MetricRule::RecordingRule(rule) => rule.get_rule_state_count(),
        }
    }

    fn get_all_entries(&self) -> Vec<RuleStateEntry> {
        match self {
            MetricRule::AlertingRule(rule) => rule.get_all_entries(),
            MetricRule::RecordingRule(rule) => rule.get_all_entries(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
// var errDuplicate = "result contains metrics with the same labelset after applying rules labels. See https://docs.victoriametrics.com/vmalert.html#series-with-the-same-labelset for details";

impl Display for MetricRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt_rule(self, f)
    }
}

pub(super) fn fmt_rule(rule: &dyn Rule, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let rule_type = rule.rule_type().name();

    write!(f, "{} rule {}; expr: {}", rule_type, rule.name(), rule.expr())?;
    let labels = match rule.rule_type() {
        RuleType::Alerting => {
            let alert_rule = rule.as_any().downcast_ref::<AlertingRule>().unwrap();
            &alert_rule.labels
        },
        RuleType::Recording => {
            let recording_rule = rule.as_any().downcast_ref::<RecordingRule>().unwrap();
            &recording_rule.labels
        },
    };
    let mut keys = labels.keys().collect::<Vec<_>>();
    keys.sort();
    
    if !keys.is_empty() {
        write!(f, "; labels:")?;
    }

    for (i, key) in keys.iter().enumerate() {
        if let Some(value) = labels.get(*key) {
            write!(f, " ")?;
            write!(f, "{}={}", key, value)?;
            if i < keys.len() - 1 {
                write!(f, ",")?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[derive(GetSize)]
pub struct RuleState(pub VecDeque<RuleStateEntry>);

impl Default for RuleState {
    fn default() -> Self {
        let limit = GLOBAL_SETTINGS.rule_update_entries_limit;
        RuleState(VecDeque::with_capacity(limit))
    }
}

impl RuleState {
    pub fn with_capacity(size: usize) -> Self {
        let queue = VecDeque::with_capacity(size);
        RuleState(queue)
    }

    pub fn push(&mut self, entry: RuleStateEntry) {
        // drop oldest entry if capacity is reached
        if self.0.len() == self.0.capacity() {
            self.0.pop_front();
        }
        self.0.push_back(entry);
    }
    
    pub fn get_last(&self) -> Option<&RuleStateEntry> {
        self.0.iter().last()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get_all(&self) -> Vec<RuleStateEntry> {
        let ts_default = Timestamp::default();
        self.0
            .iter()
            .rev()
            .filter(|e| e.time != ts_default || e.at != ts_default)
            .cloned()
            .collect::<Vec<_>>()
    }

    pub fn add(&mut self, e: RuleStateEntry) {
        self.push(e);
    }

    pub fn reset(&mut self) {
        self.0.clear();
    }

    pub fn iter(&self) -> std::collections::vec_deque::Iter<RuleStateEntry> {
        self.0.iter()
    }
}

pub fn calc_rule_hash(rule: &dyn Rule) -> AlertsResult<u64> {
    let mut h = FastHasher::default();

    fn hash_labels(h: &mut FastHasher, labels: &HashMap<String, String>) -> Result<(), String> {
        if!labels.is_empty() {
            let mut keys: Vec<_> = labels.keys().collect();
            keys.sort();
            h.write("labels".as_bytes());
            for k in keys {
                h.write(k.as_bytes());
                h.write(labels.get(k).unwrap().as_bytes());
                h.write("\0xff".as_ref());
            }
        }
        Ok(())
    }

    rule.expr().hash(&mut h);

    let rule_type_name = rule.rule_type().name();
    std::hash::Hasher::write(&mut h, rule_type_name.as_bytes());
    h.write(rule.name().as_bytes());

    match rule.rule_type() {
        RuleType::Alerting => {
            let rule = &rule.as_any().downcast_ref::<AlertingRule>().unwrap();
            hash_labels(&mut h, &rule.labels)
        },
        RuleType::Recording => {
            let rule = &rule.as_any().downcast_ref::<RecordingRule>().unwrap();
            hash_labels(&mut h, &rule.labels)
        },
    }.map_err(|_| AlertsError::Generic("ERR hashing rule".to_string()))?;

    Ok(h.finish())
}