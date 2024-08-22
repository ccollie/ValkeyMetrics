use std::collections::VecDeque;
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::time::Duration;
use redis_module::{Context as RedisContext};
use serde::{Deserialize, Serialize};
use crate::common::types::{Timestamp};
use crate::rules::alerts::{AlertingRule, AlertsError, AlertsResult, Querier, RecordingRule};
use crate::rules::types::RawTimeSeries;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    pub fn is_recording(&self) -> bool {
        matches!(self, RuleType::Recording)
    }

    pub fn is_alerting(&self) -> bool {
        matches!(self, RuleType::Alerting)
    }
}

impl Default for RuleType {
    fn default() -> Self {
        RuleType::Alerting
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
            _ => Err(format!("unknown rule type: {}", s)),
        }
    }
}

pub struct EvalContext<'a> {
    pub querier: Box<dyn Querier>,
    pub redis_ctx: &'a RedisContext
}

impl<'a> EvalContext<'a> {
    pub fn new(querier: Box<dyn Querier>, redis_ctx: &'a RedisContext) -> Self {
        EvalContext {
            querier,
            redis_ctx
        }
    }

    pub fn log_debug(&self, msg: &str) {
        self.redis_ctx.log_debug(msg);
    }

    pub fn log_info(&self, msg: &str) {
        self.redis_ctx.log_info(msg);
    }

    pub fn log_warning(&self, msg: &str) {
        self.redis_ctx.log_warning(msg);
    }
}

/// Rule represents alerting or recording_rule rule that has unique id, can be executed
/// and updated with other Rule.
pub trait Rule: Debug {
    /// id returns unique id that may be used for identifying this Rule among others.
    fn id(&self) -> u64;
    fn rule_type(&self) -> RuleType;
    /// exec executes the rule with given context at the given timestamp and limit.
    /// returns an err if number of resulting time series exceeds the limit.
    fn exec(&mut self, querier: &impl Querier, ts: Timestamp, limit: usize) -> AlertsResult<Vec<RawTimeSeries>>;
    /// exec_range executes the rule on the given time range.
    fn exec_range(&mut self, querier: &impl Querier, start: Timestamp, end: Timestamp) -> AlertsResult<Vec<RawTimeSeries>>;
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RuleStateEntry {
    /// stores last moment of time rule.exec() was called
    pub time: Timestamp,
    /// stores the timestamp with which rule.exec() was called
    pub at: Timestamp,
    /// stores the duration of the last rule.exec() call
    pub duration: Duration,
    /// stores last error that happened in exec func resets on every successful exec
    /// may be used as Health ruleState
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<AlertsError>,    // todo: error type
    /// stores the number of samples returned during the last evaluation
    pub samples: usize,
    /// stores the number of time series fetched during the last evaluation.
    pub series_fetched: usize
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PromRule {
    AlertRule(AlertingRule),
    RecordRule(RecordingRule),
}

impl Rule for PromRule {
    fn id(&self) -> u64 {
        match self {
            PromRule::AlertRule(rule) => rule.id(),
            PromRule::RecordRule(rule) => rule.id(),
        }
    }

    fn rule_type(&self) -> RuleType {
        match self {
            PromRule::AlertRule(rule) => rule.rule_type(),
            PromRule::RecordRule(rule) => rule.rule_type(),
        }
    }

    fn exec(&mut self, querier: &impl Querier, ts: Timestamp, limit: usize) -> AlertsResult<Vec<RawTimeSeries>> {
        match self {
            PromRule::AlertRule(rule) => rule.exec(querier, ts, limit),
            PromRule::RecordRule(rule) => rule.exec(querier, ts, limit),
        }
    }

    fn exec_range(&mut self, querier: &impl Querier, start: Timestamp, end: Timestamp) -> AlertsResult<Vec<RawTimeSeries>> {
        match self {
            PromRule::AlertRule(rule) => rule.exec_range(querier, start, end),
            PromRule::RecordRule(rule) => rule.exec_range(querier, start, end),
        }
    }
}
// var errDuplicate = "result contains metrics with the same labelset after applying rule labels. See https://docs.victoriametrics.com/vmalert.html#series-with-the-same-labelset for details";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RuleState(pub VecDeque<RuleStateEntry>);

impl RuleState {
    pub fn new(size: usize) -> RuleState {
        let queue = VecDeque::with_capacity(size);
        RuleState(queue)
    }

    pub fn get_last(&self) -> Option<&RuleStateEntry> {
        self.0.iter().last()
    }

    pub fn size(&self) -> usize {
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
        if self.0.len() == self.0.capacity() {
            let _ = self.0.pop_front();
        }
        self.0.push_back(e);
    }

    pub fn reset(&mut self) {
        self.0.clear();
    }

    pub fn iter(&self) -> std::collections::vec_deque::Iter<RuleStateEntry> {
        self.0.iter()
    }

}
