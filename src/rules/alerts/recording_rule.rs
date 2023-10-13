use std::collections::HashMap;
use std::default::Default;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use ahash::{AHashMap, AHashSet};
use metricsql_engine::METRIC_NAME_LABEL;
use serde::{Deserialize, Serialize};
use crate::common::current_time_millis;

use crate::rules::{Rule, RuleState, RuleStateEntry, RuleType};
use crate::rules::alerts::{AlertsError, AlertsResult, Group, Metric, Querier, RuleConfig};
use crate::rules::recording::stringify_labels;
use crate::rules::types::{new_time_series, RawTimeSeries};
use crate::ts::Timestamp;

const ERR_DUPLICATE: &str =
    "result contains metrics with the same labelset after applying rule labels.";

/// RecordingRule is a Rule that evaluates a configured expression and returns a timeseries as result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordingRule {
    pub(crate) rule_type: RuleType,
    pub(crate) rule_id: u64,
    pub name: String,
    pub expr: String,
    pub labels: AHashMap<String, String>,
    pub group_id: u64,

    /// id of created time series id
    pub ts_key: String,

    #[serde(skip)]
    querier: Box<dyn Querier>,

    /// state stores recent state changes during evaluations
    pub state: RuleState,

    /// eval_alignment will make the timestamp of group query requests be aligned with interval
    pub eval_alignment: Option<bool>,

    metrics: RecordingRuleMetrics,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordingRuleMetrics {
    errors: AtomicU64,
    samples: AtomicU64,
}

type DatasourceMetric = Metric;

impl RecordingRule {
    pub fn new(group: &Group, cfg: &RuleConfig) -> Self {
        let mut rr = RecordingRule {
            rule_type: RuleType::Recording,
            rule_id: cfg.hash(),
            name: cfg.name().to_string(),
            expr: cfg.expr.clone(),
            labels: cfg.labels.clone(),
            group_id: group.ID(),
            state: RuleState::new(cfg.update_entries_limit()),
            eval_alignment: None,
            metrics: Default::default(),
            querier: Default::default(),
            ts_key: Default::default(),
        };
        return rr;
    }

    pub fn update(&mut self, rule: &RecordingRule) {
        self.expr = rule.expr.clone();
        self.labels = rule.labels.clone();
        // rr.q = nr.q
    }

    fn to_time_series(&self, m: &Metric) -> RawTimeSeries {
        let mut labels = HashMap::with_capacity(m.labels.len() + 1);
        for label in m.labels.iter() {
            labels.insert(label.name.clone(), label.value.clone())
        }
        labels.insert(METRIC_NAME_LABEL.to_string(), self.name.to_string());
        // override existing labels with configured ones
        for (k, v) in self.labels {
            labels.insert(k.clone(), v.clone())
        }
        return new_time_series(&m.values, &m.timestamps, labels);
    }

    fn run_query(&self, ts: Timestamp) -> AlertsResult<Vec<Metric>> {
        self.querier
            .query(&self.expr, ts)
            .map_err(|e| AlertsError::QueryExecutionError(format!("{}: {:?}", self.expr, e)))
    }
}

impl Rule for RecordingRule {
    fn id(&self) -> u64 {
        self.rule_id
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Recording
    }
    fn exec(&mut self, ts: Timestamp, limit: usize) -> AlertsResult<Vec<RawTimeSeries>> {
        let start = current_time_millis();

        let mut cur_state = RuleStateEntry::default();
        cur_state.time = start;
        cur_state.at = ts;

        let res = self.run_query(ts);
        cur_state.duration = Duration::from_millis((current_time_millis() - start) as u64);

        if let Err(err) = res {
            cur_state.err = err.clone();
            self.state.add(cur_state);
            return Err(err);
        }

        // Safety: unwrap is safe because we just checked for an error above
        let q_metrics = res.unwrap();
        cur_state.samples = q_metrics.len();

        let num_series = q_metrics.len();
        if limit > 0 && num_series > limit {
            let msg = format!("exec exceeded limit of {limit} with {num_series} series");
            let err = AlertsError::QueryExecutionError(msg);
            cur_state.err = err.clone();
            self.state.add(cur_state);
            return Err(err);
        }

        cur_state.series_fetched = num_series;

        let duplicates: AHashSet<String> = AHashSet::with_capacity(q_metrics.len());
        let mut tss: Vec<RawTimeSeries> = Vec::with_capacity(q_metrics.len());
        for (_, r) in q_metrics.iter().enumerate() {
            let ts = self.to_time_series(r);
            let key = stringify_labels(&ts);
            if duplicates.contains(&key) {
                let msg = format!(
                    "original metric {:?}; resulting labels {key}: {}",
                    r.labels, ERR_DUPLICATE
                );
                self.state.add(cur_state);
                return Err(AlertsError::DuplicateSeries(msg));
            }
            duplicates.push(key);
            tss.push(ts)
        }

        self.state.add(cur_state);
        Ok(tss)
    }

    /// exec_range executes recording_rule rule on the given time range similarly to exec.
    /// It doesn't update internal states of the Rule and is meant to be used just
    /// to get time series for backfilling.
    fn exec_range(&mut self, start: Timestamp, end: Timestamp) -> AlertsResult<Vec<RawTimeSeries>> {
        let mut res = self
            .querier
            .query_range(&self.expr, start, end)
            .map_err(|e| AlertsError::QueryExecutionError(format!("{}: {:?}", self.expr, e)))?;

        let mut duplicates: AHashSet<String> = AHashSet::with_capacity(res.len());
        let mut tss = Vec::with_capacity(res.len());
        for s in res.data.into_iter() {
            let ts = self.to_time_series(s);
            let key = stringify_labels(&ts);
            if duplicates.contains_key(&key) {
                let msg = format!(
                    "original metric {:?}; resulting labels {}: {}",
                    s.labels, key, ERR_DUPLICATE
                );
                return Err(AlertsError::DuplicateSeries(msg));
            }
            duplicates.push(key);
            tss.push(ts)
        }
        Ok(tss)
    }
}
