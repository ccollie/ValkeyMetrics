use crate::rules::alerts::{AlertsError, AlertsResult, Querier};
use crate::rules::types::{new_time_series, RawTimeSeries};
use crate::rules::{Rule, RuleStateEntry, RuleType};
use crate::storage::{Label, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use ahash::{AHashMap, AHashSet};
use metricsql_parser::label::Labels;
use crate::common::{current_time_millis, METRIC_NAME_LABEL};

const ERR_DUPLICATE: &str =
    "result contains metrics with the same labelset after applying rule labels.";

/// RecordingRule is a Rule that evaluates a configured expression and returns a timeseries as result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingRule {
    rule_type: RuleType,
    rule_id: u64,
    name: String,
    expr: String,
    labels: Labels,
    group_id: u64,

    // state stores recent state changes during evaluations
    state: Vec<RuleStateEntry>,
    metrics: RecordingRuleMetrics,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordingRuleMetrics {
    errors: AtomicU64,
    samples: AtomicU64,
}

impl Clone for RecordingRuleMetrics {
    fn clone(&self) -> Self {
        RecordingRuleMetrics {
            errors: AtomicU64::new(self.errors.load(std::sync::atomic::Ordering::Relaxed)),
            samples: AtomicU64::new(self.samples.load(std::sync::atomic::Ordering::Relaxed)),
        }
    }
}

type DatasourceMetric = crate::rules::alerts::Metric;

impl RecordingRule {
    fn to_time_series(&self, m: DatasourceMetric) -> RawTimeSeries {
        let mut labels = AHashMap::with_capacity(m.labels.len() + 1);
        for label in m.labels.iter() {
            labels.insert(label.name.clone(), label.value.clone());
        }
        labels.insert(METRIC_NAME_LABEL.to_string(), self.name.to_string());
        // override existing labels with configured ones
        for Label { name, value } in self.labels.iter() {
            labels.insert(name.clone(), value.clone());
        }
        return new_time_series(m.key, &m.values, &m.timestamps, labels);
    }

    fn run_query(&self, querier: &impl Querier, ts: Timestamp) -> AlertsResult<Vec<DatasourceMetric>> {
        querier
            .query(&self.expr, ts)
            .map_err(|e| AlertsError::QueryExecutionError(format!("{}: {:?}", self.expr, e)))
    }
}

impl Rule for RecordingRule {
    fn id(&self) -> u64 {
        todo!()
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Recording
    }

    fn exec(&mut self, querier: &impl Querier, ts: Timestamp, limit: usize) -> AlertsResult<Vec<RawTimeSeries>> {
        let start = current_time_millis();

        let mut cur_state = RuleStateEntry::default();
        cur_state.time = start;
        cur_state.at = ts;

        let res = self.run_query(querier, ts);
        cur_state.duration = Duration::from_millis((current_time_millis() - start) as u64);

        if let Err(err) = res {
            cur_state.err = Some(err.clone());
            self.state.push(cur_state);
            return Err(err);
        }

        // Safety: unwrap is safe because we just checked for an error above
        let q_metrics = res.unwrap();
        let num_series = res.len();
        cur_state.samples = num_series;

        if limit > 0 && num_series > limit {
            let msg = format!("exec exceeded limit of {limit} with {num_series} series");
            let err = AlertsError::QueryExecutionError(msg);
            cur_state.err = Option::from(err.clone());
            self.state.push(cur_state);
            return Err(err);
        }

        cur_state.series_fetched = num_series;

        let mut duplicates: AHashSet<String> = AHashSet::with_capacity(num_series);
        let mut tss: Vec<RawTimeSeries> = Vec::with_capacity(num_series);
        for (_, r) in q_metrics.iter().enumerate() {
            let ts = self.to_time_series(r);
            let key = stringify_labels(&ts);
            if duplicates.contains(&key) {
                let msg = format!(
                    "original metric {:?}; resulting labels {key}: {}",
                    r.labels, ERR_DUPLICATE
                );
                self.state.push(cur_state);
                return Err(AlertsError::DuplicateSeries(msg));
            }
            duplicates.insert(key);
            tss.push(ts)
        }

        self.state.push(cur_state);
        Ok(tss)
    }

    /// exec_range executes recording rule on the given time range similarly to Exec.
    /// It doesn't update internal states of the Rule and meant to be used just
    /// to get time series for backfilling.
    fn exec_range(&mut self, querier: &impl Querier, start: Timestamp, end: Timestamp) -> AlertsResult<Vec<RawTimeSeries>> {
        let mut res = querier
            .query_range(&self.expr, start, end)
            .map_err(|e| AlertsError::QueryExecutionError(format!("{}: {:?}", self.expr, e)))?;

        let mut duplicates: AHashSet<String> = AHashSet::with_capacity(res.len());
        let mut tss = Vec::with_capacity(res.len());
        for s in res.data.into_iter() {
            let ts = self.to_time_series(s);
            let key = stringify_labels(&ts);
            if duplicates.contains(&key) {
                let msg = format!(
                    "original metric {:?}; resulting labels {}: {}",
                    s.labels, key, ERR_DUPLICATE
                );
                return Err(AlertsError::DuplicateSeries(msg));
            }
            duplicates.insert(key);
            tss.push(ts)
        }
        Ok(tss)
    }

}

pub fn stringify_labels(ts: &RawTimeSeries) -> String {
    let mut labels = ts.labels.clone();
    let mut b = String::with_capacity(40); // todo: better capacity calculation.
    let mut i = 0;
    labels.sort();
    for label in ts.labels {
        b.push_str(&*format!("{}={}", &label.name, &label.value));
        if i < labels.len() - 1 {
            b.push_str(",")
        }
    }
    b
}
