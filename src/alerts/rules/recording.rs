use std::any::Any;
use std::collections::HashMap;
use crate::alerts::rules::config::RuleConfig;
use crate::alerts::rules::{Group, Rule, RuleStateEntry, RuleType};
use crate::alerts::types::RawTimeSeries;
use crate::alerts::{AlertDatasource, AlertsError, AlertsResult, Querier};
use crate::common::types::{Label, MetricName, Sample, Timestamp};
use crate::common::{current_time_millis, METRIC_NAME_LABEL};
use crate::config::DEFAULT_RULE_UPDATE_ENTRIES_LIMIT;
use crate::query::{InstantQueryResult, RangeQueryResult};
use enquote::enquote;
use get_size::GetSize;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use ahash::AHashSet;

const ERR_DUPLICATE: &str =
    "result contains metrics with the same labelset after applying rules labels.";

/// `RecordingRule` is a Rule that evaluates a configured vector expression and records 
/// the result into new timeseries.timeseries.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[derive(GetSize)]
pub struct RecordingRule {
    pub id: u64,
    /// The valkey db key of the time series to output to. Optional.
    pub dest_key: String,
    /// The name of the time series to output to. Must be a valid metric name.
    pub name: String,
    /// The PromQL expression to evaluate. Every evaluation cycle this is
    /// evaluated at the current time, and the result recorded as a new set of
    /// time series with the metric name as given by 'record'.
    pub expr: String,
    /// Labels to add or overwrite before storing the result.
    pub labels: HashMap<String, String>,
    pub group_id: u64,
    /// The maximum number of state entries to store.
    pub max_entries_limit: Option<usize>,
    /// state stores recent state changes during evaluations
    pub state: Vec<RuleStateEntry>,
    pub metrics: RecordingRuleMetrics,
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[derive(GetSize)]
pub struct RecordingRuleMetrics {
    pub(crate) errors: AtomicU64,
    pub(crate) samples: AtomicU64,
}

impl Clone for RecordingRuleMetrics {
    fn clone(&self) -> Self {
        RecordingRuleMetrics {
            errors: AtomicU64::new(self.errors.load(Ordering::Relaxed)),
            samples: AtomicU64::new(self.samples.load(Ordering::Relaxed)),
        }
    }
}

impl RecordingRule {
    pub fn new(group: &Group, cfg: RuleConfig) -> Self {
        RecordingRule {
            id: cfg.id,
            dest_key: Default::default(),
            name: cfg.record,
            expr: cfg.expr,
            labels: cfg.labels,
            group_id: group.id(),
            metrics: Default::default(),
            max_entries_limit: cfg.update_entries_limit,
            state: Vec::new(),
        }
    }

    fn to_time_series(&self, key: String, metric: MetricName, samples: &[Sample]) -> RawTimeSeries {
        let mut metric = metric;

        // Collect label updates to avoid borrowing conflicts
        let mut updates = Vec::new();
        for (k, v) in &self.labels {
            if let Some(value) = metric.label_value(k) {
                if value != v {
                    let new_key = format!("exported_{k}");
                    updates.push((new_key, value.to_string()));
                    continue;
                }
            }
            updates.push((k.clone(), v.clone()));
        }

        // Apply updates
        for (k, v) in updates {
            metric.set(&k, &v);
        }

        let mut labels = metric.labels.clone();
        labels.insert(0, Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: self.name.clone(),
        });

        RawTimeSeries {
            key,
            samples: samples.to_vec(),
            labels,
        }
    }

    fn to_instant_time_series(&self, r: InstantQueryResult) -> RawTimeSeries {
        // TODO: properly construct name
        let key = self.dest_key.clone();
        self.to_time_series(key, r.metric, &[r.sample])
    }

    fn to_range_time_series(&self, r: RangeQueryResult) -> RawTimeSeries {
        // TODO: properly construct name
        let key = self.dest_key.clone();
        // todo: generate key for range query
        self.to_time_series(key, r.metric, &r.samples)
    }

    fn push_state(&mut self, state: RuleStateEntry) {
        self.state.push(state);
        let max_entries = self.max_entries_limit.unwrap_or(DEFAULT_RULE_UPDATE_ENTRIES_LIMIT);
        while self.state.len() > max_entries {
            self.state.remove(0);
        }
    }
    
}

impl Rule for RecordingRule {
    fn id(&self) -> u64 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Recording
    }

    fn expr(&self) -> &str {
        &self.expr
    }

    fn exec(&mut self, querier: &AlertDatasource, ts: Timestamp, limit: usize) -> AlertsResult<Vec<RawTimeSeries>> {
        let start = current_time_millis();

        let mut cur_state = RuleStateEntry {
            time: start,
            at: ts,
            ..Default::default()
        };

        let q_metrics = match querier.query(&self.expr, ts) {
            Ok(res) => res,
            Err(e) => {
                self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                cur_state.err = Some(AlertsError::QueryExecutionError(self.expr.clone()));
                self.state.push(cur_state);
                return Err(e);
            }
        };
        cur_state.duration = Duration::from_millis((current_time_millis() - start) as u64);

        let num_series = q_metrics.len();
        cur_state.samples = num_series;

        if limit > 0 && num_series > limit {
            // todo
            let msg = format!("exec exceeded limit of {limit} with {num_series} series");
            let err = AlertsError::QueryExecutionError(msg);
            cur_state.err = Option::from(err.clone());
            self.push_state(cur_state);
            return Err(err);
        }

        cur_state.series_fetched = num_series;

        let mut duplicates: AHashSet<String> = AHashSet::with_capacity(num_series);
        let mut tss: Vec<RawTimeSeries> = Vec::with_capacity(num_series);
        for r in q_metrics.into_iter() {
            let ts = self.to_instant_time_series(r);
            let key = stringify_labels(&ts);
            if duplicates.contains(&key) {
                let msg = format!(
                    "original metric {:?}; resulting labels {key}: {ERR_DUPLICATE}",
                    &ts.labels
                );
                self.push_state(cur_state);
                return Err(AlertsError::DuplicateSeries(msg));
            }
            duplicates.insert(key);
            tss.push(ts)
        }

        self.push_state(cur_state);
        Ok(tss)
    }

    /// `exec_range` executes recording rules on the given time range similarly to Exec.
    /// It doesn't update internal states of the Rule and meant to be used just to get time series
    /// for backfilling.
    fn exec_range(&mut self, querier: &AlertDatasource, start: Timestamp, end: Timestamp) -> AlertsResult<Vec<RawTimeSeries>> {
        let res = querier
            .query_range(&self.expr, start, end)
            .map_err(|e| AlertsError::QueryExecutionError(format!("{}: {:?}", self.expr, e)))?;

        let mut duplicates: AHashSet<String> = AHashSet::with_capacity(res.len());
        let mut tss = Vec::with_capacity(res.len());
        for s in res.data.into_iter() {
            let ts = self.to_range_time_series(s);
            let key = stringify_labels(&ts);
            if duplicates.contains(&key) {
                let msg = format!(
                    "original metric {:?}; resulting labels {}: {ERR_DUPLICATE}",
                    &ts.labels, key,
                );
                return Err(AlertsError::DuplicateSeries(msg));
            }
            duplicates.insert(key);
            tss.push(ts)
        }
        Ok(tss)
    }

    fn update_with(&mut self, other: &dyn Rule) -> AlertsResult<()> {
        if other.rule_type() != RuleType::Recording {
            let msg = format!("BUG: attempt to update recording rules with wrong type {}", other.rule_type());
            return Err(AlertsError::Generic(msg)); // todo: better error
        }
        
        let rr = other.as_any().downcast_ref::<RecordingRule>().unwrap();
        
        self.expr.clone_from(&rr.expr);
        self.labels.clone_from(&rr.labels);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub fn stringify_labels(ts: &RawTimeSeries) -> String {
    let mut labels = ts.labels.clone();
    let mut b = String::with_capacity(40); // todo: better capacity calculation.
    labels.sort();
    for (i, label) in ts.labels.iter().enumerate() {
        b.push_str(&format!("{}=\"{}\"", &label.name, enquote('"', &label.value)));
        if i < labels.len() - 1 {
            b.push(',')
        }
    }
    b
}
