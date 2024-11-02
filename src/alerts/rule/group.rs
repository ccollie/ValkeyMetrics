use std::collections::HashMap;
use std::default::Default;
use std::hash::Hasher;
use std::ops::Add;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::vec;
use ahash::AHashMap;
use enquote::enquote;
use crate::common::types::{Label, Timestamp, TimestampTrait};
use serde::{Deserialize, Serialize};
use tracing::info;
use valkey_module::Context;
use xxhash_rust::xxh3::Xxh3;
use crate::alerts::{AlertsError, AlertsResult, QuerierBuilder, QuerierParams};
use crate::alerts::notifier::{AlertNotifier, Notifier};
use crate::alerts::rule::{AlertingRule, GroupConfig, RecordingRule, Rule, RuleType};
use crate::alerts::rule::executor::Executor;
use crate::common::{current_time_millis, METRIC_NAME_LABEL};
use crate::config::get_global_settings;

/// Group is an entity for grouping rules
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Group {
    pub id: u64,
    pub name: String,
    pub alerting_rules: Vec<AlertingRule>,
    pub recording_rules: Vec<RecordingRule>,
    pub interval: Duration,
    pub eval_offset: Duration,
    /// Adjusts the `time` parameter of group evaluation requests to compensate for intentional query delay from the datasource.
    /// By default, the value is inherited from the `-rule.evalDelay` env var - see its description for details.
    /// If group has `latency_offset` set in `params`, then it is recommended to set `eval_delay` equal to `latency_offset`.
    /// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5155 and https://docs.victoriametrics.com/keyconcepts/#query-latency.
    pub eval_delay: Option<Duration>,
    /// The evaluation timestamp will be aligned with group's interval,
    /// instead of using the actual timestamp that evaluation happens at.
    ///
    /// It is enabled by default to get more predictable results
    /// and to visually align with graphs plotted via Grafana or vmui.
    /// When comparing with raw queries, remember to use `step` equal to evaluation interval.
    ///
    /// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5049
    pub eval_alignment: Option<bool>,
    /// Limit limits the number of alerts or recording results the rule within this group can produce.
    /// On exceeding the limit, rule will be marked with an error and all its results will be discarded.
    /// 0 is no limit.
    pub limit: usize,
    pub last_evaluation: Timestamp,
    pub labels: AHashMap<String, String>,
    pub params: AHashMap<String, String>,
    pub notifier_headers: HashMap<String, String>,
    pub notifiers: Vec<AlertNotifier>,
    pub metrics: GroupMetrics,
    pub disabled: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GroupMetrics {
    pub iteration_total: AtomicU64,
    pub iteration_duration: AtomicU64,
    pub iteration_missed: AtomicU64,
    pub iteration_interval: AtomicU64,
}

impl Clone for GroupMetrics {
    fn clone(&self) -> Self {
        GroupMetrics {
            iteration_total: AtomicU64::new(self.iteration_total.load(Ordering::Relaxed)),
            iteration_duration: AtomicU64::new(self.iteration_duration.load(Ordering::Relaxed)),
            iteration_missed: AtomicU64::new(self.iteration_missed.load(Ordering::Relaxed)),
            iteration_interval: AtomicU64::new(self.iteration_interval.load(Ordering::Relaxed)),
        }
    }
}

impl Group {
    pub fn from_config(cfg: GroupConfig, default_interval: Duration, labels: Vec<Label>) -> Group {
        let labels_empty = cfg.labels.is_empty();
        let mut g = Group {
            name: cfg.name,
            interval: Duration::default(),
            eval_offset: Duration::default(),
            limit: cfg.limit,
            params: cfg.params.unwrap_or_default().clone(),
            labels: cfg.labels.into(),
            eval_alignment: cfg.eval_alignment,
            ..Default::default()
        };
        if g.interval.is_zero() {
            g.interval = default_interval
        }
        if let Some(eval_offset) = cfg.eval_offset {
            g.eval_offset = eval_offset.clone()
        }
        for h in cfg.notifier_headers.iter() {
            g.notifier_headers.insert(h.key.clone(), h.value.clone());
        }
        g.metrics = new_group_metrics(&g);

        for mut r in cfg.rules.into_iter() {
            let mut extra_labels: AHashMap<String, String> = Default::default();
            let name = r.name();
            // apply external labels
            if !labels.is_empty() {
                for (k, v) in r.labels.iter() {
                    extra_labels.insert(k.clone(), v.clone());
                }
            }
            // apply group labels, it has priority on external labels
            if !labels_empty {
                merge_hashes(&g.name, name, &mut extra_labels, &g.labels);
            }
            // apply rules labels, it has priority on other labels
            if !extra_labels.is_empty() {
                let mut rule_labels = Vec::with_capacity(r.labels.len());
                for (k, v) in r.labels.iter() {
                    rule_labels.push(Label {
                        name: k.clone(),
                        value: v.clone(),
                    });
                }
                merge_hashes(&g.name, name, &mut extra_labels, &r.labels);
                r.labels = extra_labels;

                if matches!(r.rule_type(), RuleType::Alerting) {
                    let ar = AlertingRule::new(&g, r);
                    g.alerting_rules.push(ar);
                } else {
                    let rr = RecordingRule::new(&g, r);
                    g.recording_rules.push(rr);
                }
            }
        }
        g
    }

    /// id return unique group id that consists of rules file and group name
    pub(crate) fn id(&self) -> u64 {
        let mut hasher: Xxh3 = Xxh3::new();
        hasher.write(self.name.as_bytes());
        hasher.write(b"\xff");
        hasher.write_u128(self.interval.as_millis());
        let millis = self.eval_offset.as_millis();
        hasher.write_u128(millis);
        hasher.digest()
    }

    pub fn rule_count(&self) -> usize {
        self.alerting_rules.len() + self.recording_rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rule_count() == 0
    }

    /// restores alerts state for group rules
    pub fn restore(&mut self, ctx: &Context, qb: impl QuerierBuilder, ts: Timestamp, look_back: Duration) -> AlertsResult<()> {
        for ar in self.alerting_rules.iter_mut() {
            if ar.r#for.is_zero() {
                continue;
            }
            let querier = qb.build_with_params(QuerierParams {
                evaluation_interval: self.interval.clone(),
                eval_offset: Default::default(),
                query_params: self.params.clone(),
                debug: ar.debug,
            });

            ar.restore(ctx, &querier, ts, look_back)
                .map_err(|e| AlertsError::RuleRestoreError(format!("{}: {:?}", ar.expr, e)))?;
        }
        Ok(())
    }

    /// updates existing group with passed group object. This function ignores group
    /// evaluation interval change. It supposed to be updated in group.start function.
    /// Not thread-safe.
    pub fn update_with(&mut self, new_group: &Group) -> AlertsResult<()> {
        let mut alert_rules_registry: HashMap<u64, &AlertingRule> = HashMap::with_capacity(new_group.alerting_rules.len());
        let mut recording_rules_registry: HashMap<u64, &RecordingRule> = HashMap::with_capacity(new_group.recording_rules.len());

        let mut to_delete: Vec<usize> = vec![];

        for ar in new_group.alerting_rules.iter() {
            alert_rules_registry.insert(ar.id(), ar);
        }

        for rr in new_group.recording_rules.iter() {
            recording_rules_registry.insert(rr.id(), rr);
        }

        for (i, ar) in self.alerting_rules.iter_mut().enumerate() {
            let id = ar.id();
            if let Some(rule) = alert_rules_registry.get(&id) {
                ar.update_with(rule);
                continue;
            }
            to_delete.push(i);
        }

        // need to do this more efficiently
        for ofs in to_delete.iter().rev() {
            self.alerting_rules.remove(*ofs);
        }
        to_delete.clear();

        for (i, rr) in self.recording_rules.iter_mut().enumerate() {
            let id = rr.id();
            if let Some(rule) = recording_rules_registry.get(&id) {
                rr.update_with(rule);
                continue;
            }
            to_delete.push(i);
        }

        // need to do this more efficiently
        for ofs in to_delete.iter().rev() {
            self.recording_rules.remove(*ofs);
        }

        // note that self.interval is not updated here so the value can be compared later in
        // group.start function
        self.params = new_group.params.clone();
        self.notifier_headers = new_group.notifier_headers.clone();
        self.labels = new_group.labels.clone();
        self.limit = new_group.limit;
        Ok(())
    }

    pub fn close(&mut self) {
        self.metrics.iteration_total.store(0, Ordering::Relaxed);
        self.metrics.iteration_duration.store(0, Ordering::Relaxed);
        self.metrics.iteration_missed.store(0, Ordering::Relaxed);
        self.metrics.iteration_interval.store(0, Ordering::Relaxed);
        self.last_evaluation = 0;
    }

    pub fn get_alerting_rule(&self, name: &str) -> Option<&AlertingRule> {
        self.alerting_rules.iter().find(|ar| ar.name == name)
    }

    pub fn get_recording_rule(&self, name: &str) -> Option<&RecordingRule> {
        self.recording_rules.iter().find(|ar| ar.name == name)
    }

    pub fn contains_rule(&self, name: &str) -> bool {
        self.alerting_rules.iter().find(|ar| ar.name == name).is_some() ||
            self.recording_rules.iter().find(|rr| rr.name == name).is_some()
    }

    pub fn remove_rule(&mut self, name: &str) -> bool {
        let rule = self.alerting_rules
            .iter()
            .position(|ar| ar.name == name)
            .map(|i| self.alerting_rules.remove(i));

        if rule.is_none() {
            return false;
        }

        self.recording_rules.iter()
            .position(|rr| rr.name == name)
            .map(|i| self.recording_rules.remove(i))
            .is_some()
    }
    
    pub(crate) fn remove_notifier(&mut self, name: &str) -> bool {
        let count = self.notifiers.len();
        self.notifiers.retain(|notifier| notifier.addr() != name);
        count != self.notifiers.len()
    }
    
    pub(crate) fn eval(&mut self, e: &Executor, ts: Timestamp) {
        self.metrics.iteration_total.fetch_add(1, Ordering::Relaxed);

        let start = current_time_millis();

        if self.is_empty() {
            self.last_evaluation = start;
            return;
        }

        let resolve_duration = self.resolve_duration();
        let ts = self.adjust_req_timestamp(ts);

        let errs = e.exec_concurrently(self, ts, resolve_duration, self.limit);

        for err in errs {
            if err != nil {
                let msg = format!("group {}: {:?}", self.name, err);
                tracing::warn!("{}", msg);
            }
        }
        self.last_evaluation = start
    }

    pub(crate) fn on_tick(&self, e: &Executor, eval_ts: Timestamp) {
        self.metrics.iteration_interval.fetch_add(1, Ordering::Relaxed);
        let current = current_time_millis();
        let elapsed = eval_ts - self.last_evaluation;
        let interval_millis = self.interval.as_millis() as i64;
        let mut missed = elapsed / (interval_millis - 1) as u64 as i64;
        if missed < 0 {
            // missed can become < 0 due to irregular delays during evaluation
            // which can result in time.since(eval_ts) < g.interval
            missed = 0;
        }
        if missed > 0 {
            self.metrics.iteration_missed.fetch_add(missed as u64, Ordering::Relaxed);
        }
        let eval_ts = current.add((missed + 1) * interval_millis);
        self.eval(e, eval_ts)
    }

    pub(super) fn on_update(&mut self, ng: &Group, e: &mut Executor) -> AlertsResult<()> {
        self.update_with(ng).map_err(|_| {
            return AlertsError::Generic(format!("group {}: failed to update", self.name))
        })?;

        // ensure that staleness is tracked for existing rules only
        // e.purge_stale_results(&self.alerting_rules);
        e.purge_stale_series(&self.recording_rules);

        let mut headers = HashMap::new();
        for (key, value) in self.notifier_headers.iter() {
            headers.insert(key.clone(), value.clone());
        }

        info!("group re-started");
        Ok(())
    }

    pub(super) fn resolve_duration(&self) -> Duration {
        let settings = get_global_settings();
        get_resolve_duration(
            self.interval,
            &settings.resend_delay,
            &settings.max_resolve_duration)
    }

    pub(crate) fn adjust_req_timestamp(&self, timestamp: Timestamp) -> Timestamp {
        if !self.eval_offset.is_zero() {
            let offset = self.eval_offset.as_millis() as i64; // todo: make sure it doesn't overflow
            // calculate the min timestamp on the evaluationInterval
            let interval_start = timestamp.truncate(self.interval);
            let ts = interval_start + offset;
            if timestamp < ts {
                // if passed timestamp is before the expected evaluation offset,
                // then we should adjust it to the previous evaluation round.
                // E.g. request with evaluationInterval=1h and evaluationOffset=30m
                // was evaluated at 11:20. Then the timestamp should be adjusted
                // to 10:30, to the previous evaluationInterval.
                let interval = self.interval.as_millis().max(i64::MAX as u128) as i64;
                return ts.saturating_sub(interval)
            }
            // eval_offset shouldn't interfere with eval_alignment, so we return it immediately
            return ts
        }
        if self.eval_alignment.unwrap_or(true) {
            // align query time with interval to get similar result with grafana when plotting time series.
            // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5049
            // and https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1232
            return timestamp.truncate(self.interval)
        }
        timestamp
    }
}

fn new_group_metrics(_g: &Group) -> GroupMetrics {
    let m = GroupMetrics::default();
    m
}

// merges group rule labels into result map
// set2 has priority over set1.
fn merge_labels(group_name: &str, rule_name: &str, set1: &Vec<Label>, set2: &Vec<Label>) -> Vec<Label> {
    let mut r: Vec<Label> = set1.clone();

    for label in set2.iter() {
        let prev_v = r.iter().find(|x| x.name == label.name);
        if let Some(prev) = prev_v {
            let k = &label.name;
            let v = &label.value;
            info!("label {k}={prev} for rule {}.{} overwritten with external label {k}={v}",
                  group_name,
                  rule_name);
        }
        r.push(label.clone());
    }
    r
}

fn merge_hashes(group_name: &str, rule_name: &str, dest: &mut AHashMap<String, String>, set2: &AHashMap<String, String>) {
    for (k, v) in set2.iter() {
        use std::collections::hash_map::Entry;
        match dest.entry(k.clone()) {
            Entry::Occupied(mut entry) => {
                info!("hash {k} for rule {}.{} overwritten with external hash {k}={v}",
                      group_name,
                      rule_name);
                *entry.get_mut() = v.clone();
            }
            Entry::Vacant(entry) => {
                entry.insert(k.clone());
            }
        }
    }
}

/// get_resolve_duration returns the duration after which firing alert can be considered as resolved.
fn get_resolve_duration(group_interval: Duration,
                        delta: &Duration,
                        max_duration: &Duration) -> Duration {
    let mut delta = *delta;
    if group_interval > delta {
        delta = group_interval
    }
    let mut resolve_duration = delta * 4;
    if !max_duration.is_zero() && resolve_duration > *max_duration {
        resolve_duration = *max_duration
    }
    resolve_duration
}


pub(super) fn labels_to_string(labels: &[Label]) -> String {
    let capacity = labels.iter().fold(0, |acc, l| acc + l.name.len() + l.value.len() + 2);
    let mut b = String::with_capacity(capacity);
    b.push('{');
    for (i, label) in labels.iter().enumerate() {
        if label.name.is_empty() {
            b.push_str(METRIC_NAME_LABEL);
        } else {
            b.push_str(&label.name)
        }
        b.push('=');
        b.push_str(&*enquote('"', &label.value));
        if i < labels.len() - 1 {
            b.push(',')
        }
    }
    b.push('}');
    b
}