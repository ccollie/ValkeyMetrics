use std::collections::HashMap;
use std::default::Default;
use std::hash::Hasher;
use std::ops::Add;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use ahash::AHashMap;

use enquote::enquote;
use metricsql_engine::TimestampTrait;
use metricsql_parser::common::METRIC_NAME;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3;
use crate::common::current_time_millis;
use crate::common::types::Label;
use crate::config::get_global_settings;

use crate::rules::{EvalContext, Rule, RuleType};
use crate::rules::alerts::{AlertingRule, AlertsError, AlertsResult, DataSourceType, GroupConfig, Notifier, RecordingRule, WriteQueue};
use crate::rules::alerts::datasource::datasource::{QuerierBuilder, QuerierParams};
use crate::rules::alerts::executor::Executor;
use crate::ts::{Labels, Timestamp};

/// Group is an entity for grouping rules
#[derive(Debug, Clone, Hash, PartialEq, Serialize, Deserialize)]
pub struct Group {
    pub name: String,
    pub id: u64,
    file: String,
    pub source_type: DataSourceType,
    pub alerting_rules: Vec<AlertingRule>,
    pub recording_rules: Vec<RecordingRule>,
    pub interval: Duration,
    pub eval_offset: Duration,
    pub limit: usize,
    pub last_evaluation: Timestamp,
    pub labels: Labels,
    pub params: AHashMap<String, String>,
    pub headers: Labels,
    pub notifier_headers: AHashMap<String, String>,
    pub metrics: GroupMetrics,
    #[serde(skip)]
    cancelled: AtomicBool,
    #[serde(skip)]
    first_run: AtomicBool,
    #[serde(skip)]
    concurrency: usize,
    checksum: String,
    /// eval_alignment will make the timestamp of group query requests be aligned with interval
    pub eval_alignment: Option<bool>,
    #[serde(skip)]
    pub timer_id: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GroupMetrics {
    iteration_total: AtomicU64,
    iteration_duration: AtomicU64,
    iteration_missed: AtomicU64,
    iteration_interval: AtomicU64,
}

impl Group {
    pub fn from_config(cfg: GroupConfig, default_interval: Duration, labels: Labels) -> Group {
        let mut cfg = cfg;
        let mut g = Group {
            source_type: cfg.datasource_type,
            name: cfg.name,
            id: 0,
            file: cfg.file.unwrap_or_default(),
            alerting_rules: vec![],
            interval: Duration::default(),
            eval_offset: Duration::default(),
            limit: cfg.limit,
            concurrency: cfg.concurrency.min(1),
            checksum: cfg.checksum,
            params: cfg.params.unwrap_or_default().clone(),
            headers: Labels::new(),
            notifier_headers: Default::default(),
            labels: cfg.labels.into(),
            last_evaluation: 0,
            metrics: GroupMetrics::default(),
            recording_rules: vec![],
            cancelled: Default::default(),
            first_run: AtomicBool::new(true),
            eval_alignment: cfg.eval_alignment,
            timer_id: 0,
        };
        if g.interval.is_zero() {
            g.interval = default_interval
        }
        if let Some(eval_offset) = cfg.eval_offset {
            g.eval_offset = eval_offset.clone()
        }
        for h in cfg.headers {
            g.headers.insert(h.key, h.value);
        }
        for h in cfg.notifier_headers {
            g.notifier_headers.insert(h.key, h.value);
        }
        g.metrics = new_group_metrics(&g);

        for r in cfg.rules.iter_mut() {
            let mut extra_labels: Labels = Default::default();
            let name = r.name();
            // apply external labels
            if !labels.is_empty() {
                extra_labels = labels.clone();
            }
            // apply group labels, it has priority on external labels
            if !cfg.labels.is_empty() {
                extra_labels = merge_labels(&g.name, name, &extra_labels, &g.labels)
            }
            // apply rules labels, it has priority on other labels
            if !extra_labels.is_empty() {
                let labels = merge_labels(&g.name, name, &extra_labels, &r.labels);
                r.labels = labels.into();
            }

            if r.rule_type() == RuleType::Alerting {
                let ar = AlertingRule::new(&r, &g);
                g.alerting_rules.push(ar);
            } else {
                let rr = RecordingRule::new(&g, &r);
                g.recording_rules.push(rr);
            }
        }
        g
    }

    /// id return unique group id that consists of rules file and group name
    pub(crate) fn id(&self) -> u64 {
        let mut hasher: Xxh3 = Xxh3::new();

        hasher.write(self.file.as_bytes());
        hasher.write(b"\xff");
        hasher.write(self.name.as_bytes());
        hasher.write(self.source_type.to_string().as_bytes());
        hasher.write_u128(self.interval.as_millis());
        if let Some(offset) = self.eval_offset {
            let millis = offset.as_millis();
            hasher.write_i128(millis);
        }
        return hasher.digest();
    }

    pub fn rule_count(&self) -> usize {
        self.alerting_rules.len() + self.recording_rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rule_count() == 0
    }

    /// restores alerts state for group rules
    pub fn restore(&mut self, ctx: &EvalContext, qb: impl QuerierBuilder, ts: Timestamp, look_back: Duration) -> AlertsResult<()> {
        let settings = get_global_settings();
        for ar in self.alerting_rules.iter_mut() {
            if ar.r#for.is_zero() {
                continue;
            }
            let q = qb.build_with_params(QuerierParams {
                data_source_type: self.source_type.clone(),
                evaluation_interval: self.interval.clone(),
                eval_offset: Default::default(),
                query_params: self.params.clone(),
                headers: self.headers.clone(),
                debug: ar.debug,
            });
            ar.restore(ctx, ts, look_back)
                .map_err(|e| AlertsError::RuleRestoreError(format!("{}: {:?}", ar.expr, e)))?;
        }
        Ok(())
    }

    /// update_with updates existing group with passed group object. This function ignores group
    /// evaluation interval change. It supposed to be updated in group.start function.
    /// Not thread-safe.
    pub fn update_with(&mut self, new_group: &Group) -> AlertsResult<()> {
        let mut alert_rules_registry: HashMap<u64, &AlertingRule> = HashMap::with_capacity(new_group.alerting_rules.len());
        let mut recording_rules_registry: HashMap<u64, &RecordingRule> = HashMap::with_capacity(new_group.recording_rules.len());

        let mut to_delete: Vec<usize> = vec![];

        for ar in new_group.alerting_rules.iter() {
            alert_rules_registry.insert(ar.ID(), ar);
        }

        for rr in new_group.recording_rules.iter() {
            recording_rules_registry.insert(rr.ID(), rr);
        }

        for (i, ar) in self.alerting_rules.iter_mut().enumerate() {
            let id = ar.id();
            if let Some(rule) = alert_rules_registry.get(&id) {
                ar.update_with(rule)?;
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
                rr.update_with(rule)?;
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
        self.source_type = new_group.source_type.clone();
        self.concurrency = new_group.concurrency;
        self.params = new_group.params.clone();
        self.headers = new_group.headers.clone();
        self.notifier_headers = new_group.notifier_headers.clone();
        self.labels = new_group.labels.clone();
        self.limit = new_group.limit;
        self.checksum = new_group.checksum.to_string();
        Ok(())
    }

    pub fn get_rule(&self, name: &str) -> Option<&impl Rule> {
        let mut rule = self.alerting_rules.iter().find(|ar| ar.name() == name);
        if rule.is_some() {
            return rule;
        }
        self.recording_rules.iter().find(|rr| rr.name() == name)
    }

    pub fn contains_rule(&self, name: &str) -> bool {
        self.get_rule(name).is_some()
    }

    pub fn remove_rule(&mut self, name: &str) -> bool {
        let mut rule = self.alerting_rules.iter().position(|ar| ar.name() == name)
            .map(|i| self.alerting_rules.remove(i));
        if rule.is_none() {
            return false;
        }
        self.recording_rules.iter().position(|rr| rr.name() == name)
            .map(|i| self.recording_rules.remove(i))
            .is_some()
    }

    pub(super) fn eval<'a>(&mut self, ts: Timestamp) {
        self.metrics.iteration_total.fetch_add(1, Ordering::Relaxed);

        let start = current_time_millis();

        if self.is_empty() {
            self.last_evaluation = start;
            return;
        }

        let resolve_duration = self.resolve_duration();
        let ts = self.adjust_req_timestamp(ts);

        let errs = e.exec_concurrently(g.rules, ts, resolve_duration, self.limit);
        for err in errs {
            if err != nil {
                let msg = format!("group {}: {:?}", self.name, err);
                ctx.log_warning(&msg);
            }
        }
        self.last_evaluation = start
    }

    pub fn run<'a>(&mut self,
                   ctx: &'a EvalContext,
                   nts: fn() -> Vec<dyn Notifier>,
                   write_queue: Arc<WriteQueue>,
                   qb: impl QuerierBuilder) {
        let settings = get_global_settings();
        let eval_ts = current_time_millis();

        let mut e = Executor::new(nts, &self.notifier_headers, write_queue);

        // restore the rules state after the first evaluation so only active alerts can be restored.
        // todo: i doubt we need atomics here
        if self.first_run.fetch_or(false, Ordering::Relaxed) {
            self.first_run.store(false, Ordering::Acquire);
            if let Err(err) = self.restore(ctx, qb, eval_ts, settings.look_back) {
                let msg = format!("error while restoring ruleState for group {}: {:?}", &self.name, err);
                ctx.log_warning(&msg);
            }
        }

        // ensure that staleness is tracked for existing rules only
        e.purge_stale_series(g.rules);
        e.notifier_headers = self.notifier_headers.clone();

        let mut missed = (self.last_evaluation - eval_ts) / self.interval - 1;
        if missed < 0 {
            // missed can become < 0 due to irregular delays during evaluation
            // which can result in time.since(eval_ts) < g.interval
            missed = 0;
        }
        if missed > 0 {
            self.metrics.iteration_missed.fetch_add(missed, Ordering::Relaxed);
        }
        let eval_ts = eval_ts.add((missed + 1) * self.interval);
        self.eval(ctx, eval_ts)
    }

    pub(super) fn resolve_duration(&self) -> Duration {
        let settings = get_global_settings();
        get_resolve_duration(
            self.interval, &settings.resend_delay,
            &settings.max_resolve_duration)
    }

    pub(super) fn adjust_req_timestamp(&self, timestamp: Timestamp) -> Timestamp {
        if let Some(offset) = self.eval_offset {
            // calculate the min timestamp on the evaluationInterval
            let interval_start = timestamp.truncate(self.interval);
            let ts = interval_start.add(offset);
            if timestamp.Before(ts) {
                // if passed timestamp is before the expected evaluation offset,
                // then we should adjust it to the previous evaluation round.
                // E.g. request with evaluationInterval=1h and evaluationOffset=30m
                // was evaluated at 11:20. Then the timestamp should be adjusted
                // to 10:30, to the previous evaluationInterval.
                return ts.add(-self.interval)
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
        return timestamp
    }
}

fn new_group_metrics(g: &Group) -> GroupMetrics {
    let mut m = GroupMetrics::default();
    return m;
}

// merges group rule labels into result map
// set2 has priority over set1.
pub(crate) fn merge_labels(group_name: &str, rule_name: &str, set1: &Labels, set2: &Labels) -> Labels {
    let mut r: Labels = Default::default();
    for (k, v) in set1.iter() {
        r.insert(k.clone(), v.clone());
    }
    for (k, v) in set2.iter() {
        let prev_v = r.get(k);
        if prev_v.is_some() {
            logger.Infof("label {k}={prev_v} for rule {}.{} overwritten with external label {k}={v}",
                         group_name,
                         rule_name)
        }
        r.insert(k.clone(), v.clone());
    }
    return r;
}


/// get_resolve_duration returns the duration after which firing alert can be considered as resolved.
fn get_resolve_duration(group_interval: Duration, delta: &Duration,
                        max_duration: &Duration) -> Duration {
    let mut delta = *delta;
    if group_interval > delta {
        delta = group_interval
    }
    let mut resolve_duration = *delta * 4;
    if !max_duration.is_zero() && resolve_duration > max_duration {
        resolve_duration = max_duration
    }
    return resolve_duration;
}

/// delay_before_start returns a duration on the interval between [ts..ts+interval].
/// delay_before_start accounts for `offset`, so returned duration should be always
/// bigger than the `offset`.
fn delay_before_start(ts: Timestamp, key: u64, interval: Duration, offset: Option<Duration>) -> Duration {
    let mut rand_sleep = interval * (key / (1 << 64)) as u32;
    let sleep_offset = Duration::from_millis((ts % interval.as_millis() as u64) as u64);
    if rand_sleep < sleep_offset {
        rand_sleep += interval
    }
    rand_sleep -= sleep_offset;
    // check if `ts` after rand_sleep is before `offset`,
    // if it is, add extra eval_offset to rand_sleep.
    // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3409.
    if let Some(offset) = offset {
        let tmp_eval_ts = ts.add(rand_sleep);
        if tmp_eval_ts < tmp_eval_ts.truncate(interval).add(*offset) {
            rand_sleep += *offset
        }
    }

    rand_sleep
}

pub(super) fn labels_to_string(labels: &[Label]) -> String {
    let capacity = labels.iter().fold(0, |acc, l| acc + l.name.len() + l.value.len() + 2);
    let mut b = String::with_capacity(capacity);
    b.push('{');
    for (i, label) in labels.iter().enumerate() {
        if label.name.is_empty() {
            b.push_str(METRIC_NAME);
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