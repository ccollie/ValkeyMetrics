use crate::alerts::notifications::Alert;
use crate::alerts::rules::executor::Executor;
use crate::alerts::rules::{AlertingRule, GroupConfig, MetricRule, RecordingRule, Rule, RuleType};
use crate::alerts::{AlertsError, AlertsResult, ALERT_SETTINGS};
use crate::common::types::{Label, Timestamp, TimestampTrait};
use crate::common::{current_time_millis, METRIC_NAME_LABEL};
use crate::query::{QuerierBuilder, QuerierParams};
use enquote::enquote;
use get_size::GetSize;
use metricsql_parser::ast::{Expr, MetricExpr};
use metricsql_parser::parser::parse as parse_expr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::default::Default;
use std::hash::Hasher;
use std::ops::Add;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;
use std::vec;
use topologic::AcyclicDependencyGraph;
use tracing::info;
use valkey_module::{Context};
use xxhash_rust::xxh3::Xxh3;

// `DependencyMap` describes the dependency associations between rules in a group whereby one rules uses the
// output metric produced by another rules in its expression (i.e. as its "input"). Basically an adjacency list
pub type DependencyMap = Vec<Vec<usize>>;

/// Group is an entity for grouping rules
#[derive(Debug, Default, GetSize)]
pub struct Group {
    pub id: u64,
    pub name: String,
    pub rules: Vec<MetricRule>,
    /// How often rules in the group are evaluated.
    pub interval: Duration,
    /// A Group will be evaluated at the exact offset in the range of [0...interval].
    /// E.g. for Group with `interval: 1h` and `eval_offset: 5m` the evaluation will
    /// start at 5th minute of the hour. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/3409
    /// `eval_offset` can't be bigger than `interval`.
    pub eval_offset: Duration,
    /// Adjusts the `time` parameter of group evaluation requests to compensate for intentional query delay from the datasource.
    /// By default, the value is inherited from the `-rules.evalDelay` env var - see its description for details.
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
    /// Limit limits the number of alerts or recording results the rules within this group can produce.
    /// On exceeding the limit, rules will be marked with an error and all its results will be discarded.
    /// 0 is no limit.
    pub limit: usize,
    /// Optional list of labels added to every rule within a group.
    /// It has priority over the external labels.
    /// Labels are commonly used for adding environment or tenant-specific tag.
    pub labels: HashMap<String, String>,
    /// Optional list of parameters
    ///  applied for all rules requests within a group
    /// For example:
    ///   params:
    ///     nocache: ["1"]                # disable caching for vmselect
    ///     denyPartialResponse: ["true"] # fail if one or more vmstorage nodes returned an error
    ///     extra_label: ["env=dev"]      # apply additional label filter "env=dev" for all requests
    /// see more details at https://docs.victoriametrics.com#prometheus-querying-api-enhancements
    pub params: HashMap<String, String>,
    pub notifier_headers: HashMap<String, String>,
    /// A DAG of rules ids represented as an adjacency list
    pub dependencies: Option<DependencyMap>,
    pub metrics: GroupMetrics,
    pub disabled: bool,
    pub last_evaluation: AtomicI64,
}

impl Clone for Group {
    fn clone(&self) -> Self {
        Group {
            id: self.id,
            name: self.name.clone(),
            rules: self.rules.clone(),
            interval: self.interval,
            eval_offset: self.eval_offset,
            eval_delay: self.eval_delay,
            eval_alignment: self.eval_alignment,
            limit: self.limit,
            last_evaluation: AtomicI64::new(self.last_evaluation.load(Ordering::Relaxed)),
            labels: self.labels.clone(),
            params: self.params.clone(),
            notifier_headers: self.notifier_headers.clone(),
            dependencies: self.dependencies.clone(),
            metrics: self.metrics.clone(),
            disabled: self.disabled,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, GetSize)]
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
    pub fn new() -> Self {
        let mut group = Group::default();
        group.eval_alignment = Some(true);
        group
    }

    pub fn from_config(cfg: GroupConfig, default_interval: Duration, labels: Vec<Label>) -> Group {
        let labels_empty = cfg.labels.is_empty();
        let mut g = Group {
            name: cfg.name,
            interval: Duration::default(),
            eval_offset: Duration::default(),
            limit: cfg.limit,
            params: cfg.params.unwrap_or_default().clone(),
            labels: cfg.labels,
            eval_alignment: cfg.eval_alignment,
            ..Default::default()
        };
        if g.interval.is_zero() {
            g.interval = default_interval
        }
        if let Some(eval_offset) = cfg.eval_offset {
            g.eval_offset = eval_offset
        }
        for h in cfg.notifier_headers.iter() {
            g.notifier_headers.insert(h.key.clone(), h.value.clone());
        }
        g.metrics = new_group_metrics(&g);

        for mut r in cfg.rules.into_iter() {
            let mut extra_labels: HashMap<String, String> = Default::default();
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
                    let ar = MetricRule::AlertingRule(AlertingRule::new(&g, r));
                    g.rules.push(ar);
                } else {
                    let rr = MetricRule::RecordingRule(RecordingRule::new(&g, r));
                    g.rules.push(rr);
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
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rule_count() == 0
    }

    /// restores alerts state for group rules
    pub fn restore(
        &mut self,
        ctx: &Context,
        qb: impl QuerierBuilder,
        ts: Timestamp,
        look_back: Duration,
    ) -> AlertsResult<()> {
        for ar in self.rules.iter_mut() {
            if let MetricRule::AlertingRule(alerting_rule) = ar {
                if alerting_rule.r#for.is_zero() {
                    continue;
                }
                let querier = qb.build_with_params(QuerierParams {
                    evaluation_interval: self.interval,
                    eval_offset: Default::default(),
                    query_params: self.params.clone(),
                    debug: alerting_rule.debug,
                });

                alerting_rule
                    .restore(ctx, &querier, ts, look_back)
                    .map_err(|e| {
                        AlertsError::RuleRestoreError(format!("{}: {:?}", alerting_rule.expr, e))
                    })?;
            }
        }
        Ok(())
    }

    /// updates existing group with passed group object. This function ignores group
    /// evaluation interval change. It supposed to be updated in group.start function.
    /// Not thread-safe.
    pub fn update_with(&mut self, new_group: &Group) -> AlertsResult<()> {
        let mut rules_registry = HashMap::new();

        for rule in new_group.rules.iter() {
            rules_registry.insert(rule.id(), rule);
        }

        let mut to_delete = vec![];

        for (i, or) in self.rules.iter_mut().enumerate() {
            let id = or.id();
            if let Some(rule) = rules_registry.get(&id) {
                or.update_with(*rule)?;
            } else {
                to_delete.push(i);
            }
        }

        // need to do this more efficiently
        for ofs in to_delete.iter().rev() {
            self.rules.remove(*ofs);
        }
        to_delete.clear();

        for rule in rules_registry.values() {
            let rule = (*rule).clone();
            self.rules.push(rule);
        }

        // note that self.interval is not updated here so the value can be compared later in
        // group.start function
        self.params.clone_from(&new_group.params);
        self.notifier_headers
            .clone_from(&new_group.notifier_headers);
        self.labels.clone_from(&new_group.labels);
        self.limit = new_group.limit;
        self.update_dependencies();

        Ok(())
    }

    pub fn close(&mut self) {
        self.metrics.iteration_total.store(0, Ordering::Relaxed);
        self.metrics.iteration_duration.store(0, Ordering::Relaxed);
        self.metrics.iteration_missed.store(0, Ordering::Relaxed);
        self.metrics.iteration_interval.store(0, Ordering::Relaxed);
        self.last_evaluation = AtomicI64::new(0);
    }

    pub fn add_rule(&mut self, rule: MetricRule) -> AlertsResult<()> {
        if rule.id() == 0 {
            // todo: error
        }
        if self.get_rule_by_id(rule.id()).is_some() {
            let as_string = rule.to_string();
            return Err(AlertsError::RuleAlreadyExists(as_string));
        }
        self.rules.push(rule);
        self.update_dependencies();
        Ok(())
    }

    pub fn get_rule_by_id(&self, id: u64) -> Option<&MetricRule> {
        self.rules.iter().find(|r| r.id() == id)
    }

    pub fn get_alert(&self, id: u64) -> Option<&Alert> {
        for rule in self.rules.iter() {
            if let MetricRule::AlertingRule(alerting_rule) = rule {
                let alert = alerting_rule.alerts.get(&id);
                if alert.is_some() {
                    return alert;
                }
            }
        }
        None
    }

    pub fn remove_rule_by_id(&mut self, id: u64) -> bool {
        let len = self.rules.len();
        self.rules.retain(|x| x.id() != id);
        let changed = len != self.rules.len();
        if changed {
            self.update_dependencies();
        }
        changed
    }

    /// `build_dependencies` builds an adjacency list based DAG of the relationships between rules within a group.
    ///
    /// Alert rules, by definition, cannot have any dependents - but they can have dependencies. Any recording rules on whose
    /// output an Alert rules depends will not be able to run concurrently.
    ///
    /// There is a class of rules expressions which are considered "indeterminate", because either relationships cannot be
    /// inferred, or concurrent evaluation of rules depending on these series would produce undefined/unexpected behaviour:
    ///   - wildcard queries like {cluster="prod1"} which would match every series with that label selector
    ///   - any "meta" series (series produced by Prometheus itself) like ALERTS, ALERTS_FOR_STATE
    ///
    /// Rules which are independent can run concurrently without side effects.
    ///
    /// Returns an adjacency list of rules ids which represents the topologically sorted execution order
    /// of rules within the group. The first index contains rules that have no dependencies.
    /// Each subsequent element contains the rules that depend on the rules in the previous layer.
    ///
    /// None is returned if the group contains "indeterminate" rules expressions
    fn build_dependencies(&self) -> Option<DependencyMap> {
        if self.rules.len() <= 1 {
            // No relationships if group has 1 or fewer rules.
            return None;
        }

        // collect rules which haven't added any dependencies to the graph.
        let mut no_dependents: Vec<usize> = Vec::with_capacity(self.rules.len());
        let graph: AcyclicDependencyGraph<usize> = AcyclicDependencyGraph::new();

        let mut is_indeterminate = false;

        for (i, rule) in self.rules.iter().enumerate() {
            if let Some(vector_selector) = inspect_query(rule) {
                if vector_selector.name.is_none() && !vector_selector.matchers.is_empty() {
                    // indeterminate
                    is_indeterminate = true;
                    break;
                }

                let name = vector_selector.name.unwrap_or_default();

                if name == "ALERTS" || name == "ALERTS_FOR_STATE" {
                    // indeterminate
                    is_indeterminate = true;
                    break;
                }

                // only include a metric if it's related to one of our rules
                if let Some(rule_idx) = self.rules.iter().position(|x| x.name() == name) {
                    graph.depends_on(&i, &rule_idx);
                    continue;
                }
            }
            no_dependents.push(i);
        }

        if is_indeterminate {
            return None;
        }

        for i in 0..self.rules.len() {
            // Get the set of nodes that a given node depends on.
            let dependencies = graph.get_forward_dependencies(&i);
            if dependencies.is_empty() {
                no_dependents.push(i);
            }
        }

        let mut result: Vec<Vec<usize>> = Vec::new();
        let deps = graph.get_forward_dependency_topological_layers();
        for (i, dependency) in deps.iter().enumerate() {
            let mut layer = Vec::with_capacity(dependency.len());
            for index in dependency {
                layer.push(*index);
            }
            if i == 0 {
                layer.append(&mut no_dependents);
            }
            result.push(layer);
        }

        Some(result)
    }

    fn update_dependencies(&mut self) {
        self.dependencies = self.build_dependencies();
    }

    pub fn get_last_evaluation(&self) -> Timestamp {
        self.last_evaluation.load(Ordering::Relaxed)
    }

    pub fn set_last_evaluation(&self, ts: Timestamp) {
        self.last_evaluation.store(ts, Ordering::Relaxed);
    }

pub(crate) fn eval(&mut self, e: &Executor, ts: Timestamp) {
    self.metrics.iteration_total.fetch_add(1, Ordering::Relaxed);
    let start = current_time_millis();

    if self.is_empty() {
        self.set_last_evaluation(start);
        return;
    }

    let resolve_duration = self.resolve_duration();
    let ts = self.adjust_req_timestamp(ts);

    if let Err(res) = e.exec(self, ts, resolve_duration, self.limit) {
        let ctx_guard = valkey_module::MODULE_CONTEXT.lock();
        let log_error = |err: &AlertsError| {
            ctx_guard.log_warning(&format!("group {}: failed to execute rules {}", self.name, err))
        };

        match res {
            AlertsError::GroupExecutionError(errs) => errs.iter().for_each(log_error),
            _ => log_error(&res),
        }
    }

    self.set_last_evaluation(start);
}

    pub(crate) fn on_tick(&mut self, e: &Executor, eval_ts: Timestamp) {
        self.metrics
            .iteration_interval
            .fetch_add(1, Ordering::Relaxed);
        
        let elapsed = eval_ts - self.get_last_evaluation();
        let interval_millis = self.interval.as_millis() as i64;
        let missed = (elapsed / interval_millis.max(1) as u64 as i64).max(0);

        if missed > 0 {
            self.metrics
                .iteration_missed
                .fetch_add(missed as u64, Ordering::Relaxed);
        }

        let eval_ts = eval_ts.add((missed + 1) * interval_millis);
        self.eval(e, eval_ts);
    }

    pub(super) fn on_update(&mut self, ng: &Group) -> AlertsResult<()> {
        self.update_with(ng)
            .map_err(|_| AlertsError::Generic(format!("group {}: failed to update", self.name)))?;

        let mut headers = HashMap::new();
        for (key, value) in self.notifier_headers.iter() {
            headers.insert(key.clone(), value.clone());
        }

        info!("group re-started");
        Ok(())
    }

    pub(super) fn resolve_duration(&self) -> Duration {
        get_resolve_duration(
            self.interval,
            &ALERT_SETTINGS.resend_delay,
            &ALERT_SETTINGS.max_resolve_duration,
        )
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
                return ts.saturating_sub(interval);
            }
            // eval_offset shouldn't interfere with eval_alignment, so we return it immediately
            return ts;
        }
        if self.eval_alignment.unwrap_or(true) {
            // align query time with interval to get similar result with grafana when plotting time series.
            // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5049
            // and https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1232
            return timestamp.truncate(self.interval);
        }
        timestamp
    }

    pub fn remove_inactive_alerts(&mut self, ts: Timestamp) -> usize {
        let mut count = 0;
        for rule in self.rules.iter_mut() {
            if let MetricRule::AlertingRule(ar) = rule {
                count += ar.remove_inactive_alerts(ts);
            }
        }
        count
    }
}

fn inspect_query(rule: &impl Rule) -> Option<MetricExpr> {
    match parse_expr(rule.expr()) {
        Ok(expr) => match expr {
            Expr::MetricExpression(me) => Some(me),
            _ => None,
        },
        Err(_) => None, // Handle parsing errors here
    }
}

fn new_group_metrics(_g: &Group) -> GroupMetrics {
    GroupMetrics::default()
}

fn merge_hashes(
    group_name: &str,
    rule_name: &str,
    dest: &mut HashMap<String, String>,
    set2: &HashMap<String, String>,
) {
    for (k, v) in set2.iter() {
        use std::collections::hash_map::Entry;
        match dest.entry(k.clone()) {
            Entry::Occupied(mut entry) => {
                info!(
                    "hash {k} for rules {}.{} overwritten with external hash {k}={v}",
                    group_name, rule_name
                );
                entry.get_mut().clone_from(v);
            }
            Entry::Vacant(entry) => {
                entry.insert(k.clone());
            }
        }
    }
}

/// get_resolve_duration returns the duration after which firing alert can be considered as resolved.
fn get_resolve_duration(
    group_interval: Duration,
    delta: &Duration,
    max_duration: &Duration,
) -> Duration {
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
    let capacity = labels
        .iter()
        .fold(0, |acc, l| acc + l.name.len() + l.value.len() + 2);
    let mut b = String::with_capacity(capacity);
    b.push('{');
    for (i, label) in labels.iter().enumerate() {
        if label.name.is_empty() {
            b.push_str(METRIC_NAME_LABEL);
        } else {
            b.push_str(&label.name)
        }
        b.push('=');
        b.push_str(&enquote('"', &label.value));
        if i < labels.len() - 1 {
            b.push(',')
        }
    }
    b.push('}');
    b
}
