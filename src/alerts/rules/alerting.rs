use crate::alerts::constants::*;
use crate::alerts::datasource::AlertDatasource;
use crate::alerts::notifications::{exec_template, Alert, AlertState, AlertTplData};
use crate::alerts::rules::rule::fmt_rule;
use crate::alerts::rules::{Group, Rule, RuleConfig, RuleState, RuleStateEntry, RuleType};
use crate::alerts::templates::TemplateQueryContext;
use crate::alerts::types::{hashmap_to_labels, RawTimeSeries};
use crate::alerts::{AlertsError, AlertsResult};
use crate::common::types::{Label, MetricName, Sample, Timestamp, TimestampTrait};
use crate::common::{current_time_millis, METRIC_NAME_LABEL};
use crate::query::Querier;
use ahash::AHasher;
use enquote::enquote;
use get_size::GetSize;
use metricsql_common::hash::FastHasher;
use metricsql_common::prelude::humanize_duration;
use metricsql_parser::ast::Expr;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::ops::Sub;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::debug;
use valkey_module::{Context, ValkeyError, ValkeyResult};
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vmalert/alerting.go#L612

/// the duration for which a resolved alert instance is kept in memory state and consequently
/// repeatedly sent to the AlertManager.
const RESOLVED_RETENTION: Duration = Duration::from_micros(15 * 60 * 1000);

// todo: move to global config
const DISABLE_ALERT_GROUP_LABEL: bool = false;

#[derive(Debug, Default, Serialize, Deserialize, GetSize)]
pub struct AlertingRuleMetrics {
    pub(crate) errors: AtomicU64,
    pub(crate) pending: AtomicU64,
    pub(crate) active: AtomicU64,
    pub(crate) samples: AtomicU64,
    pub(crate) series_fetched: AtomicU64,
}

impl Clone for AlertingRuleMetrics {
    fn clone(&self) -> Self {
        AlertingRuleMetrics {
            errors: AtomicU64::new(self.errors.load(Ordering::Relaxed)),
            pending: AtomicU64::new(self.pending.load(Ordering::Relaxed)),
            active: AtomicU64::new(self.active.load(Ordering::Relaxed)),
            samples: AtomicU64::new(self.samples.load(Ordering::Relaxed)),
            series_fetched: AtomicU64::new(self.series_fetched.load(Ordering::Relaxed)),
        }
    }
}

impl Eq for AlertingRuleMetrics {}
impl PartialEq for AlertingRuleMetrics {
    fn eq(&self, other: &Self) -> bool {
        self.errors.load(Ordering::Relaxed) == other.errors.load(Ordering::Relaxed)
            && self.pending.load(Ordering::Relaxed) == other.pending.load(Ordering::Relaxed)
            && self.active.load(Ordering::Relaxed) == other.active.load(Ordering::Relaxed)
            && self.samples.load(Ordering::Relaxed) == other.samples.load(Ordering::Relaxed)
            && self.series_fetched.load(Ordering::Relaxed)
                == other.series_fetched.load(Ordering::Relaxed)
    }
}

/// `AlertingRule` is basic alert entity
#[derive(Clone, Debug, Default, Serialize, Deserialize, GetSize)]
pub struct AlertingRule {
    pub rule_id: u64,
    pub name: String,
    /// The PromQL expression to evaluate.
    pub expr: String,
    #[serde(rename = "for_duration")]
    pub r#for: Duration,
    pub keep_firing_for: Duration,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub group_id: u64,      // I don't think this needs to be stored
    pub group_name: String, // neither this
    pub eval_interval: Duration,
    pub debug: bool,

    /// state stores recent state changes during evaluations
    pub state: RuleState,

    /// stores list of active alerts
    pub alerts: HashMap<u64, Alert>,

    pub metrics: AlertingRuleMetrics,
}

#[derive(Debug, Default, Clone, PartialEq)]
struct LabelSet {
    /// `origin` labels extracted from received time series plus extra labels (group labels, service
    /// labels like `ALERT_NAME_LABEL`). In case of conflicts, origin labels from time series preferred.
    /// Used for templating annotations
    origin: HashMap<String, String>,
    /// `processed` labels includes origin labels plus extra labels (group labels, service labels
    /// like `ALERT_NAME_LABEL`). In case of conflicts, extra labels are preferred.
    /// Used as labels attached to notifications.Alert and ALERTS series written to remote storage.
    processed: HashMap<String, String>,
}

impl Display for AlertingRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt_rule(self, f)
    }
}

impl AlertingRule {
    pub fn new(group: &Group, rule: RuleConfig) -> Self {
        let updates_limit = rule.update_entries_limit();
        AlertingRule {
            rule_id: rule.hash(),
            name: rule.name().to_string(),
            expr: rule.expr,
            r#for: rule.r#for,
            keep_firing_for: rule.keep_firing_for,
            labels: rule.labels,
            annotations: rule.annotations,
            group_id: group.id(),
            group_name: group.name.clone(),
            eval_interval: group.interval,
            debug: rule.debug,
            state: RuleState::with_capacity(updates_limit),
            metrics: AlertingRuleMetrics::default(),
            ..Default::default()
        }
    }

    /// restores the value of active_at field for active alerts, based on previously written
    /// time series `alertForStateMetricName`.
    /// Only rules with for > 0 can be restored.
    pub fn restore(
        &mut self,
        ctx: &Context,
        querier: &Box<dyn Querier>,
        ts: Timestamp,
        look_back: Duration,
    ) -> AlertsResult<()> {
        if self.r#for.is_zero() {
            return Ok(());
        }

        for (_k, a) in self.alerts.iter_mut() {
            if a.restored || a.state != AlertState::Pending {
                continue;
            }

            let mut labels_filter: Vec<String> = Vec::with_capacity(a.labels.len());
            for (k, v) in a.labels.iter() {
                labels_filter.push(format!("{k}={v}"));
            }
            labels_filter.sort();
            let expr = format!(
                "last_over_time({}{{ {} }}[{}])",
                ALERT_FOR_STATE_METRIC_NAME,
                labels_filter.join(","),
                look_back.as_secs()
            );

            ctx.log_debug("restoring alert state via query {expr}");

            let mut res = querier
                .query(&expr, ts)
                .map_err(|e| AlertsError::QueryExecutionError(format!("{}: {:?}", expr, e)))?;

            if res.is_empty() {
                ctx.log_debug("no response was received from restore query");
                continue;
            }

            // only one series expected in response
            let mut m = res.remove(0);
            // __name__ is supposed to be alertForStateMetricName
            m.metric.labels.retain(|x| x.name != METRIC_NAME_LABEL);

            // we assume that restore query contains all label matchers,
            // so all received labels will match anyway if their number is equal.
            if m.metric.labels.len() != a.labels.len() {
                let msg = format!(
                    "state restore query returned not expected label-set {:?}",
                    m.metric.labels
                );
                ctx.log_debug(&msg);
                continue;
            }
            a.active_at = m.sample.value.floor() as Timestamp;
            a.restored = true;
            let msg = format!(
                "alert {} ({}) restored to state at {}",
                a.name, a.id, a.active_at
            );
            ctx.log_notice(&msg);
        }
        Ok(())
    }

    fn to_labels(
        &self,
        metric: &MetricName,
        value: f64,
        ctx: TemplateQueryContext,
    ) -> AlertsResult<LabelSet> {
        let mut ls = LabelSet {
            origin: Default::default(),
            processed: Default::default(),
        };

        if !metric.measurement.is_empty() {
            ls.origin
                .insert(METRIC_NAME_LABEL.to_string(), metric.measurement.clone());
        }

        for Label { name, value } in metric.labels.iter() {
            ls.origin.insert(name.clone(), value.clone());
            // drop __name__ to be consistent with Prometheus alerting
            if name == METRIC_NAME_LABEL {
                continue;
            }
            ls.processed.insert(name.clone(), value.clone());
        }

        let mut data = AlertTplData::default();
        data.labels.clone_from(&ls.origin);
        data.value = value;
        data.expr.clone_from(&self.expr);
        data.r#for = Default::default();

        let extra_labels = exec_template(ctx, &self.labels, data)
            .map_err(|e| AlertsError::FailedToExpandLabels(e.to_string()))?;

        for (k, v) in extra_labels {
            ls.origin.insert(k.to_string(), v.to_string());
            ls.processed.insert(k, v);
        }

        // set additional labels to identify group and rules name
        if !self.name.is_empty() {
            ls.origin
                .insert(ALERT_NAME_LABEL.to_string(), self.name.clone());
        }
        if !DISABLE_ALERT_GROUP_LABEL && !self.group_name.is_empty() {
            ls.processed.insert(
                ALERT_GROUP_NAME_LABEL.to_string(),
                self.group_name.to_string(),
            );
            ls.origin.insert(
                ALERT_GROUP_NAME_LABEL.to_string(),
                self.group_name.to_string(),
            );
        }

        Ok(ls)
    }

    fn expand_templates(
        &self,
        metric: &MetricName,
        ts: Timestamp,
        value: f64,
        ctx: TemplateQueryContext,
    ) -> AlertsResult<(LabelSet, HashMap<String, String>)> {
        let ls = self.to_labels(metric, value, ctx.clone())?;
        let tpl_data = AlertTplData {
            value,
            labels: ls.origin.clone(),
            expr: self.expr.clone(),
            alert_id: hash_map(&ls.processed),
            group_id: self.group_id,
            active_at: ts,
            r#for: self.r#for.into(),
        };
        let res = exec_template(ctx, &self.annotations, tpl_data)?;
        Ok((ls, res))
    }

    fn to_time_series(&self, timestamp: Timestamp) -> Vec<RawTimeSeries> {
        self.alerts
            .iter()
            .filter(|(_hash, a)| a.state != AlertState::Inactive)
            .flat_map(|(_, alert)| self.alert_to_timeseries(alert, timestamp))
            .collect()
    }

    fn alert_to_timeseries(&self, alert: &Alert, timestamp: Timestamp) -> Vec<RawTimeSeries> {
        let mut tss: Vec<RawTimeSeries> = vec![];
        tss.push(alert_to_time_series(alert, timestamp));
        if !self.r#for.is_zero() {
            tss.push(alert_for_to_time_series(alert, timestamp))
        }
        tss
    }

    /// walks through the current alerts of AlertingRule and returns only those which should be sent
    /// to notifications.
    pub fn process_alerts_to_send<F>(
        &mut self,
        ts: Timestamp,
        resolve_duration: Duration,
        resend_delay: Duration,
        f: F,
    ) -> AlertsResult<()>
    where
        F: Fn(Vec<&Alert>) -> AlertsResult<()>,
    {
        let delay = resend_delay.as_millis() as i64;

        let mut ids: Vec<u64> = Vec::new();

        let resolve_duration = resolve_duration.as_millis() as i64;

        for (_, alert) in self.alerts.iter_mut() {
            if !alert.needs_sending(ts, delay) {
                continue;
            }

            alert.end = if alert.state == AlertState::Inactive {
                alert.resolved_at
            } else {
                ts.saturating_add(resolve_duration)
            };

            alert.last_sent = ts;
            ids.push(alert.id);
        }

        let to_send = ids
            .iter()
            .filter_map(|id| self.alerts.get(id))
            .collect::<Vec<_>>();
        f(to_send)
    }

    fn new_alert(
        &mut self,
        start: Timestamp,
        value: f64,
        labels: HashMap<String, String>,
        annotations: HashMap<String, String>,
    ) -> Alert {
        Alert {
            group_id: self.group_id,
            name: self.name.clone(),
            labels,
            value,
            active_at: start,
            start,
            expr: self.expr.clone(),
            r#for: self.r#for,
            annotations,
            state: AlertState::Inactive,
            ..Default::default()
        }
    }

    fn count_alerts_in_state(&self, state: AlertState) -> usize {
        self.alerts
            .iter()
            .filter(|(_, alert)| alert.state == state)
            .count()
    }

    pub fn count_active_alerts(&self) -> usize {
        self.count_alerts_in_state(AlertState::Firing)
    }

    pub fn count_pending_alerts(&self) -> usize {
        self.count_alerts_in_state(AlertState::Pending)
    }

    pub fn samples(&self) -> usize {
        if let Some(last) = self.state.iter().last() {
            return last.samples;
        }
        0
    }

    pub fn series_fetched(&self) -> usize {
        if let Some(last) = self.state.iter().last() {
            return last.series_fetched.unwrap_or(0);
        }
        0usize
    }

    pub fn remove_inactive_alerts(&mut self, ts: Timestamp) -> usize {
        let to_delete: Vec<u64> = self
            .alerts
            .iter()
            .filter_map(|(h, alert)| {
                if alert.state == AlertState::Inactive
                    && ts.sub(alert.resolved_at) > RESOLVED_RETENTION.as_millis() as i64
                {
                    self.log_debug(ts, Some(alert), "deleted as inactive");
                    debug!("deleted as inactive");
                    Some(*h)
                } else {
                    None
                }
            })
            .collect();

        let count = to_delete.len();
        for h in to_delete {
            self.alerts.remove(&h);
        }

        count
    }

    fn log_debug(&self, at: Timestamp, alert: Option<&Alert>, message: &str) {
        if !self.debug {
            return;
        }
        let mut prefix = format!(
            "DEBUG rules {}:{} ({}) at {}: ",
            self.group_name,
            self.name,
            self.rule_id,
            at.to_rfc3339()
        );

        if let Some(alert) = alert {
            let mut label_keys = self.labels.keys().collect::<Vec<_>>();
            label_keys.sort();

            let labels = label_keys
                .iter()
                .map(|x| {
                    let label_value = if let Some(value) = alert.labels.get(*x) {
                        value.as_str()
                    } else {
                        ""
                    };
                    format!("{}={}", x, enquote('"', label_value))
                })
                .collect::<Vec<_>>()
                .join(",");

            let alert_msg = format!("alert {} {} ", alert.id, labels);
            prefix.push_str(&alert_msg);
        }

        prefix.push_str(message);

        // todo: use redis ctx.log_debug
        debug!("{}", prefix);
    }
}

fn alert_to_time_series(alert: &Alert, timestamp: Timestamp) -> RawTimeSeries {
    let mut labels = hashmap_to_labels(alert.labels.iter());
    labels.push(Label {
        name: ALERT_STATE_LABEL.to_string(),
        value: alert.state.to_string(),
    });
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: ALERT_METRIC_NAME.to_string(),
    });
    labels.sort();

    let key = make_series_key(&labels);
    RawTimeSeries {
        key,
        samples: vec![Sample {
            timestamp,
            value: 1.0,
        }],
        labels,
    }
}

/// returns a series that represents the state of active alerts, where value is the timestamp when
/// the alert became active
fn alert_for_to_time_series(alert: &Alert, timestamp: Timestamp) -> RawTimeSeries {
    let mut labels = hashmap_to_labels(alert.labels.iter());
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: ALERT_FOR_STATE_METRIC_NAME.to_string(),
    });
    labels.sort();

    let value = alert.active_at as f64;
    let key = make_series_key(&labels);
    RawTimeSeries {
        key,
        samples: vec![Sample { timestamp, value }],
        labels,
    }
}

impl Rule for AlertingRule {
    fn id(&self) -> u64 {
        self.rule_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Alerting
    }

    fn expr(&self) -> &str {
        &self.expr
    }

    fn exec(
        &mut self,
        querier: &AlertDatasource,
        ts: Timestamp,
        limit: usize,
    ) -> AlertsResult<Vec<RawTimeSeries>> {
        let start = current_time_millis();

        let mut cur_state = RuleStateEntry {
            time: start,
            at: ts,
            duration: Duration::from_millis(0),
            samples: 0,
            err: None,
            series_fetched: None,
        };

        let res = match querier.query(&self.expr, ts) {
            Ok(res) => res,
            Err(e) => {
                self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                cur_state.err = Some(e.clone());
                cur_state.duration = Duration::from_millis((current_time_millis() - start) as u64);
                let msg = format!("failed to execute query {}: {:?}", self.expr, e);
                self.state.push(cur_state);
                return Err(AlertsError::QueryExecutionError(msg));
            }
        };

        let end = current_time_millis();
        cur_state.duration = Duration::from_millis((end - start) as u64);

        if self.debug {
            let msg = format!(
                "query returned {} samples (elapsed: {})",
                cur_state.samples,
                humanize_duration(&cur_state.duration)
            );

            self.log_debug(ts, None, &msg)
        }

        let query_ctx = TemplateQueryContext::Query(*querier, ts);

        // template labels and annotations before updating alerts,
        // since they could use `query` function which takes a while to execute,
        // see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6079.
        let mut expanded_labels: Vec<LabelSet> = Vec::with_capacity(res.len());
        let mut expanded_annotations: Vec<HashMap<String, String>> = Vec::with_capacity(res.len());
        for m in res.iter() {
            let value = m.sample.value;
            match self.expand_templates(&m.metric, ts, value, query_ctx.clone()) {
                Ok((ls, annotations)) => {
                    expanded_labels.push(ls);
                    expanded_annotations.push(annotations);
                }
                Err(err) => {
                    self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                    let msg = format!("{:?}", err);
                    cur_state.err = Some(AlertsError::TemplateExpansionError(msg.clone()));
                    self.state.add(cur_state);
                    return Err(AlertsError::TemplateExpansionError(msg));
                }
            }
        }

        // delete expired alerts
        self.remove_inactive_alerts(ts);

        let for_duration = self.r#for.as_millis() as i64;

        // HACK. Avoid borrow checker error in log_debug
        let mut alerts = std::mem::take(&mut self.alerts);

        // update list of active alerts
        let mut updated = HashSet::new();
        for ((m, labels), annotations) in res
            .into_iter()
            .zip(expanded_labels.into_iter())
            .zip(expanded_annotations.into_iter())
        {
            let h = hash_map(&labels.processed);
            let value = m.sample.value;
            updated.insert(h);

            if let Some(alert) = alerts.get_mut(&h) {
                if alert.state == AlertState::Inactive {
                    // alert could be in inactive state for resolvedRetention
                    // so when we again receive metrics for it - we switch it
                    // back to AlertState::Pending
                    alert.state = AlertState::Pending;
                    alert.active_at = ts;
                    self.log_debug(ts, Some(alert), "INACTIVE => PENDING")
                }
                alert.value = value; //

                alert.annotations = annotations;
                alert.keep_firing_since = current_time_millis();
                continue;
            }

            let ts = current_time_millis();

            let mut alert = self.new_alert(ts, value, labels.processed, annotations);
            alert.id = h;
            alert.state = AlertState::Pending;
            self.log_debug(ts, Some(&alert), "created in state PENDING");
            self.alerts.insert(alert.id, alert);
        }

        let mut num_active_pending = 0;

        let mut to_delete = Vec::new();
        let keep_firing_for = self.keep_firing_for.as_millis() as i64;

        for (h, alert) in alerts.iter_mut() {
            // if alert wasn't updated in this iteration it means it is resolved already
            if !updated.contains(h) {
                if alert.state == AlertState::Pending {
                    // alert was in Pending state - it is not active anymore
                    to_delete.push(h);
                    self.log_debug(
                        ts,
                        Some(alert),
                        "PENDING => DELETED: is absent in current evaluation round",
                    );
                    continue;
                }
                // check if alert should keep Firing if rules has `keep_firing_for` field
                if alert.state == AlertState::Firing {
                    if !self.keep_firing_for.is_zero() && alert.keep_firing_since == 0 {
                        alert.keep_firing_since = ts
                    }
                    // alerts with ar.keep_firing_for > 0 may remain FIRING
                    // even if their expression isn't true anymore
                    if ts.sub(alert.keep_firing_since) > keep_firing_for {
                        alert.state = AlertState::Inactive;
                        alert.resolved_at = ts;
                        self.log_debug(
                            ts,
                            Some(alert),
                            "FIRING => INACTIVE: is absent in current evaluation round",
                        );
                        continue;
                    }
                    if self.debug {
                        let msg = format!(
                            "KEEP_FIRING: will keep firing for {}s since {}",
                            self.keep_firing_for.as_secs(),
                            alert.keep_firing_since
                        );
                        self.log_debug(ts, Some(alert), &msg);
                    }
                }
            }

            num_active_pending += 1;
            if alert.state == AlertState::Pending && ts.sub(alert.active_at) >= for_duration {
                alert.state = AlertState::Firing;
                alert.start = ts;
                // alertsFired.Inc()
                if self.debug {
                    let msg = format!(
                        "PENDING => FIRING: {}ms since becoming active at {}",
                        ts.sub(alert.active_at),
                        alert.active_at
                    );
                    self.log_debug(ts, Some(alert), &msg);
                }
            }
        }

        self.alerts = alerts;

        if limit > 0 && num_active_pending > limit {
            self.alerts.clear();
            let msg = format!("exec exceeded limit of {limit} with {num_active_pending} alerts");
            let err = AlertsError::Generic(msg);
            cur_state.err = Some(err.clone());
            self.state.add(cur_state);
            return Err(err);
        }

        self.state.add(cur_state);
        Ok(self.to_time_series(ts))
    }

    /// `exec_range` executes alerting rules on the given time range similarly to exec.
    /// It doesn't update internal states of the Rule and is meant to be used just to get time series
    /// for back-filling.
    /// It returns `ALERT` and `ALERT_FOR_STATE` time series as a result.
    fn exec_range(
        &mut self,
        querier: &AlertDatasource,
        start: Timestamp,
        end: Timestamp,
    ) -> AlertsResult<Vec<RawTimeSeries>> {
        let res = querier.query_range(&self.expr, start, end)?;
        let mut result = Vec::new();
        let mut hold_alert_state = HashMap::new();

        let query_ctx = TemplateQueryContext::Error(
            "`query` template function isn't supported in replay mode".to_string(),
        );

        let eval_interval = self.eval_interval.as_millis() as i64;
        let for_duration = self.r#for.as_millis() as i64;

        let ts = current_time_millis();
        for series in res.data {
            let value = series.samples[0].value;

            let (ls, annotations) = self
                .expand_templates(&series.metric, ts, value, query_ctx.clone())
                .map_err(|_| {
                    let arg = format!("{}", &series);
                    AlertsError::FailedToExpandLabels(arg)
                })?;

            let alert_id = hash_map(&ls.processed);
            let mut alert = self.new_alert(ts, value, ls.processed, annotations);

            let mut prev_t: Timestamp = current_time_millis();
            for sample in series.samples.iter() {
                let at = sample.timestamp;
                if at == start {
                    if let Some(a) = self.alerts.get(&alert_id) {
                        alert = a.clone();
                        prev_t = at;
                    }
                }
                if at.sub(prev_t) > eval_interval {
                    // reset to Pending if there are gaps > eval_interval between DPs
                    alert.state = AlertState::Pending;
                    alert.active_at = at;
                    // re-template the annotations as active timestamp is changed
                    let (_ls, annotations) =
                        self.expand_templates(&series.metric, at, sample.value, query_ctx.clone())?;
                    alert.annotations = annotations;
                    alert.start = 0;
                } else if at.sub(alert.active_at) >= for_duration
                    && alert.state != AlertState::Firing
                {
                    alert.state = AlertState::Firing;
                    alert.start = at;
                }
                prev_t = at;

                if for_duration == 0 {
                    // rules with `for: 0` are always firing when they have Value
                    alert.state = AlertState::Firing;
                }

                result.extend(self.alert_to_timeseries(&alert, sample.timestamp));

                if at == end {
                    hold_alert_state.insert(alert_id, alert.clone());
                }
            }
        }

        self.alerts = hold_alert_state;
        Ok(result)
    }

    /// update_with copies all significant fields. alerts state isn't copied since
    /// it should be updated in next 2 Execs
    fn update_with(&mut self, other: &dyn Rule) -> AlertsResult<()> {
        if self.rule_type() != other.rule_type() {
            let msg = format!(
                "BUG: attempt to update alerting rules with wrong type {}",
                other.rule_type()
            );
            return Err(AlertsError::Generic(msg)); // todo: better error
        }

        let rule = other.as_any().downcast_ref::<AlertingRule>().unwrap();

        self.expr.clone_from(&rule.expr);
        self.r#for = rule.r#for;
        self.keep_firing_for = rule.keep_firing_for;
        self.labels.clone_from(&rule.labels);
        self.annotations.clone_from(&rule.annotations);
        self.eval_interval = rule.eval_interval;
        self.debug = rule.debug;
        self.state = rule.state.clone();

        Ok(())
    }

    fn get_last_entry(&self) -> Option<&RuleStateEntry> {
        self.state.get_last()
    }

    fn get_rule_state_count(&self) -> usize {
        self.state.len()
    }

    fn get_all_entries(&self) -> Vec<RuleStateEntry> {
        self.state.get_all()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn hash_map(labels: &HashMap<String, String>) -> u64 {
    let mut hasher = AHasher::default();

    let mut labels = labels
        .iter()
        // drop __name__ to be consistent with Prometheus alerting
        .filter(|(k, _v)| *k != METRIC_NAME_LABEL)
        .collect::<Vec<_>>();
    labels.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    for (label, value) in labels.iter() {
        hasher.write(label.as_bytes());
        hasher.write_u8(0xff);
        hasher.write(value.as_bytes());
    }
    hasher.finish()
}

// Generate a unique key for a series based on its labels. Assumes that labels are sorted,
pub(crate) fn make_series_key(labels: &[Label]) -> String {
    let mut hasher = FastHasher::default();
    let mut measurement: String = "".to_string();
    for Label { name, value } in labels {
        if name == METRIC_NAME_LABEL {
            measurement.push('{');
            measurement.push_str(value);
            measurement.push_str("}:");
            value.hash(&mut hasher);
        } else {
            name.hash(&mut hasher);
            hasher.write_u8(0xfe);
            value.hash(&mut hasher);
        }
    }
    format!("{KEY_PREFIX}:{measurement}{:x}", hasher.finish())
}
// maybe x-vm:{alert_for_name}::name=joe::foo=bar::bar=baz

pub(crate) fn validate_alert_expr(expr: &str) -> ValkeyResult<()> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Err(ValkeyError::Str("ERR missing expression"));
    }
    match metricsql_parser::parser::parse(expr) {
        Ok(expr) => {
            // ensure we have a comparison
            if let Expr::BinaryOperator(binop) = &expr {
                if !binop.op.is_comparison() {
                    return Err(ValkeyError::Str("ERR expected comparison operator"));
                }
            }
            Ok(())
        }
        Err(err) => Err(ValkeyError::String(format!(
            "ERR invalid expression: {:?}",
            err
        ))),
    }
}
