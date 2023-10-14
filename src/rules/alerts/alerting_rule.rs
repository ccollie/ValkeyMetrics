use crate::common::current_time_millis;
use crate::common::hash::hash_labels_without_metric_name;
use crate::common::types::Label;
use crate::rules::alerts::datasource::datasource::Querier;
use crate::rules::alerts::template::QueryFn;
use crate::rules::alerts::{exec_template, Alert, AlertState, AlertTplData, AlertsError, AlertsResult, Group, QueryResult, RuleConfig, ALERT_FOR_STATE_METRIC_NAME, ALERT_GROUP_NAME_LABEL, ALERT_METRIC_NAME, ALERT_NAME_LABEL, ALERT_STATE_LABEL, DatasourceMetric};
use crate::rules::types::{new_time_series, RawTimeSeries};
use crate::rules::{EvalContext, Rule, RuleState, RuleStateEntry, RuleType};
use crate::ts::Timestamp;
use ahash::AHashMap;
use metricsql_engine::METRIC_NAME_LABEL;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ops::{Add, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Duration;

// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vmalert/alerting.go#L612

/// the duration for which a resolved alert instance is kept in memory state and consequently
/// repeatedly sent to the AlertManager.
const RESOLVED_RETENTION: Duration = Duration::from_micros(15 * 60 * 1000);

// todo: move to global config
const DISABLE_ALERT_GROUP_LABEL: bool = false;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertingRuleMetrics {
    errors: AtomicU64,
    pending: AtomicU64,
    active: AtomicU64,
    sample: AtomicU64,
    series_fetched: AtomicU64,
}

/// AlertingRule is basic alert entity
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertingRule {
    pub rule_type: RuleType,
    rule_id: u64,
    pub name: String,
    pub expr: String,
    pub r#for: Duration,
    pub keep_firing_for: Duration,
    pub labels: AHashMap<String, String>,
    pub annotations: AHashMap<String, String>,
    pub group_id: u64,
    pub group_name: String,
    pub eval_interval: Duration,
    pub debug: bool,

    #[serde(skip)]
    querier: Box<dyn Querier>,
    /// stores list of active alerts
    pub alerts: RwLock<HashMap<u64, Alert>>,
    /// state stores recent state changes during evaluations
    pub state: RuleState,

    pub metrics: AlertingRuleMetrics,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
struct LabelSet {
    /// origin labels extracted from received time series plus extra labels (group labels, service
    /// labels like ALERT_NAME_LABEL). In case of conflicts, origin labels from time series preferred.
    /// Used for templating annotations
    origin: AHashMap<String, String>,
    /// processed labels includes origin labels plus extra labels (group labels, service labels
    /// like ALERT_NAME_LABEL). In case of conflicts, extra labels are preferred.
    /// Used as labels attached to notifier.Alert and ALERTS series written to remote storage.
    processed: AHashMap<String, String>,
}

impl AlertingRule {
    pub fn new(rule: &RuleConfig, group: &Group) -> Self {
        let updates_limit = rule.update_entries_limit();
        let mut ar: AlertingRule = Default::default();
        ar.rule_type = rule.rule_type();
        ar.rule_id = rule.hash();
        ar.name = rule.name().to_string();
        ar.expr = rule.expr.to_string();
        ar.group_id = group.id();
        ar.group_name = group.name.clone();
        ar.eval_interval = group.interval.clone();
        ar.r#for = rule.r#for.clone();
        ar.keep_firing_for = rule.keep_firing_for.clone();
        ar.labels = rule.labels.clone();
        ar.annotations = rule.annotations.clone();
        ar.debug = rule.debug;
        ar.alerts = Default::default();
        ar.metrics = AlertingRuleMetrics::default();
        ar.state = RuleState::new(updates_limit);

        ar
    }

    /// update_with copies all significant fields. alerts state isn't copied since
    /// it should be updated in next 2 Execs
    pub fn update_with(&mut self, rule: &AlertingRule) {
        self.expr = rule.expr.clone();
        self.r#for = rule.r#for.clone();
        self.keep_firing_for = rule.keep_firing_for;
        self.labels = rule.labels.clone();
        self.annotations = rule.annotations.clone();
        self.eval_interval = rule.eval_interval.clone();
        self.debug = rule.debug;
        self.state = rule.state.clone()
    }

    /// restores the value of active_at field for active alerts, based on previously written
    /// time series `alertForStateMetricName`.
    /// Only rules with for > 0 can be restored.
    pub fn restore<'a>(
        &mut self,
        ctx: &'a EvalContext,
        ts: Timestamp,
        look_back: Duration,
    ) -> AlertsResult<()> {
        if self.r#for.is_zero() {
            return Ok(());
        }

        let mut alerts = self.alerts.write().unwrap();

        for (k, a) in alerts.iter_mut() {
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

            let mut res = ctx
                .querier
                .query(&expr, ts)
                .map_err(|e| AlertsError::QueryExecutionError(format!("{}: {:?}", expr, e)))?;

            let q_metrics = res.data;
            if res.data.is_empty() {
                ctx.log_debug("no response was received from restore query");
                continue;
            }

            // only one series expected in response
            let mut m = res.data.remove(0);
            // __name__ is supposed to be alertForStateMetricName
            m.labels.retain(|x| x.name != METRIC_NAME_LABEL);

            // we assume that restore query contains all label matchers,
            // so all received labels will match anyway if their number is equal.
            if m.labels.len() != a.labels.len() {
                let msg = format!(
                    "state restore query returned not expected label-set {:?}",
                    m.labels
                );
                ctx.log_debug(&msg);
                continue;
            }
            a.active_at = m.values[0].floor() as Timestamp;
            a.restored = true;
            let msg = format!(
                "alert {} ({}) restored to state at {}",
                a.name, a.id, a.active_at
            );
            ctx.log_info(&msg);
        }
        Ok(())
    }

    fn to_labels(&self, m: &DatasourceMetric, query_fn: QueryFn) -> AlertsResult<LabelSet> {
        let mut ls = LabelSet {
            origin: Default::default(),
            processed: Default::default(),
        };
        for Label { name, value } in m.labels {
            ls.origin.insert(name.clone(), value.clone());
            // drop __name__ to be consistent with Prometheus alerting
            if name == METRIC_NAME_LABEL {
                continue;
            }
            ls.processed.insert(name.clone(), value.clone());
        }

        let mut data = AlertTplData::default();
        data.labels = ls.origin.clone();
        data.value = m.values[0];
        data.expr = self.expr.clone();
        data.r#for = Default::default();

        let extra_labels = exec_template(query_fn, &self.labels, &data)
            .map_err(|e| AlertsError::FailedToExpandLabels(e.to_string()))?;

        for (k, v) in extra_labels {
            ls.processed.insert(k, v);
            ls.origin.insert(k.to_string(), v.to_string());
        }

        // set additional labels to identify group and rule name
        if !self.name.is_empty() {
            ls.origin
                .insert(ALERT_NAME_LABEL.to_string(), self.name.clone());
        }
        if !*DISABLE_ALERT_GROUP_LABEL && !self.group_name.is_empty() {
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

    fn to_time_series(&self, timestamp: Timestamp) -> Vec<RawTimeSeries> {
        let reader = self.alerts.read().unwrap();
        reader
            .iter()
            .filter(|(_hash, a)| a.state != AlertState::Inactive)
            .map(|(_, alert)| self.alert_to_timeseries(alert, timestamp))
            .collect()
    }

    fn alert_to_timeseries(&self, alert: &Alert, timestamp: Timestamp) -> Vec<RawTimeSeries> {
        let mut tss: Vec<RawTimeSeries> = vec![];
        tss.push(alert_to_time_series(alert, timestamp));
        if !self.r#for.is_zero() {
            tss.push(alert_for_to_time_series(alert, timestamp))
        }
        return tss;
    }

    /// alertsToSend walks through the current alerts of AlertingRule
    /// and returns only those which should be sent to notifier.
    /// Isn't concurrent safe.
    pub fn alerts_to_send(
        &self,
        ts: Timestamp,
        resolve_duration: Duration,
        resend_delay: Duration,
    ) -> Vec<&Alert> {
        let needs_sending = |a: &Alert| -> bool {
            if a.state == AlertState::Pending {
                return false;
            }
            if a.resolved_at > a.last_sent {
                return true;
            }
            return a.last_sent + resend_delay < ts;
        };

        let mut alerts = Vec::with_capacity(10); // ?????
        let mut alerts_inner = self.alerts.write().unwrap();

        for (_, alert) in alerts_inner.iter_mut() {
            if !needs_sending(alert) {
                continue;
            }
            alert.end = ts.add(resolve_duration.as_millis() as i64);
            if alert.state == AlertState::Inactive {
                alert.end = alert.resolved_at;
            }
            alert.last_sent = ts;
            alerts.push(&alert)
        }
        return alerts;
    }

    fn new_alert(
        &mut self,
        m: &DatasourceMetric,
        ls: Option<&LabelSet>,
        start: Timestamp,
        q_fn: QueryFn,
    ) -> AlertsResult<Alert> {
        let ls = if ls.is_none() {
            self.to_labels(m, q_fn)
                .map_err(|e| AlertsError::FailedToExpandLabels(e.to_string()))?
        } else {
            ls.unwrap()
        };

        let mut alert = Alert {
            group_id: self.group_id,
            name: self.name.clone(),
            labels: ls.processed.clone(),
            value: m.values[0],
            id: 0,
            restored: false,
            active_at: start,
            start,
            end: 0,
            resolved_at: 0,
            last_sent: 0,
            expr: self.expr.clone(),
            r#for: self.r#for.clone(),
            annotations: Default::default(),
            state: AlertState::Inactive,
            keep_firing_since: 0,
            external_url: "".to_string(),
        };
        alert.annotations = alert.exec_template(q_fn, &ls.origin, &self.annotations)?;
        return Ok(alert);
    }

    fn count_alerts_in_state(&self, state: AlertState) -> usize {
        let alerts = self.alerts.read().unwrap();
        alerts
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
        if let Some(last) = self.state.last() {
            last.samples
        }
        0
    }

    pub fn series_fetched(&self) -> usize {
        if let Some(last) = self.state.last() {
            last.series_fetched
        }
        0
    }
}

fn alert_to_time_series(alert: &Alert, timestamp: Timestamp) -> RawTimeSeries {
    let mut labels = alert.labels.clone();
    labels.insert(METRIC_NAME_LABEL.to_string(), ALERT_METRIC_NAME.to_string());
    labels.insert(ALERT_STATE_LABEL.to_string(), alert.state.to_string());
    let values: [f64; 1] = [1.0];
    let timestamps: [i64; 1] = [timestamp];
    new_time_series(&values, &timestamps, labels)
}

/// returns a series that represents the state of active alerts, where value is the timestamp when
/// the alert became active
fn alert_for_to_time_series(alert: &Alert, timestamp: Timestamp) -> RawTimeSeries {
    let mut labels = alert.labels.clone();
    labels.insert(
        METRIC_NAME_LABEL.to_string(),
        ALERT_FOR_STATE_METRIC_NAME.to_string(),
    );
    let values: [f64; 1] = [alert.active_at as f64];
    let timestamps: [i64; 1] = [timestamp];
    new_time_series(&values, &timestamps, labels)
}

impl Rule for AlertingRule {
    fn id(&self) -> u64 {
        self.rule_id
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Alerting
    }

    fn exec(&mut self, ts: Timestamp, limit: usize) -> AlertsResult<Vec<RawTimeSeries>> {
        let start = current_time_millis();
        let mut res = self.querier.query(&self.expr, ts)?;
        let end = current_time_millis();
        let mut cur_state = RuleStateEntry {
            time: start,
            at: ts,
            duration: Duration::from_millis((end - start) as u64),
            samples: res.data.len(),
            err: None,
            series_fetched: res.series_fetched,
        };

        defer! {
            self.state.add(cur_state)
        }

        if let Err(e) = res {
            self.metrics.errors.fetch_add(1, Ordering::Relaxed);
            cur_state.err = Some(e.clone());
            let msg = format!("failed to execute query {}: {:?}", self.expr, res);
            return Err(e);
        }
        res = res.unwrap();

        // self.logDebugf(ts, nil, "query returned {} samples (elapsed: {})", curState.samples, curState.duration)

        let mut alerts = self.alerts.read().unwrap();
        let mut to_delete = Vec::new();
        for (h, alert) in alerts.iter_mut() {
            // cleanup inactive alerts from previous Exec
            if alert.state == AlertState::Inactive
                && ts.sub(alert.resolved_at) > RESOLVED_RETENTION.as_millis() as i64
            {
                // ar.logDebugf(ts, alert, "deleted as inactive");
                to_delete.push(h);
            }
        }
        for h in to_delete {
            alerts.remove(&h);
        }

        let query_fn = |query: &str| -> AlertsResult<QueryResult> { self.querier.query(query, ts) };

        let mut updated = HashSet::new();
        for m in res.data.into_iter() {
            let labels = self.to_labels(&m, query_fn);
            if labels.is_err() {
                self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                let msg = format!("{:?}", labels.unwrap_err());
                return Err(AlertsError::FailedToExpandLabels(msg));
            }
            let labels = labels.unwrap();
            let h = hash_labels_without_metric_name(&labels.processed);
            updated.insert(h);

            if let Some(alert) = alerts.get_mut(&h) {
                if alert.state == AlertState::Inactive {
                    // alert could be in inactive state for resolvedRetention
                    // so when we again receive metrics for it - we switch it
                    // back to AlertState::Pending
                    alert.state = AlertState::Pending;
                    alert.active_at = ts;
                    // self.logDebugf(ts, alert, "INACTIVE => PENDING")
                }
                alert.value = m.values[0]; //
                                           // re-exec template since value or query can be used in annotations

                alert.annotations =
                    alert.exec_template(query_fn, &labels.origin, &self.annotations)?;
                alert.keep_firing_since = current_time_millis();
                continue;
            }

            let alert = Alert {
                id: h,
                restored: false,
                group_id: self.group_id,
                name: self.name.clone(),
                labels: labels.processed,
                annotations: self.annotations.clone(),
                state: AlertState::Pending,
                expr: self.expr.clone(),
                active_at: ts,
                start: ts,
                end: ts + self.r#for.as_millis() as i64,
                resolved_at: ts,
                last_sent: ts,
                keep_firing_since: ts,
                value: m.values[0],
                r#for: Default::default(),
                external_url: "".to_string(),
            };

            alerts.insert(h, alert);
            // ar.logDebugf(ts, a, "created in state PENDING")
        }
        let mut num_active_pending = 0;

        let mut to_delete = Vec::new();
        for (h, alert) in alerts {
            // if alert wasn't updated in this iteration means it is resolved already
            if updated.contains(h) {
                if alert.State == AlertState::Pending {
                    // alert was in Pending state - it is not active anymore
                    to_delete.push(h);
                    // ar.logDebugf(ts, a, "PENDING => DELETED: is absent in current evaluation round")
                    continue;
                }
                // check if alert should keep Firing if rule has
                // `keep_firing_for` field
                if alert.State == AlertState::Firing {
                    if !self.keep_firing_for.is_zero() {
                        if alert.keep_firing_since.is_zero() {
                            alert.keep_firing_since = ts
                        }
                    }
                    // alerts with ar.keep_firing_for > 0 may remain FIRING
                    // even if their expression isn't true anymore
                    if ts.sub(alert.keep_firing_since) > self.keep_firing_for {
                        alert.state = AlertState::Inactive;
                        alert.resolved_at = ts;
                        // ar.logDebugf(ts, a, "FIRING => INACTIVE: is absent in current evaluation round")
                        continue;
                    }
                    // self.logDebugf(ts, a, "KEEP_FIRING: will keep firing for %fs since %v", ar.KeepFiringFor.Seconds(), a.KeepFiringSince)
                }
            }
            num_active_pending += 1;
            if alert.state == AlertState::Pending && ts.sub(alert.active_at) >= self.r#for {
                alert.state = AlertState::Firing;
                alert.start = ts;
                // alertsFired.Inc()
                // ar.logDebugf(ts, a, "PENDING => FIRING: %s since becoming active at %v", ts.Sub(a.ActiveAt), a.ActiveAt)
            }
        }
        if limit > 0 && num_active_pending > limit {
            alerts.clear();
            let msg = format!("exec exceeded limit of {limit} with {num_active_pending} alerts");
            let err = AlertsError::Generic(msg);
            cur_state.err = Some(err.clone());
            return Err(err);
        }

        self.to_timeseries(ts)
    }

    /// exec_range executes alerting rule on the given time range similarly to exec.
    /// It doesn't update internal states of the Rule and meant to be used just to get time series
    /// for backfilling.
    /// It returns ALERT and ALERT_FOR_STATE time series as a result.
    fn exec_range(&mut self, start: Timestamp, end: Timestamp) -> AlertsResult<Vec<RawTimeSeries>> {
        let res = self.querier.query_range(&self.expr, start, end)?;
        let mut result: Vec<RawTimeSeries> = vec![];
        let q_fn = |query: &str| -> AlertsResult<Vec<DatasourceMetric>> {
            return Err(AlertsError::Generic(format!(
                "`query` template isn't supported in replay mode"
            )));
        };

        for s in res.data.into_iter() {
            let start = current_time_millis();
            let mut alert = self
                .new_alert(&s, None, start, q_fn)
                .map_err(|e| AlertsError::FailedToCreateAlert(e.to_string()))?; // initial alert

            if self.r#for.is_zero() {
                // if alert is instant
                alert.state = AlertState::Firing;
                for ts in s.timestamps.iter() {
                    let vals = self.alert_to_timeseries(&alert, *ts);
                    result.extend(vals);
                }
                continue;
            }

            // if alert with For > 0
            let mut prev_t = current_time_millis();
            for (ts, v) in s.timestamps.iter().zip(s.values.iter()) {
                let at = *ts;
                if at.sub(prev_t) > self.eval_interval.as_millis() as i64 {
                    // reset to Pending if there are gaps > eval_interval between DPs
                    alert.state = AlertState::Pending;
                    alert.active_at = at;
                } else if at.sub(alert.active_at) >= self.r#for.as_millis() as i64 {
                    alert.state = AlertState::Firing;
                    alert.start = at;
                }
                prev_t = at;
                let vals = self.alert_to_timeseries(&alert, *ts);
                result.extend(vals);
            }
        }

        Ok(result)
    }
}
