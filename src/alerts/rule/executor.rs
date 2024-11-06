use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::alerts::constants::STALE_NAN;
use crate::alerts::notifier::{AlertNotifier, Notifier};
use crate::alerts::rule::group::labels_to_string;
use crate::alerts::rule::{make_series_key, AlertingRule, Group, MetricRule, Rule, RuleType};
use crate::alerts::types::RawTimeSeries;
use crate::alerts::{AlertDatasource, AlertsError, AlertsResult, WriteQueue};
use crate::common::types::{Label, Sample, Timestamp, TimestampTrait};
use crate::config::get_global_settings;
use ahash::AHashMap;
use rayon::iter::{IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator};
use valkey_module::ThreadSafeContext;

pub type PreviouslySentSeries = HashMap<u64, AHashMap<String, Vec<Label>>>;

#[derive(Clone, Default)]
pub struct Executor {
    pub rw: Arc<WriteQueue>,
    pub querier: AlertDatasource,
    pub notifiers: Arc<Vec<AlertNotifier>>,

    /// `previously_sent_series` stores series sent to the write queue on previous iteration
    /// HashMap<RuleID, HashMap<ruleLabels, Vec<Label>>
    /// where `ruleID` is id of the Rule within a Group and `ruleLabels` is Vec<Label> marshalled
    /// to a string
    previously_sent_series: Arc<Mutex<PreviouslySentSeries>>,
    pub last_evaluation: Timestamp,
}

/// SKIP_RAND_SLEEP_ON_GROUP_START will skip random sleep delay in group first evaluation
static mut SKIP_RAND_SLEEP_ON_GROUP_START: bool = false;

impl Executor {
    pub fn new(
        rw: Arc<WriteQueue>,
        querier: AlertDatasource,
    ) -> Self {
        Executor {
            rw,
            querier,
            previously_sent_series: Arc::new(Mutex::new(HashMap::new())),
            last_evaluation: Timestamp::now(),
            notifiers: Arc::new(Vec::new()),
        }
    }

    /// `get_stale_series` checks whether there are stale series from previously sent ones.
    fn get_stale_series(&self, rule: &mut impl Rule, tss: &[RawTimeSeries], timestamp: Timestamp) -> Vec<RawTimeSeries> {
        let mut rule_labels: AHashMap<String, Vec<Label>> = AHashMap::with_capacity(tss.len());
        for ts in tss.iter() {
            // convert labels to strings, so we can compare with previously sent series
            let key = labels_to_string(&ts.labels);
            rule_labels.insert(key, ts.labels.clone());
        }

        let rid = rule.id();
        let mut stales: Vec<RawTimeSeries> = Vec::with_capacity(tss.len());
        // check whether there are series which disappeared and need to be marked as stale
        let mut map = self.previously_sent_series.lock().unwrap();

        if let Some(entry) = map.get_mut(&rid) {
            for (key, labels) in entry.iter_mut() {
                if rule_labels.contains_key(key) {
                    continue;
                }
                // todo: is this correct ?
                let key = make_series_key(labels);
                // previously sent series are missing in current series, so we mark them as stale
                let ss = RawTimeSeries {
                    key,
                    labels: labels.clone(),
                    samples: vec![Sample { timestamp, value: *STALE_NAN }],
                };
                stales.push(ss)
            }
        }

        // set previous series to current
        map.insert(rid, rule_labels);

        stales
    }

    /// Deletes references in tracked previously_sent_series_to_rw list to rules
    /// which aren't present in the given active_rules list. The method is used when the list
    /// of loaded rules has changed and executor has to remove references to non-existing rules.
    pub(super) fn purge_stale_series(&mut self, active_rules: &[impl Rule]) {
        let mut map = self.previously_sent_series.lock().unwrap();
        let mut new_series = PreviouslySentSeries::new();
        for rule in active_rules.iter() {
            let id = rule.id();
            if let Some(prev) = map.get_mut(&id) {
                // keep previous series for staleness detection
                new_series.insert(id, prev.clone());
            }
        }
        *map = new_series;
    }

    pub(super) fn exec_rules(&self,
                             group: &mut Group,
                             ts: Timestamp,
                             resolve_duration: Duration,
                             limit: usize) -> AlertsResult<()> {

        if group.dependencies.is_some() {
            return self.exec_dag(group, ts, resolve_duration, limit);
        }

        self.exec_recording_rules(group, ts, limit)?;
        self.exec_alerting_rules(group, ts, resolve_duration, limit)?;

        Ok(())
    }
    
    fn exec_dag(&self, group: &mut Group, ts: Timestamp, resolve_duration: Duration, limit: usize) -> AlertsResult<()> {
        // Ugly Hack to avoid borrow checker issues to allow parallelism
        let mut rules = std::mem::take(&mut group.rules);
        let dag = std::mem::take(&mut group.dependencies).unwrap();
        
        for dependencies in dag.iter() {
            let results: Vec<AlertsResult<()>> = dependencies
                .par_iter()
                .map(|rule_id| {
                    let rule_option = rules.iter_mut().find(|x| x.id() == *rule_id);
                    match rule_option {
                        Some(rule) => self.exec_rule(group, rule, ts, resolve_duration, limit),
                        None => {
                            // todo: should not happen, but log it
                            Ok(())
                        }
                    }
                })
                .collect();

            // Handle errors if needed
            for result in results {
                result?;
            }
        }
        
        group.dependencies = Some(dag);
        group.rules = rules;
        
        Ok(())
    }

    pub(super) fn exec_alerting_rules(&self,
                                      group: &mut Group,
                                      ts: Timestamp,
                                      resolve_duration: Duration,
                                      limit: usize) -> AlertsResult<()> {
        group.rules
            .par_iter_mut()
            .try_for_each(|rule| {
                if rule.rule_type() == RuleType::Alerting {
                    self.exec_rule(group, rule, ts, resolve_duration, limit)
                } else {
                    // todo: log it out
                    Ok(())
                }
            })
    }

    pub fn exec_recording_rules(&self, group: &mut Group, ts: Timestamp, limit: usize) -> AlertsResult<()> {
        group
            .rules
            .par_iter_mut()
            .try_for_each(|rule| {
                if rule.rule_type() == RuleType::Recording {
                    self.exec_internal(rule, ts, limit)
                } else {
                    // todo: log it out
                    Ok(())
                }
            })
    }

    fn exec_internal(&self, rule: &mut MetricRule, ts: Timestamp, limit: usize) -> AlertsResult<()> {
        let tss = rule.exec(&self.querier, ts, limit)
            .map_err(|err| {
                // todo: log it out
                AlertsError::QueryExecutionError(format!("rule {:?}: failed to execute: {:?}", rule, err))
            })?;

        let stale_series = self.get_stale_series(rule, &tss, ts);

        self.push_to_rw(tss);
        self.push_to_rw(stale_series);
        Ok(())
    }

    fn exec_rule(&self,
               group: &Group,
               rule: &mut MetricRule,
               ts: Timestamp, 
               resolve_duration: Duration,
               limit: usize) -> AlertsResult<()> {
        let res = self.exec_internal(rule, ts, limit);
        if res.is_ok() {
            match rule { 
                MetricRule::AlertingRule(alerting_rule) => {
                    let settings = get_global_settings();
                    return self.send_notifications(group, alerting_rule, ts, resolve_duration, settings.resend_delay)
                },
                _ => {}, // not applicable for recording rules
            }
        }
        
        res
    }

    fn push_to_rw(&self, tss: Vec<RawTimeSeries>) {
        tss.into_iter().for_each(|ts| self.rw.add(ts));
    }

    fn send_notifications(&self,
                          group: &Group,
                          rule: &mut AlertingRule,
                          ts: Timestamp,
                          resolve_duration: Duration,
                          resend_delay: Duration) -> AlertsResult<()> {
        rule.process_alerts_to_send(ts, resolve_duration, resend_delay, |alerts| {
            let thread_ctx = ThreadSafeContext::new();
            let context_guard = thread_ctx.lock();
            let alert_slice = alerts.as_slice();
            for nt in self.notifiers.iter() {
                if let Err(err) = nt.send(&context_guard, alert_slice, &group.notifier_headers) {
                    let msg = format!("failed to send alerts to addr {}: {:?}", nt.addr(), err);
                    return Err(AlertsError::Generic(msg));
                }
            }
            Ok(())
        })
    }
}
