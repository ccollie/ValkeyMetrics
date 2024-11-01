use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::alerts::constants::STALE_NAN;
use crate::alerts::notifier::Notifier;
use crate::alerts::rule::group::labels_to_string;
use crate::alerts::rule::{make_series_key, AlertingRule, Group, RecordingRule, Rule};
use crate::alerts::types::RawTimeSeries;
use crate::alerts::{AlertDatasource, AlertsError, AlertsResult, WriteQueue};
use crate::common::types::{Label, Sample, Timestamp, TimestampTrait};
use crate::config::get_global_settings;
use ahash::{AHashMap, AHashSet};
use rayon::iter::{IntoParallelRefMutIterator, ParallelIterator};
use valkey_module::ThreadSafeContext;

pub type PreviouslySentSeries = HashMap<u64, AHashMap<String, Vec<Label>>>;

#[derive(Clone, Default)]
pub struct Executor {
    pub rw: Arc<WriteQueue>,
    pub querier: AlertDatasource,

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
        let id_hash_set: AHashSet<u64> = active_rules.iter().map(|r| r.id()).collect();

        let mut map = self.previously_sent_series.lock().unwrap();

        map.retain(|id, _| id_hash_set.contains(id));
    }

    pub(super) fn exec_concurrently(&self,
                                    group: &mut Group,
                                    ts: Timestamp,
                                    resolve_duration: Duration,
                                    limit: usize) -> AlertsResult<()> {
        // todo: rayon::join!
        self.exec_recording_rules(group, ts, limit)?;
        self.exec_alerting_rules(group, ts, resolve_duration, limit)
    }

    pub(super) fn exec_alerting_rules(&self,
                                      group: &mut Group,
                                      ts: Timestamp,
                                      resolve_duration: Duration,
                                      limit: usize) -> AlertsResult<()> {
        group.alerting_rules
            .par_iter_mut()
            .try_for_each(|rule| self.exec_alerting_rule(group, rule, ts, limit, resolve_duration))
    }

    pub fn exec_recording_rules(&self, group: &mut Group, ts: Timestamp, limit: usize) -> AlertsResult<()> {
        group
            .recording_rules
            .par_iter_mut()
            .try_for_each(|rule| self.exec_recording_rule(rule, ts, limit))
    }

    fn exec_internal(&self, rule: &mut impl Rule, ts: Timestamp, limit: usize) -> AlertsResult<()> {
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

    fn exec_recording_rule(&self, rule: &mut RecordingRule, ts: Timestamp, limit: usize) -> AlertsResult<()> {
        self.exec_internal(rule, ts, limit)
    }

    fn exec_alerting_rule(&self,
                          group: &Group,
                          rule: &mut AlertingRule,
                          ts: Timestamp,
                          limit: usize,
                          resolve_duration: Duration) -> AlertsResult<()> {
        self.exec_internal(rule, ts, limit)?;
        let settings = get_global_settings();
        self.send_notifications(group, rule, ts, resolve_duration, settings.resend_delay)
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
            let alerts_slice = alerts.iter().map(|x| &**x).collect();
            for nt in group.notifiers.iter() {
                if let Err(err) = nt.send(&context_guard, alerts_slice, &group.notifier_headers) {
                    let msg = format!("failed to send alerts to addr {}: {:?}", nt.addr(), err);
                    return Err(AlertsError::Generic(msg));
                }
            }
            Ok(())
        })
    }
}
