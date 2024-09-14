use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::common::decimal::STALE_NAN_BITS;
use crate::common::types::Timestamp;
use crate::config::get_global_settings;
use crate::rules::alerts::group::labels_to_string;
use crate::rules::alerts::{AlertingRule, AlertsError, AlertsResult, Notifier, Querier, WriteQueue};
use crate::rules::{RawTimeSeries, Rule, RuleType};
use crate::storage::{Label, SeriesData};
use ahash::{AHashMap, AHashSet};
use metricsql_runtime::types::TimestampTrait;
use valkey_module::Context;

pub type PreviouslySentSeries = HashMap<u64, AHashMap<String, Vec<Label>>>;

pub struct Executor {
    eval_ts: Timestamp,
    pub notifiers: Arc<Vec<Box<dyn Notifier>>>,
    pub notifier_headers: Arc<HashMap<String, String>>,
    pub rw: Arc<WriteQueue>,
    pub querier: Arc<dyn Querier>,

    /// `previously_sent_series` stores series sent to RW on previous iteration
    /// HashMap<RuleID, HashMap<ruleLabels, Vec<Label>>
    /// where `ruleID` is id of the Rule within a Group and `ruleLabels` is Vec<Label> marshalled
    /// to a string
    previously_sent_series: Mutex<PreviouslySentSeries>,
    pub last_evaluation: Timestamp,
}

/// SKIP_RAND_SLEEP_ON_GROUP_START will skip random sleep delay in group first evaluation
static mut SKIP_RAND_SLEEP_ON_GROUP_START: bool = false;

impl Executor {
    pub fn new(
        notifiers: Arc<Vec<Box<dyn Notifier>>>,
        notifier_headers: Arc<HashMap<String, String>>,
        rw: Arc<WriteQueue>,
        querier: impl Querier,
    ) -> Self {
        Executor {
            eval_ts: Timestamp::now(),
            notifiers,
            notifier_headers,
            rw,
            querier: Arc::new(querier),
            previously_sent_series: Mutex::new(HashMap::new()),
            last_evaluation: Timestamp::now(),
        }
    }

    /// get_stale_series checks whether there are stale series from previously sent ones.
    fn get_stale_series(&self, rule: impl Rule, tss: &[RawTimeSeries], timestamp: Timestamp) -> Vec<RawTimeSeries> {
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

        let stale_nan: f64 = f64::from_bits(STALE_NAN_BITS);

        if let Some(entry) = map.get_mut(&rid) {
            for (key, labels) in entry.iter_mut() {
                if rule_labels.contains_key(key) {
                    continue;
                }
                let stamps = [timestamp];
                let values = [stale_nan];
                // previously sent series are missing in current series, so we mark them as stale
                let ss = new_time_series(key.to_string(), &values, &stamps, &labels);
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

    pub(super) fn exec_concurrently(&mut self,
                                    ctx: &Context,
                                    rules: &mut [impl Rule],
                                    ts: Timestamp,
                                    resolve_duration: Duration,
                                    limit: usize) -> AlertsResult<()> {
        rules
            .par_items_mut()
            .try_for_each(|rule| self.exec(ctx, rule, ts, resolve_duration, limit))
    }

    pub fn exec(&mut self,
                ctx: &Context,
                rule: &mut impl Rule,
                ts: Timestamp,
                resolve_duration: Duration,
                limit: usize) -> AlertsResult<()> {
        let tss = rule.exec(self.querier, ts, limit)
            .map_err(|err| AlertsError::QueryExecutionError(format!("rule {:?}: failed to execute: {:?}", rule, err)))?;

        let stale_series = self.get_stale_series(rule, &tss, ts);

        self.push_to_rw(rule, tss)?;
        self.push_to_rw(rule, stale_series)?;

        if matches!(rule.rule_type(), RuleType::Alerting) {
            let settings = get_global_settings();
            let alerting_rule = rule.downcast_ref::<AlertingRule>().unwrap();
            return self.send_notifications(ctx, alerting_rule, ts, resolve_duration, settings.resend_delay);
        }
        Ok(())
    }

    fn push_to_rw(&mut self, rule: &impl Rule, tss: Vec<RawTimeSeries>) -> AlertsResult<()> {
        let mut last_err = "".to_string();
        for ts in tss {
            if let Err(err) = self.rw.add(ts) {
                last_err = format!("rule {:?}: remote write failure: {:?}", rule, err);
            }
        }
        if !last_err.is_empty() {
            // todo: specific error type
            return Err(AlertsError::Generic(last_err));
        }
        Ok(())
    }

    fn send_notifications(&self,
                          ctx: &Context,
                          rule: &mut AlertingRule,
                          ts: Timestamp,
                          resolve_duration: Duration,
                          resend_delay: Duration) -> AlertsResult<()> {

        rule.process_alerts_to_send(ts, resolve_duration, resend_delay, |alerts| {
            for nt in self.notifiers.iter() {
                if let Err(err) = nt.send(ctx, &alerts, &self.notifier_headers) {
                    let msg = format!("failed to send alerts to addr {}: {:?}", nt.addr(), err);
                    return Err(AlertsError::Generic(msg));
                }
            }
            Ok(())
        })
    }
}

fn new_time_series(key: String, values: &[f64], timestamps: &[i64], labels: &[Label]) -> RawTimeSeries {
    let mut data = SeriesData::new(values.len());
    data.values = values.to_vec();
    data.timestamps = timestamps.to_vec();

    RawTimeSeries {
        key,
        data,
        labels: labels.to_vec(),
    }
}