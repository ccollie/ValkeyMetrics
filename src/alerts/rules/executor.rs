use std::sync::Arc;
use crate::alerts::datasource::{AlertDatasource, WriteQueue};
use crate::alerts::notifications::Notifier;
use crate::alerts::rules::{AlertingRule, Group, MetricRule, Rule, RuleType};
use crate::alerts::types::RawTimeSeries;
use crate::alerts::{AlertsError, AlertsResult, ALERT_SETTINGS, NOTIFIERS};
use crate::common::types::Timestamp;
use get_size::GetSize;
use rayon::iter::{IntoParallelRefMutIterator, ParallelIterator};
use std::time::Duration;
use valkey_module::ThreadSafeContext;
use crate::common::set_current_db;

#[derive(Clone, Default)]
pub struct Executor {
    querier: AlertDatasource,
    db: i32,
    write_queue: Arc<WriteQueue>
}

impl GetSize for Executor {
    fn get_size(&self) -> usize {
        self.querier.get_size() +
            size_of_val(&self.db) +
            size_of::<Arc<WriteQueue>>() +
            size_of_val(&self.write_queue)
    }
}

impl Executor {
    pub fn new(db: i32, querier: AlertDatasource, write_queue: Arc<WriteQueue>) -> Self {
        Executor { querier, db, write_queue }
    }

    pub(super) fn exec(
        &self,
        group: &mut Group,
        ts: Timestamp,
        resolve_duration: Duration,
        limit: usize,
    ) -> AlertsResult<()> {
        // group.dependencies.is_some() means we have the dependencies in topological order.
        // group.dependencies.is_none() means we have indeterminate dependencies, and rules
        // should be evaluated sequentially

        // We need to execute the rules in dependency order
        if group.dependencies.is_some() {
            return self.exec_dag(group, ts, resolve_duration, limit);
        }

        let mut errors = vec![];
        self.exec_rules_sequentially(
            group,
            RuleType::Recording,
            ts,
            resolve_duration,
            limit,
            &mut errors,
        );

        if errors.is_empty() {
            return Ok(());
        }

        Err(AlertsError::GroupExecutionError(errors.into()))
    }

    fn exec_dag(
        &self,
        group: &mut Group,
        ts: Timestamp,
        resolve_duration: Duration,
        limit: usize,
    ) -> AlertsResult<()> {
        let mut rules = std::mem::take(&mut group.rules);
        let dag = group.dependencies.as_ref().unwrap();
        let mut errors = Vec::new();

        for dependencies in dag.iter() {
            let mut rule_dependencies: Vec<MetricRule> = dependencies
                .iter()
                .map(|&idx| std::mem::take(&mut rules[idx]))
                .collect();

            let results: Vec<AlertsError> = rule_dependencies
                .par_iter_mut()
                .filter_map(|rule| {
                    self.exec_rule(group, rule, ts, resolve_duration, limit)
                        .err()
                })
                .collect();

            errors.extend(results);

            for (&index, rule) in dependencies.iter().zip(rule_dependencies) {
                rules[index] = rule;
            }
        }

        group.rules = rules;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(AlertsError::GroupExecutionError(errors.into()))
        }
    }

    fn exec_rules_sequentially(
        &self,
        group: &mut Group,
        rule_type: RuleType,
        ts: Timestamp,
        resolve_duration: Duration,
        limit: usize,
        errors: &mut Vec<AlertsError>,
    ) {
        let mut rules = std::mem::take(&mut group.rules);

        errors.extend(rules.iter_mut().filter_map(|rule| {
            (rule.rule_type() == rule_type)
                .then(|| {
                    self.exec_rule(group, rule, ts, resolve_duration, limit)
                        .err()
                })
                .flatten()
        }));

        group.rules = rules;
    }

    fn exec_rule(
        &self,
        group: &Group,
        rule: &mut MetricRule,
        ts: Timestamp,
        resolve_duration: Duration,
        limit: usize,
    ) -> AlertsResult<()> {
        rule.exec(&self.querier, ts, limit)
            .map_err(|err| {
                AlertsError::QueryExecutionError(format!(
                    "rules {:?}: failed to execute: {:?}",
                    rule, err
                ))
            })
            .map(|tss| {
                self.push_to_rw(tss);
                
                if let MetricRule::AlertingRule(alerting_rule) = rule {
                    return self.send_notifications(
                        group,
                        alerting_rule,
                        ts,
                        resolve_duration,
                        ALERT_SETTINGS.resend_delay,
                    );
                }
                Ok(())
            })?
    }

    fn push_to_rw(&self, tss: Vec<RawTimeSeries>) {
        tss.into_iter().for_each(|ts| {
            self.write_queue.add(ts);
        });
    }

    fn send_notifications(
        &self,
        group: &Group,
        rule: &mut AlertingRule,
        ts: Timestamp,
        resolve_duration: Duration,
        resend_delay: Duration,
    ) -> AlertsResult<()> {
        rule.process_alerts_to_send(ts, resolve_duration, resend_delay, |alerts| {
            let alert_slice = alerts.as_slice();

            let thread_ctx = ThreadSafeContext::new();
            let context_guard = thread_ctx.lock();
            set_current_db(&context_guard, self.db);
            
            for nt in NOTIFIERS.iter() {
                if let Err(err) = nt.send(&context_guard, alert_slice, &group.notifier_headers) {
                    let msg = format!("failed to send alerts to addr {}: {:?}", nt.addr(), err);
                    return Err(AlertsError::Generic(msg));
                }
            }
            Ok(())
        })
    }
}
