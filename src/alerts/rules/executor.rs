use std::time::Duration;
use get_size::GetSize;
use crate::alerts::datasource::AlertDatasource;
use crate::alerts::notifications::Notifier;
use crate::alerts::rules::{AlertingRule, Group, MetricRule, Rule, RuleType};
use crate::alerts::types::RawTimeSeries;
use crate::alerts::{AlertsError, AlertsResult, NOTIFIERS, WRITE_QUEUE};
use crate::common::types::Timestamp;
use crate::config::get_global_settings;
use rayon::iter::{IntoParallelRefMutIterator, ParallelIterator};
use valkey_module::ThreadSafeContext;

#[derive(Clone, Default, GetSize)]
pub struct Executor {
    querier: AlertDatasource,
}

impl Executor {
    pub fn new(querier: AlertDatasource) -> Self {
        Executor { querier }
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
                    let settings = get_global_settings();
                    return self.send_notifications(
                        group,
                        alerting_rule,
                        ts,
                        resolve_duration,
                        settings.resend_delay,
                    );
                }
                Ok(())
            })?
    }

    fn push_to_rw(&self, tss: Vec<RawTimeSeries>) {
        tss.into_iter().for_each(|ts| {
            WRITE_QUEUE.add(ts);
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
            let thread_ctx = ThreadSafeContext::new();
            let context_guard = thread_ctx.lock();
            let alert_slice = alerts.as_slice();
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
