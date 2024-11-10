use crate::alerts::rules::RulesFilter;
use crate::alerts::notifications::{Alert, AlertState};
use crate::alerts::rules::{AlertingRule, Group, MetricRule, RecordingRule, Rule, RuleStateEntry, RuleType};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::ValkeyValue;

const RULE_TYPE_RECORDING: &str = "recording";
const RULE_TYPE_ALERTING: &str = "alerting";


pub fn rule_to_api(group: &Group, rule: &dyn Rule, exclude_alerts: bool) -> ValkeyValue {
    match rule.rule_type() {
        RuleType::Alerting => {
            let ar = rule.as_any().downcast_ref::<AlertingRule>().unwrap();
            alerting_rule_to_api(group, ar, exclude_alerts)
        },
        RuleType::Recording => {
            let rr = rule.as_any().downcast_ref::<RecordingRule>().unwrap();
            recording_rule_to_api(group, rr)
        },
    }
}

fn rule_state_entry_value(state: &RuleStateEntry) -> ValkeyValue {
    let mut result: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::new();

    result.insert("time".into(), ValkeyValue::from(state.time));
    result.insert("at".into(), ValkeyValue::from(state.at));
    result.insert("duration".into(), ValkeyValue::from(state.duration.as_secs_f64()));
    result.insert("samples".into(), ValkeyValue::from(state.samples));
    result.insert("series_fetched".into(), ValkeyValue::from(state.series_fetched));
    if let Some(err) = &state.err {
        result.insert("err".into(), err.to_string().into());
    };
    result.into()
}

fn string_hash_map_to_value(map: &HashMap<String, String>) -> ValkeyValue {
    let mut result: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::new();
    for (key, value) in map.iter() {
        result.insert(key.into(), ValkeyValue::BulkString(value.clone()));
    }
    result.into()
}

pub(super) fn recording_rule_to_api(group: &Group, rr: &RecordingRule) -> ValkeyValue {
    let entry = RuleStateEntry::default();
    let last_state = rr.get_last_entry().unwrap_or(&entry);
    let max_updates = rr.get_rule_state_count();

    let mut hash: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::new();
    let updates = rr.get_all_entries().iter().map(rule_state_entry_value).collect();
    hash.insert("id".into(), ValkeyValue::BulkString(format!("{}", rr.rule_id)));
    hash.insert("rule_type".into(), RULE_TYPE_RECORDING.into());
    hash.insert("name".into(), ValkeyValue::BulkString(rr.name.clone()));
    hash.insert("query".into(), ValkeyValue::BulkString(rr.expr.clone()));
    hash.insert("labels".into(), string_hash_map_to_value(&rr.labels));
    hash.insert("last_evaluation".into(), ValkeyValue::Integer(last_state.time));
    hash.insert("evaluation_time".into(), ValkeyValue::Float(last_state.duration.as_secs_f64()));
    hash.insert("last_samples".into(), ValkeyValue::Integer(last_state.samples as i64));
    if let Some(last_fetched) = last_state.series_fetched {
        hash.insert("last_series_fetched".into(), ValkeyValue::Integer(last_fetched as i64));
    }
    hash.insert("max_updates".into(), ValkeyValue::Integer(max_updates as i64));
    hash.insert("updates".into(), ValkeyValue::Array(updates));
    hash.insert("group_id".into(), ValkeyValue::BulkString(group.id.to_string()));
    hash.insert("group_name".into(), ValkeyValue::BulkString(group.name.clone()));
    hash.insert("state".into(), "inactive".into());
    hash.insert("duration".into(), ValkeyValue::Float(0.0));
    hash.insert("keep_firing_for".into(), ValkeyValue::Float(0.0));
    hash.insert("annotations".into(), ValkeyValue::Null);
    hash.insert("alerts".into(), ValkeyValue::Null);
    hash.insert("debug".into(), ValkeyValue::Bool(false));
    if let Some(err) = &last_state.err {
        hash.insert("last_error".into(), err.to_string().into());
        hash.insert("health".into(), "err".into());
    } else {
        hash.insert("last_error".into(), "".into());
        hash.insert("health".into(), "ok".into());
    }

    ValkeyValue::Map(hash)
}

pub(super) fn alerting_rule_to_api(group: &Group, ar: &AlertingRule, exclude_alerts: bool) -> ValkeyValue {
    let entry = RuleStateEntry::default();
    let last_state = ar.get_last_entry().unwrap_or(&entry);
    let last_evaluation = last_state.time;
    let last_series_fetched = last_state.series_fetched;
    let evaluation_time = last_state.duration.as_secs_f64();
    let keep_firing_for = ar.keep_firing_for.as_secs_f64();
    let max_updates = ar.get_rule_state_count();

    let mut health = "ok".to_string();
    if last_state.err.is_some() {
        health = "err".to_string();
    }

    let mut state = "inactive".to_string();
    if !ar.alerts.is_empty() {
        state = "pending".to_string();
        for (_, a) in ar.alerts.iter() {
            if a.state == AlertState::Firing {
                state = "firing".to_string();
                break;
            }
        }
    }

    let mut hash: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::new();
    let updates = ar.get_all_entries().iter().map(rule_state_entry_value).collect();
    let duration = ar.r#for.as_secs_f64();

    hash.insert("rule_type".into(), RULE_TYPE_ALERTING.into());
    hash.insert("name".into(), ValkeyValue::BulkString(ar.name.clone()));
    hash.insert("query".into(), ValkeyValue::BulkString(ar.expr.to_string()));
    hash.insert("duration".into(), ValkeyValue::Float(duration));
    hash.insert("keep_firing_for".into(), ValkeyValue::Float(keep_firing_for));
    hash.insert("labels".into(), string_hash_map_to_value(&ar.labels));
    hash.insert("annotations".into(), string_hash_map_to_value(&ar.annotations));
    hash.insert("last_evaluation".into(), ValkeyValue::from(last_evaluation));
    hash.insert("evaluation_time".into(), ValkeyValue::Float(evaluation_time));
    hash.insert("state".into(), ValkeyValue::BulkString(state));
    if exclude_alerts {
        // hash.insert("alerts".into(), ValkeyValue::Null);
    } else {
        hash.insert("alerts".into(), ValkeyValue::Array(rule_to_api_alerts(ar)));
    }
    hash.insert("last_samples".into(), ValkeyValue::Integer(last_state.samples as i64));
    if let Some(last_fetched) = last_series_fetched {
        hash.insert("last_series_fetched".into(), ValkeyValue::from(last_fetched));
    }
    hash.insert("max_updates".into(), ValkeyValue::Integer(max_updates as i64));
    hash.insert("updates".into(), ValkeyValue::Array(updates));
    hash.insert("debug".into(), ValkeyValue::Bool(ar.debug));
    hash.insert("id".into(), ValkeyValue::BulkString(ar.rule_id.to_string()));
    hash.insert("group_id".into(), ValkeyValue::BulkString(group.id.to_string()));
    hash.insert("group_name".into(), ValkeyValue::BulkString(group.name.clone()));

    if let Some(err) = &last_state.err {
        hash.insert("last_error".into(), err.to_string().into());
    }
    hash.insert("health".into(), ValkeyValue::BulkString(health));

    ValkeyValue::Map(hash)
}

pub(crate) fn rule_to_api_alerts(ar: &AlertingRule) -> Vec<ValkeyValue> {
    ar.alerts
        .values()
        .filter(|x| x.state != AlertState::Inactive)
        .map(|a| new_alert_api(ar, a))
        .collect()
}

pub(super) fn new_alert_api(ar: &AlertingRule, a: &Alert) -> ValkeyValue {
    let mut hash: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::new();
    let stabilizing = a.state == AlertState::Firing && a.keep_firing_since == 0;

    hash.insert("id".into(), ValkeyValue::BulkString(format!("{}", a.id)));
    hash.insert("group_id".into(), format!("{}", a.group_id).into());
    hash.insert("name".into(), ValkeyValue::BulkString(a.name.clone()));
    hash.insert("expression".into(), ValkeyValue::BulkString(ar.expr.clone()));
    hash.insert("labels".into(), string_hash_map_to_value(&a.labels));
    hash.insert("annotations".into(), string_hash_map_to_value(&a.annotations));
    hash.insert("state".into(), ValkeyValue::BulkString(a.state.to_string()));
    hash.insert("active_at".into(), ValkeyValue::from(a.active_at));
    hash.insert("restored".into(), ValkeyValue::Bool(a.restored));
    hash.insert("value".into(), ValkeyValue::BulkString(format!("{}", a.value)));
    hash.insert("stabilizing".into(), ValkeyValue::Bool(stabilizing));
    ValkeyValue::Map(hash)
}

pub(super) fn group_to_api(group: &Group, filter: Option<&RulesFilter>) -> ValkeyValue {
    let mut hash: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::new();
    let last_evaluation = group.last_evaluation.load(Ordering::Relaxed);

    hash.insert("id".into(), ValkeyValue::BulkString(format!("{}", group.id())));
    hash.insert("name".into(), ValkeyValue::BulkString(group.name.clone()));
    hash.insert("interval".into(), ValkeyValue::Float(group.interval.as_secs_f64()));
    hash.insert("last_evaluation".into(), ValkeyValue::from(last_evaluation));
    hash.insert("params".into(), string_hash_map_to_value(&group.params));
    hash.insert("notifier_headers".into(), string_hash_map_to_value(&group.params));
    hash.insert("labels".into(), string_hash_map_to_value(&group.labels));
    hash.insert("eval_offset".into(), group.eval_offset.as_secs_f64().into());
    let delay = group.eval_delay.unwrap_or_default().as_secs_f64();
    hash.insert("eval_delay".into(), ValkeyValue::Float(delay));

    let rules = filtered_rules_to_value(group, &group.rules, filter);

    hash.insert("rules".into(), rules);

    ValkeyValue::Map(hash)
}


fn filtered_rules_to_value(group: &Group, rules: &[MetricRule], filter: Option<&RulesFilter>) -> ValkeyValue {
    if let Some(filter) = filter {
        let exclude_alerts = filter.exclude_alerts.unwrap_or_default();
        rules
           .iter()
           .filter(|r| {
                if let Some(rule_type) = &filter.rule_type {
                    if *rule_type != r.rule_type() {
                        return false;
                    }
                }
                if !is_in_list(&filter.rule_names, r.name()) {
                    return false;
                }
                true
            })
           .map(|r| rule_to_api(group, r, exclude_alerts))
           .collect::<Vec<_>>()
           .into()
    } else {
        let res = rules.iter().map(|r| rule_to_api(group, r, false))
                .collect::<Vec<_>>();

        ValkeyValue::Array(res)
    }
}

pub(super) fn is_in_list(list: &[String], needle: &str) -> bool {
    if list.is_empty() {
        return true;
    }
    list.iter().any(|i| i == needle)
}