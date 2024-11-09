use crate::alerts::notifications::{Alert, AlertState};
use crate::alerts::rules::{AlertingRule, Group, MetricRule, RecordingRule, Rule, RuleStateEntry, RuleType};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::ValkeyValue;

const PARAM_GROUP_ID: &str = "group_id";
const PARAM_ALERT_ID: &str = "alert_id";
const PARAM_RULE_ID: &str = "rule_id";

const RULE_TYPE_RECORDING: &str = "recording";
const RULE_TYPE_ALERTING: &str = "alerting";

pub struct RulesFilter {
    pub(crate) group_names: Vec<String>,
    pub(crate) rule_names: Vec<String>,
    pub(crate) rule_type: Option<RuleType>,
    pub(crate) exclude_alerts: Option<bool>
}

#[derive(Debug, Clone)]
struct GroupAlerts {
    group: ValkeyValue,
    alerts: Vec<ApiAlert>,
}

#[derive(Debug, Clone)]
struct ApiRuleWithUpdates {
    api_rule: ApiRule,
    state_updates: Vec<RuleStateEntry>,
}


pub fn rule_to_api(rule: &dyn Rule, exclude_alerts: bool) -> ValkeyValue {
    match rule.rule_type() {
        RuleType::Alerting => {
            let ar = rule.as_any().downcast_ref::<AlertingRule>().unwrap();
            alerting_rule_to_api(ar, exclude_alerts)
        },
        RuleType::Recording => {
            let rr = rule.as_any().downcast_ref::<RecordingRule>().unwrap();
            recording_rule_to_api(rr)
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

pub(super) fn recording_rule_to_api(rr: &RecordingRule) -> ValkeyValue {
    let entry = RuleStateEntry::default();
    let last_state = rr.get_last_entry().unwrap_or(&entry);
    let max_updates = rr.get_rule_state_count();

    let mut hash: HashMap<String, ValkeyValue> = HashMap::new();
    let updates = rr.get_all_entries().iter().map(rule_state_entry_value).collect();
    hash.insert("id".to_string(), rr.rule_id.into());
    hash.insert("rule_type".to_string(), RULE_TYPE_RECORDING.into());
    hash.insert("name".to_string(), ValkeyValue::BulkString(rr.name.clone()));
    hash.insert("query".to_string(), ValkeyValue::BulkString(rr.expr.clone()));
    hash.insert("labels".to_string(), string_hash_map_to_value(&rr.labels));
    hash.insert("last_evaluation".to_string(), ValkeyValue::Integer(*last_state.time));
    hash.insert("evaluation_time".to_string(), ValkeyValue::Float(*last_state.duration.as_secs_f64()));
    hash.insert("last_samples".to_string(), ValkeyValue::Integer(*last_state.samples.into()));
    hash.insert("last_series_fetched".to_string(), ValkeyValue::Integer(*last_state.series_fetched as i64));
    hash.insert("max_updates".to_string(), ValkeyValue::Integer(max_updates.into()));
    hash.insert("updates".to_string(), ValkeyValue::Array(updates));
    hash.insert("group_id".to_string(), rr.group_id.into());
    hash.insert("group_name".to_string(), rr.group_name.into());
    hash.insert("state".to_string(), "inactive".into());
    hash.insert("duration".to_string(), ValkeyValue::Float(0.0));
    hash.insert("keep_firing_for".to_string(), ValkeyValue::Float(0.0));
    hash.insert("annotations".to_string(), ValkeyValue::Null);
    hash.insert("alerts".to_string(), ValkeyValue::Null);
    hash.insert("debug".to_string(), ValkeyValue::Bool(false));
    if let Some(err) = &last_state.err {
        hash.insert("last_error".to_string(), err.to_string().into());
        hash.insert("health".to_string(), "err".into());
    } else {
        hash.insert("last_error".to_string(), "".into());
        hash.insert("health".to_string(), "ok".into());
    }

    ValkeyValue::Map(hash)
}

pub(super) fn alerting_rule_to_api(ar: &AlertingRule, exclude_alerts: bool) -> ValkeyValue {
    let entry = RuleStateEntry::default();
    let last_state = ar.get_last_entry().unwrap_or(&entry);
    let last_evaluation = *last_state.time;
    let last_series_fetched = last_state.series_fetched;
    let evaluation_time = *last_state.duration.as_secs_f64();
    let keep_firing_for = ar.keep_firing_for.as_secs_f64();
    let max_updates = ar.get_rule_state_count();

    let mut health = "ok".to_string();
    if let Some(err) = &last_state.err {
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
        hash.insert("alerts".into(), ValkeyValue::Array(rule_to_api_alert(ar)));   
    }
    hash.insert("last_samples".into(), ValkeyValue::Integer(*last_state.samples));
    hash.insert("last_series_fetched".into(), ValkeyValue::Integer(last_series_fetched.into()));
    hash.insert("max_updates".into(), ValkeyValue::Integer(max_updates.into()));
    hash.insert("updates".into(), ValkeyValue::Array(updates));
    hash.insert("debug".into(), ValkeyValue::Bool(ar.debug));
    hash.insert("id".into(), ValkeyValue::BulkString(ar.rule_id.to_string()));
    hash.insert("group_id".into(), ValkeyValue::BulkString(ar.group_id.to_string()));
    hash.insert("group_name".into(), ValkeyValue::BulkString(r.group_name));

    if let Some(err) = &last_state.err {
        hash.insert("last_error".into(), err.to_string().into());
    }
    hash.insert("health".into(), ValkeyValue::BulkString(health));

    ValkeyValue::Map(hash)
}

fn rule_to_api_alert(ar: &AlertingRule) -> Vec<ValkeyValue> {
    ar.alerts
        .values()
        .filter(|x| x.state != AlertState::Inactive)
        .map(|a| new_alert_api(ar, a))
        .collect()
}

fn new_alert_api(ar: &AlertingRule, a: &Alert) -> ValkeyValue {
    let mut hash = HashMap::new();
    let stabilizing = a.state == AlertState::Firing && a.keep_firing_since == 0;

    hash.insert("id".into(), ValkeyValue::BulkString(format!("{}", a.id)));
    hash.insert("group_id".into(), a.group_id.into());
    hash.insert("name".into(), ValkeyValue::BulkString(a.name.clone()));
    hash.insert("expression".into(), ValkeyValue::BulkString(ar.expr.clone()));
    hash.insert("labels".into(), match &a.labels {
        Some(labels) => string_hash_map_to_value(labels),
        None => ValkeyValue::Null,
    });
    hash.insert("annotations".into(), string_hash_map_to_value(&a.annotations));
    hash.insert("state".into(), ValkeyValue::BulkString(a.state.to_string()));
    hash.insert("active_at".into(), ValkeyValue::from(a.active_at));
    hash.insert("restored".into(), ValkeyValue::Bool(a.restored));
    hash.insert("value".into(), ValkeyValue::BulkString(format!("{}", a.value)));
    hash.insert("stabilizing".into(), ValkeyValue::Bool(stabilizing));
    ValkeyValue::Map(hash)
}

pub(super) fn group_to_api(g: &Group, filter: Option<&RulesFilter>) -> ValkeyValue {
    let mut hash = HashMap::new();
    let last_evaluation = g.last_evaluation.load(Ordering::Relaxed);

    hash.insert("id".into(), ValkeyValue::BulkString(format!("{}", g.id())));
    hash.insert("name".into(), ValkeyValue::BulkString(g.name.clone()));
    hash.insert("interval".into(), ValkeyValue::Float(g.interval.as_secs_f64()));
    hash.insert("last_evaluation".into(), ValkeyValue::from(last_evaluation));
    hash.insert("params".into(), string_hash_map_to_value(&g.params));
    hash.insert("notifier_headers".into(), string_hash_map_to_value(&g.params));
    hash.insert("labels".into(), match &g.labels {
        Some(labels) => string_hash_map_to_value(labels),
        None => ValkeyValue::Null,
    });
    hash.insert("eval_offset".into(), g.eval_offset.as_secs_f64().into());
    let delay = g.eval_delay.unwrap_or_default().as_secs_f64();
    hash.insert("eval_delay".into(), ValkeyValue::Float(delay));
    
    let rules = filtered_rules_to_value(&g.rules, filter);
 
    hash.insert("rules".into(), rules);

    ValkeyValue::Map(hash)
}


fn filtered_rules_to_value(rules: &[MetricRule], filter: Option<&RulesFilter>) -> ValkeyValue {
    if let Some(filter) = filter {
        let exclude_alerts = filter.exclude_alerts.unwrap_or_default();
        rules
           .iter()
           .filter(|r| {
                if let Some(rule_type) = &filter.rule_type {
                    if rule_type != r.rule_type() {
                        return false;
                    }
                }
                if !is_in_list(&filter.rule_names, r.name()) {
                    return false;
                }
                true
            })
           .map(|r| rule_to_api(r, exclude_alerts))
           .collect()
           .into()
    } else {
        Ok(
            rules.iter()
                .map(|r| rule_to_api(r, false))
                .collect()
                .into(),
        )
    }   
}

pub(super) fn is_in_list(list: &[String], needle: &str) -> bool {
    if list.is_empty() {
        return true;
    }
    list.iter().any(|i| i == needle)
}