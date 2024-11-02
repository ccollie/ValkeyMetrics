use crate::alerts::notifier::{Alert, AlertState};
use crate::alerts::rule::{
    AlertingRule,
    AlertingRuleMetrics, 
    Group,
    GroupMetrics,
    RecordingRule,
    RecordingRuleMetrics,
    RuleState,
    RuleStateEntry
};
use crate::alerts::AlertsError;
use crate::common::serialization::*;
use ahash::AHashMap;
use std::collections::VecDeque;
use std::ffi::c_int;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use valkey_module::{raw, RedisModuleIO, ValkeyError, ValkeyResult};

fn save_atomic_u64(rdb: *mut RedisModuleIO, value: &AtomicU64) {
    raw::save_unsigned(rdb, value.load(std::sync::atomic::Ordering::Relaxed))
}

fn load_atomic_u64(rdb: *mut RedisModuleIO) -> ValkeyResult<AtomicU64> {
    let value = raw::load_unsigned(rdb)?;
    Ok(AtomicU64::new(value))
}

pub(crate) fn save_rule_state_entry(rdb: *mut RedisModuleIO, state_entry: &RuleStateEntry) {
    rdb_save_timestamp(rdb, state_entry.time);
    rdb_save_timestamp(rdb, state_entry.at);
    rdb_save_duration(rdb, &state_entry.duration);
    // Note: if an error exists, we serialize it as a string, and on reading we instantate
    // the Generic variant. IOW, this is not round-trip safe
    if let Some(error) = &state_entry.err {
        let err_msg = error.to_string();
        raw::save_string(rdb, &err_msg);
    } else {
        raw::save_string(rdb, "");
    }
    rdb_save_usize(rdb, state_entry.samples);
    rdb_save_usize(rdb, state_entry.series_fetched);
}

pub(crate) fn load_rule_state_entry(rdb: *mut RedisModuleIO) -> ValkeyResult<RuleStateEntry> {
    let time = rdb_load_timestamp(rdb)?;
    let at = rdb_load_timestamp(rdb)?;
    let duration = rdb_load_duration(rdb)?;
    let err_msg = raw::load_string(rdb)?;
    let samples = rdb_load_usize(rdb)?;
    let series_fetched = rdb_load_usize(rdb)?;

    let err = if err_msg.is_empty() {
        None
    } else {
        Some(AlertsError::Generic(err_msg.to_string_lossy()))
    };
    
    Ok(RuleStateEntry {
        time,
        at,
        duration,
        err,
        samples,
        series_fetched,
    })
}

fn save_rule_state(rdb: *mut RedisModuleIO, state: &RuleState) {
    rdb_save_usize(rdb, state.len());
    for rule in state.iter() {
        save_rule_state_entry(rdb, rule);
    }
}

fn load_rule_state(rdb: *mut RedisModuleIO) -> ValkeyResult<RuleState> {
    let len = rdb_load_usize(rdb)?;
    let mut state = VecDeque::with_capacity(len);
    for _ in 0..len {
        state.push_back(load_rule_state_entry(rdb)?);
    }
    Ok(RuleState(state))
}

fn save_recording_rule_metrics(rdb: *mut RedisModuleIO, metrics: &RecordingRuleMetrics) {
    save_atomic_u64(rdb, &metrics.errors);
    save_atomic_u64(rdb, &metrics.samples);
}

fn load_recording_rule_metrics(rdb: *mut RedisModuleIO) -> ValkeyResult<RecordingRuleMetrics> {
    let errors = load_atomic_u64(rdb)?;
    let samples = load_atomic_u64(rdb)?;
    Ok(RecordingRuleMetrics { errors, samples })
}

pub(crate) fn save_recording_rule(rdb: *mut RedisModuleIO, rule: &RecordingRule) {
    raw::save_unsigned(rdb, rule.id);
    raw::save_string(rdb, &rule.name);
    raw::save_string(rdb, &rule.key);
    raw::save_string(rdb, &rule.expr);
    rdb_save_ahashmap(rdb, &rule.labels);
    raw::save_unsigned(rdb, rule.group_id);
    // save rule state entries
    rdb_save_usize(rdb, rule.state.len());
    for state_entry in rule.state.iter() {
        save_rule_state_entry(rdb, state_entry);
    }
    save_recording_rule_metrics(rdb, &rule.metrics);
}

pub(crate) fn load_recording_rule(rdb: *mut RedisModuleIO) -> ValkeyResult<RecordingRule> {
    let id = raw::load_unsigned(rdb)?;
    let name = rdb_load_string(rdb)?;
    let key = raw::load_string(rdb)?;
    let expr = rdb_load_string(rdb)?;
    let labels = rdb_load_ahashmap(rdb)?;
    let group_id = raw::load_unsigned(rdb)?;
    let state_count = rdb_load_usize(rdb)?;
    let mut state = Vec::with_capacity(state_count);
    for _ in 0..state_count {
        state.push(load_rule_state_entry(rdb)?);
    }
    let metrics = load_recording_rule_metrics(rdb)?;
    Ok(RecordingRule {
        id,
        key: key.to_string_lossy(),
        name,
        expr,
        labels,
        group_id,
        max_entries_limit: None,
        state,
        metrics,
    })
}

fn save_alerting_rule_metrics(rdb: *mut RedisModuleIO, metrics: &AlertingRuleMetrics) {
    save_atomic_u64(rdb, &metrics.errors);
    save_atomic_u64(rdb, &metrics.active);
    save_atomic_u64(rdb, &metrics.pending);
    save_atomic_u64(rdb, &metrics.samples);
    save_atomic_u64(rdb, &metrics.series_fetched);
}

fn load_alerting_rule_metrics(rdb: *mut RedisModuleIO) -> ValkeyResult<AlertingRuleMetrics> {
    let errors = load_atomic_u64(rdb)?;
    let active = load_atomic_u64(rdb)?;
    let pending = load_atomic_u64(rdb)?;
    let samples = load_atomic_u64(rdb)?;
    let series_fetched = load_atomic_u64(rdb)?;
    Ok(AlertingRuleMetrics {
        errors,
        active,
        pending,
        samples,
        series_fetched,
    })
}

pub(crate) fn save_alerting_rule(rdb: *mut RedisModuleIO, rule: &AlertingRule) {
    raw::save_unsigned(rdb, rule.rule_id);
    rdb_save_string(rdb, &rule.name);
    rdb_save_string(rdb, &rule.expr);
    rdb_save_duration(rdb, &rule.r#for);
    rdb_save_duration(rdb, &rule.keep_firing_for);
    rdb_save_duration(rdb, &rule.eval_interval);
    rdb_save_ahashmap(rdb, &rule.labels);
    rdb_save_ahashmap(rdb, &rule.annotations);
    
    raw::save_unsigned(rdb, rule.group_id);
    rdb_save_string(rdb, &rule.group_name);
    
    save_rule_state(rdb, &rule.state);
    
    // serialize alerts
    rdb_save_usize(rdb, rule.alerts.len());
    for (_, alert) in rule.alerts.iter() {
        save_alert(rdb, alert);
    }
 
    save_alerting_rule_metrics(rdb, &rule.metrics);
}


pub(crate) fn load_alerting_rule(rdb: *mut RedisModuleIO) -> ValkeyResult<AlertingRule> {
    let rule_id = raw::load_unsigned(rdb)?;
    let name = rdb_load_string(rdb)?;
    let expr = rdb_load_string(rdb)?;
    let r#for = rdb_load_duration(rdb)?;
    let keep_firing_for = rdb_load_duration(rdb)?;
    let eval_interval = rdb_load_duration(rdb)?;
    let labels = rdb_load_ahashmap(rdb)?;
    let annotations = rdb_load_ahashmap(rdb)?;
    let group_id = raw::load_unsigned(rdb)?;
    let group_name = rdb_load_string(rdb)?;
    
    let state = load_rule_state(rdb)?;
    
    let alerts_count = rdb_load_usize(rdb)?;
    let mut alerts = AHashMap::new();
    
    for _ in 0..alerts_count {
        let alert = load_alert(rdb)?;
        alerts.insert(alert.id, alert);
    }
    
    let metrics = load_alerting_rule_metrics(rdb)?;
    Ok(AlertingRule {
        rule_id,
        name,
        expr,
        r#for,
        keep_firing_for,
        eval_interval,
        labels,
        annotations,
        group_id,
        group_name,
        state,
        alerts,
        metrics,
        debug: false,
    })
}

pub(crate) fn save_alert(rdb: *mut RedisModuleIO, alert: &Alert) {
    raw::save_unsigned(rdb, alert.id);
    raw::save_unsigned(rdb, alert.group_id);
    raw::save_string(rdb, &alert.name);
    raw::save_string(rdb, &alert.expr);
    rdb_save_ahashmap(rdb, &alert.labels);
    rdb_save_ahashmap(rdb, &alert.annotations);
    
    let state = alert.state.name();
    raw::save_string(rdb, state);
    
    rdb_save_timestamp(rdb, alert.active_at);
    rdb_save_timestamp(rdb, alert.start);
    rdb_save_timestamp(rdb, alert.end);
    rdb_save_timestamp(rdb, alert.resolved_at);
    rdb_save_timestamp(rdb, alert.last_sent);
    rdb_save_timestamp(rdb, alert.keep_firing_since);
    raw::save_double(rdb, alert.value);
    rdb_save_bool(rdb, alert.restored);
    rdb_save_duration(rdb, &alert.r#for);
}

pub(crate) fn load_alert(rdb: *mut RedisModuleIO) -> ValkeyResult<Alert> {
    let id = raw::load_unsigned(rdb)?;
    let group_id = raw::load_unsigned(rdb)?;
    let name = rdb_load_string(rdb)?;
    let expr = rdb_load_string(rdb)?;
    let labels = rdb_load_ahashmap(rdb)?;
    let annotations = rdb_load_ahashmap(rdb)?;
    let state_str = rdb_load_string(rdb)?;
    let state = AlertState::from_str(&state_str)
        .map_err(|_| ValkeyError::Str("Invalid alert state"))?;
    let active_at = rdb_load_timestamp(rdb)?;
    let start = rdb_load_timestamp(rdb)?;
    let end = rdb_load_timestamp(rdb)?;
    let resolved_at = rdb_load_timestamp(rdb)?;
    let last_sent = rdb_load_timestamp(rdb)?;
    let keep_firing_since = rdb_load_timestamp(rdb)?;
    let value = raw::load_double(rdb)?;
    let restored = rdb_load_bool(rdb)?;
    let r#for = rdb_load_duration(rdb)?;

    Ok(Alert {
        id,
        group_id,
        name,
        expr,
        labels,
        annotations,
        state,
        active_at,
        start,
        end,
        resolved_at,
        last_sent,
        keep_firing_since,
        value,
        restored,
        r#for,
    })
}

fn save_group_metrics(rdb: *mut RedisModuleIO, metrics: &GroupMetrics) {
    save_atomic_u64(rdb, &metrics.iteration_total);
    save_atomic_u64(rdb, &metrics.iteration_duration);
    save_atomic_u64(rdb, &metrics.iteration_missed);
    save_atomic_u64(rdb, &metrics.iteration_interval);
}

fn load_group_metrics(rdb: *mut RedisModuleIO) -> ValkeyResult<GroupMetrics> {
    let iteration_total = load_atomic_u64(rdb)?;
    let iteration_duration = load_atomic_u64(rdb)?;
    let iteration_missed = load_atomic_u64(rdb)?;
    let iteration_interval = load_atomic_u64(rdb)?;
    Ok(GroupMetrics {
        iteration_total,
        iteration_duration,
        iteration_missed,
        iteration_interval,
    })
}

pub(crate) fn save_group(rdb: *mut RedisModuleIO, group: &Group) {
    raw::save_unsigned(rdb, group.id);
    rdb_save_string(rdb, &group.name);
    
    rdb_save_usize(rdb, group.alerting_rules.len());
    for rule in &group.alerting_rules {
        save_alerting_rule(rdb, rule);
    }

    rdb_save_usize(rdb, group.recording_rules.len());
    for rule in &group.recording_rules {
        save_recording_rule(rdb, rule);
    }
    
    rdb_save_duration(rdb, &group.interval);
    rdb_save_duration(rdb, &group.eval_offset);
    rdb_save_optional_duration(rdb, &group.eval_delay);
    
    let eval_alignment = if let Some(val) = group.eval_alignment {
        if val { 1 } else { 0 }
    } else {
        2 // null marker
    };
    
    rdb_save_u8(rdb, eval_alignment);
    rdb_save_usize(rdb, group.limit);
    rdb_save_timestamp(rdb, group.last_evaluation);
    rdb_save_ahashmap(rdb, &group.labels);
    rdb_save_ahashmap(rdb, &group.params);
    rdb_save_string_hashmap(rdb, &group.notifier_headers);
    save_group_metrics(rdb, &group.metrics);
    // todo: notifiers
    rdb_save_bool(rdb, group.disabled);
}

pub(crate) fn load_group(rdb: *mut RedisModuleIO, _encver: c_int) -> ValkeyResult<Group> {
    let id = raw::load_unsigned(rdb)?;
    let name = rdb_load_string(rdb)?;
    
    let alerting_rules_count = rdb_load_usize(rdb)?;
    let mut alerting_rules = Vec::with_capacity(alerting_rules_count);
    for _ in 0..alerting_rules_count {
        alerting_rules.push(load_alerting_rule(rdb)?);
    }

    let recording_rules_count = rdb_load_usize(rdb)?;
    let mut recording_rules = Vec::with_capacity(recording_rules_count);
    for _ in 0..recording_rules_count {
        recording_rules.push(load_recording_rule(rdb)?);
    }
    
    let interval = rdb_load_duration(rdb)?;
    let eval_offset = rdb_load_duration(rdb)?;
    let eval_delay = rdb_load_optional_duration(rdb)?;
    
    let eval_alignment = match rdb_load_u8(rdb)? {
        1 => Some(true),
        0 => Some(false),
        2 => None, // null marker
        _ => return Err(ValkeyError::Str("Invalid eval alignment")),
    };
    let limit = rdb_load_usize(rdb)?;
    let last_evaluation = rdb_load_timestamp(rdb)?;
    let labels = rdb_load_ahashmap(rdb)?;
    let params = rdb_load_ahashmap(rdb)?;
    let notifier_headers = rdb_load_string_hashmap(rdb)?;
    let metrics = load_group_metrics(rdb)?;
    // todo: notifiers
    let disabled = rdb_load_bool(rdb)?;
    
    Ok(Group {
        id,
        name,
        alerting_rules,
        recording_rules,
        interval,
        eval_offset,
        eval_delay,
        eval_alignment,
        limit,
        last_evaluation,
        labels,
        params,
        notifier_headers,
        notifiers: vec![],
        metrics,
        disabled,
    })
}