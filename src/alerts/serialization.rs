use crate::alerts::notifications::{Alert, AlertState};
use crate::alerts::rules::{
    AlertingRule,
    AlertingRuleMetrics,
    Group,
    GroupMetrics,
    MetricRule,
    RecordingRule,
    RecordingRuleMetrics,
    RuleState,
    RuleStateEntry
};
use crate::alerts::{AlertsError, GroupManager, GroupManagerMap, GroupMeta, GROUP_MANAGERS};
use crate::common::serialization::*;
use std::collections::{HashMap, VecDeque};
use std::ffi::c_int;
use std::str::FromStr;
use std::sync::atomic::AtomicI64;
use std::sync::LazyLock;
use valkey_module::{raw, RedisModuleIO, ValkeyError, ValkeyResult};
use crate::server_events::is_async_loading_in_progress;

const RULE_TYPE_ALERTING: u8 = 1;
const RULE_TYPE_RECORDING: u8 = 2;

pub(crate) static STAGING_GROUP_MANAGERS: LazyLock<GroupManagerMap> = LazyLock::new(GroupManagerMap::new);


pub(crate) fn save_rule_state_entry(rdb: *mut RedisModuleIO, state_entry: &RuleStateEntry) {
    rdb_save_timestamp(rdb, state_entry.time);
    rdb_save_timestamp(rdb, state_entry.at);
    rdb_save_duration(rdb, &state_entry.duration);
    // Note: if an error exists, we serialize it as a string, and on reading we instantiate
    // the Generic variant. IOW, this is not round-trip safe
    if let Some(error) = &state_entry.err {
        let err_msg = error.to_string();
        raw::save_string(rdb, &err_msg);
    } else {
        raw::save_string(rdb, "");
    }
    rdb_save_usize(rdb, state_entry.samples);
    rdb_save_optional_usize(rdb, state_entry.series_fetched);
}

pub(crate) fn load_rule_state_entry(rdb: *mut RedisModuleIO) -> ValkeyResult<RuleStateEntry> {
    let time = rdb_load_timestamp(rdb)?;
    let at = rdb_load_timestamp(rdb)?;
    let duration = rdb_load_duration(rdb)?;
    let err_msg = raw::load_string(rdb)?;
    let samples = rdb_load_usize(rdb)?;
    let series_fetched = rdb_load_optional_usize(rdb)?;

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
    rdb_save_usize(rdb, state.0.capacity());
    rdb_save_usize(rdb, state.len());
    for rule in state.iter() {
        save_rule_state_entry(rdb, rule);
    }
}

fn load_rule_state(rdb: *mut RedisModuleIO) -> ValkeyResult<RuleState> {
    let capacity = rdb_load_usize(rdb)?;
    let len = rdb_load_usize(rdb)?;
    let mut state = VecDeque::with_capacity(capacity);
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
    raw::save_unsigned(rdb, rule.rule_id);
    raw::save_string(rdb, &rule.name);
    raw::save_string(rdb, &rule.expr);
    rdb_save_string_hashmap(rdb, &rule.labels);
    save_rule_state(rdb, &rule.state);
    save_recording_rule_metrics(rdb, &rule.metrics);
}

pub(crate) fn load_recording_rule(rdb: *mut RedisModuleIO) -> ValkeyResult<RecordingRule> {
    let id = raw::load_unsigned(rdb)?;
    let name = rdb_load_string(rdb)?;
    let expr = rdb_load_string(rdb)?;
    let labels = rdb_load_string_hashmap(rdb)?;
    let state = load_rule_state(rdb)?;
    let metrics = load_recording_rule_metrics(rdb)?;
    Ok(RecordingRule {
        rule_id: id,
        name,
        expr,
        labels,
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
    rdb_save_string_hashmap(rdb, &rule.labels);
    rdb_save_string_hashmap(rdb, &rule.annotations);
    
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
    let labels = rdb_load_string_hashmap(rdb)?;
    let annotations = rdb_load_string_hashmap(rdb)?;
    let group_id = raw::load_unsigned(rdb)?;
    let group_name = rdb_load_string(rdb)?;
    
    let state = load_rule_state(rdb)?;
    
    let alerts_count = rdb_load_usize(rdb)?;
    let mut alerts = HashMap::new();
    
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

pub fn save_metric_rule(rdb: *mut RedisModuleIO, rule: &MetricRule) {
    match rule {
        MetricRule::AlertingRule(alert_rule) => {
            rdb_save_u8(rdb, RULE_TYPE_ALERTING);
            save_alerting_rule(rdb, alert_rule);
        }
        MetricRule::RecordingRule(recording_rule) => {
            rdb_save_u8(rdb, RULE_TYPE_RECORDING);
            save_recording_rule(rdb, recording_rule);
        }
    }    
}

pub fn load_metric_rule(rdb: *mut RedisModuleIO) -> ValkeyResult<MetricRule> {
    let rule_type = rdb_load_u8(rdb)?;
    match rule_type {
        RULE_TYPE_ALERTING => Ok(MetricRule::AlertingRule(load_alerting_rule(rdb)?)),
        RULE_TYPE_RECORDING => Ok(MetricRule::RecordingRule(load_recording_rule(rdb)?)),
        _ => Err(ValkeyError::Str("Invalid rules type")),
    }
}


pub(crate) fn save_alert(rdb: *mut RedisModuleIO, alert: &Alert) {
    raw::save_unsigned(rdb, alert.id);
    raw::save_unsigned(rdb, alert.group_id);
    raw::save_string(rdb, &alert.name);
    raw::save_string(rdb, &alert.expr);
    rdb_save_string_hashmap(rdb, &alert.labels);
    rdb_save_string_hashmap(rdb, &alert.annotations);
    
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
    let labels = rdb_load_string_hashmap(rdb)?;
    let annotations = rdb_load_string_hashmap(rdb)?;
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

fn save_group_rules(rdb: *mut RedisModuleIO, group: &Group) {
    rdb_save_usize(rdb, group.rules.len());
    for rule in group.rules.iter() {
        save_metric_rule(rdb, rule);
    }
}

fn load_group_rules(rdb: *mut RedisModuleIO) -> ValkeyResult<Vec<MetricRule>> {
    let rule_count = rdb_load_usize(rdb)?;
    let mut rules = Vec::with_capacity(rule_count);
    for _ in 0..rule_count {
        rules.push(load_metric_rule(rdb)?);
    }
    Ok(rules)
}

pub(crate) fn save_group(rdb: *mut RedisModuleIO, group: &Group) {
    raw::save_unsigned(rdb, group.id);
    rdb_save_string(rdb, &group.name);
    
    save_group_rules(rdb, group);
    
    rdb_save_duration(rdb, &group.interval);
    rdb_save_duration(rdb, &group.eval_offset);
    rdb_save_optional_duration(rdb, &group.eval_delay);
    
    let last_evaluation = group.get_last_evaluation();
    save_optional_bool(rdb, group.eval_alignment);
    rdb_save_usize(rdb, group.limit);
    rdb_save_timestamp(rdb, last_evaluation);
    rdb_save_string_hashmap(rdb, &group.labels);
    rdb_save_string_hashmap(rdb, &group.params);
    rdb_save_string_hashmap(rdb, &group.notifier_headers);
    save_group_metrics(rdb, &group.metrics);

    rdb_save_bool(rdb, group.disabled);
}

pub(crate) fn load_group(rdb: *mut RedisModuleIO, _enc_ver: c_int) -> ValkeyResult<Group> {
    let id = raw::load_unsigned(rdb)?;
    let name = rdb_load_string(rdb)?;
    
    let rules = load_group_rules(rdb)?;
    
    let interval = rdb_load_duration(rdb)?;
    let eval_offset = rdb_load_duration(rdb)?;
    let eval_delay = rdb_load_optional_duration(rdb)?;
    
    let eval_alignment = load_optional_bool(rdb)?;
    let limit = rdb_load_usize(rdb)?;
    let last_evaluation = rdb_load_timestamp(rdb)?;
    let labels = rdb_load_string_hashmap(rdb)?;
    let params = rdb_load_string_hashmap(rdb)?;
    let notifier_headers = rdb_load_string_hashmap(rdb)?;
    let metrics = load_group_metrics(rdb)?;
    let disabled = rdb_load_bool(rdb)?;
    
    Ok(Group {
        id,
        name,
        rules,
        interval,
        eval_offset,
        eval_delay,
        eval_alignment,
        limit,
        last_evaluation: AtomicI64::new(last_evaluation),
        labels,
        params,
        notifier_headers,
        dependencies: None,
        metrics,
        disabled,
    })
}

fn save_group_meta(rdb: *mut RedisModuleIO, meta: &GroupMeta) {
    raw::save_unsigned(rdb, meta.hash);
    rdb_save_string(rdb, &meta.name);
    rdb_save_bool(rdb, meta.started);
    raw::save_slice(rdb, &meta.group_key);
    // doesn't make sense to store the timer id
    // raw::save_unsigned(rdb, meta.timer_id);
}

fn load_group_meta(rdb: *mut RedisModuleIO, _enc_ver: c_int) -> ValkeyResult<GroupMeta> {
    let hash = raw::load_unsigned(rdb)?;
    let name = rdb_load_string(rdb)?;
    let started = rdb_load_bool(rdb)?;
    let key_buf = raw::load_string_buffer(rdb)?;
    let group_key = key_buf.as_ref().to_vec().into_boxed_slice();
    
    Ok(GroupMeta {
        hash,
        name,
        started,
        group_key,
        ..Default::default()
    })
}

fn save_group_manager(rdb: *mut RedisModuleIO, manager: &GroupManager) {
    // write_queue, querier_builder and notifiers are globals. we ignore them
    let groups = manager.groups_by_id.pin();
    rdb_save_usize(rdb, groups.len());
    for (id, group) in groups.iter() {
        raw::save_unsigned(rdb, *id);
        save_group_meta(rdb, group);
    }
}

fn load_group_manager(rdb: *mut RedisModuleIO, _enc_ver: c_int) -> ValkeyResult<GroupManager> {
    let groups_count = rdb_load_usize(rdb)?;
    let mut manager = GroupManager::default();
    let mut groups = papaya::HashMap::with_capacity(groups_count);
    let map = groups.pin();
    for _ in 0..groups_count {
        let id = raw::load_unsigned(rdb)?;
        let meta = load_group_meta(rdb, _enc_ver)?;
        map.insert(id, meta);
    }
    manager.groups_by_id = groups;
    Ok(manager)
}

pub fn rdb_save_group_managers(rdb: *mut RedisModuleIO) {
    let managers = GROUP_MANAGERS.pin();
    rdb_save_usize(rdb, managers.len());
    for (db, manager) in managers.iter() {
        raw::save_unsigned(rdb, *db as u64);
        save_group_manager(rdb, manager);
    }
}

pub fn rdb_load_group_managers(rdb: *mut RedisModuleIO, _enc_ver: c_int) -> ValkeyResult<()> {
    let is_async = is_async_loading_in_progress();
    let groups_count = rdb_load_usize(rdb)?;
    let managers = if is_async {
        STAGING_GROUP_MANAGERS.pin()
    } else {
        GROUP_MANAGERS.pin()
    };
    managers.clear();
    
    if groups_count == 0 {
        return Ok(());
    }
    
    for _ in 0..groups_count {
        let id = raw::load_unsigned(rdb)? as u32;
        let meta = load_group_manager(rdb, _enc_ver)?;
        managers.insert(id, meta);
    }
    
    Ok(())
}

pub fn rdb_on_async_load_completed() {
    let mut staging = STAGING_GROUP_MANAGERS.pin();
    let mut current_managers = GROUP_MANAGERS.pin();
    // todo: it's much faster to do a swap, but LazyLock doesn't support it and using
    // a Mutex would be a performance hit
    staging.iter().collect_into(&mut current_managers);
}

pub fn rdb_on_async_load_aborted() {
    STAGING_GROUP_MANAGERS.pin().clear();
}