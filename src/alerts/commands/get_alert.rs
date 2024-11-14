use crate::alerts::commands::api::new_alert_api;
use crate::alerts::rules::MetricRule;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString};
use valkey_module_macros::command;
use crate::alerts::meta::with_rule_group;

/// VM.GET_ALERT groupId alertId
#[command(
    {
        name: "VM.GET-ALERT",
        flags: [ReadOnly, NoMandatoryKeys],
        arity: 3,
        summary: "Get an alert by id",
        key_spec: [
            {
                flags: [ReadOnly, Access],
                begin_search: Index({ index : 0 }),
                find_keys: Range({ last_key : 0, steps : 0, limit : 0 }),
            }
        ]
    }
)]
pub fn get_alert(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    
    let group_id = args.next_u64()?;
    let alert_id = args.next_u64()?;
    
    with_rule_group(ctx, group_id, |group| {
        for rule in group.rules.iter() {
            if let MetricRule::AlertingRule(alerting_rule) = rule {
                if let Some(alert) = alerting_rule.alerts.get(&alert_id) {
                    return Ok(new_alert_api(alerting_rule, alert));
                }
            }
        }
        Err(ValkeyError::Str("ERR: Alert not found"))
    })?
}