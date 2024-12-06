use crate::alerts::commands::api::{group_to_api, rule_to_api_alerts};
use crate::alerts::meta::with_rule_groups;
use crate::alerts::rules::MetricRule;
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};
use valkey_module_macros::command;

/// VM.ALERTS groupName...
#[command(
    {
        name: "VM.ALERTS",
        flags: [ReadOnly, NoMandatoryKeys],
        arity: -1,
        summary: "Returns a list of active alerts.",
        key_spec: [
            {
                flags: [ReadOnly, Access, NotKey],
                begin_search: Index({ index : 0 }),
                find_keys: Range({ last_key : 0, steps: 0, limit : 0 }),
            }
        ]
    }
)]
pub fn alerts(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let group_names: Vec<_> = args
        .into_iter()
        .skip(1)
        .map(|x| x.to_string_lossy())
        .collect();

    struct GroupState {
        name: String,
        group: ValkeyValue,
        alerts: ValkeyValue,
    }

    let mut groups: Vec<GroupState> = Vec::new();
    let name_filter = group_names.as_slice();
    with_rule_groups(ctx, name_filter, &mut groups, |state, group| {
        let mut alerts = Vec::new();

        for r in group.rules.iter().filter_map(|r| match r {
            MetricRule::AlertingRule(a) => Some(a),
            _ => None,
        }) {
            alerts.extend(rule_to_api_alerts(r));
        }
        if !alerts.is_empty() {
            state.push(GroupState {
                name: group.name.clone(),
                group: group_to_api(group, None),
                alerts: alerts.into(),
            });
        }
    });

    groups.sort_by(|a, b| a.name.cmp(&b.name));

    let alerts: Vec<_> = groups
        .into_iter()
        .map(|state| {
            let mut map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::new();
            map.insert("group".into(), state.group);
            map.insert("alerts".into(), state.alerts);
        })
        .collect();

    Ok(alerts.into())
}
