use crate::alerts::commands::api::group_to_api;
use crate::alerts::group_manager::GROUP_MANAGER;
use crate::alerts::rules::{RuleType, RulesFilter};
use crate::error_consts;
use crate::module::arg_parse::parse_label_list;
use std::cmp::Ordering;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use valkey_module_macros::command;

/// VM.GROUPS [RULE_GROUP groupName...] [RULE_NAME ruleName...] [RULE_TYPE alert|record] [EXCLUDE_ALERTS]
#[command(
    {
        name: "VM.GROUPS",
        flags: [ReadOnly, NoMandatoryKeys],
        arity: -1,
        summary: "Returns a list of groups and associated rules.",
        key_spec: [
            {
                flags: [ReadOnly, Access, NotKey],
                begin_search: Index({ index : 0 }),
                find_keys: Range({ last_key : 0, steps: 0, limit : 0 }),
            }
        ]
    }
)]
pub fn groups(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    struct GroupState {
        name: String,
        id: u64,
        group: ValkeyValue,
    }

    let filter = parse_alert_rules_filter(args)?;

    let mut groups: Vec<GroupState> = Vec::new();
    let name_filter = filter.group_names.as_slice();
    GROUP_MANAGER.with_groups(ctx, name_filter, &mut groups, |state, group| {
        state.push(GroupState {
            name: group.name.clone(),
            id: group.id,
            group: group_to_api(group, Some(&filter)),
        });
    });

    groups.sort_by(|a, b| {
        let ordering = a.name.cmp(&b.name);
        if ordering == Ordering::Equal {
            let right_id = b.id;
            a.id.cmp(&right_id)
        } else {
            ordering
        }
    });

    let groups: Vec<_> = groups.into_iter().map(|x| x.group).collect();
    Ok(groups.into())
}

const CMD_ARG_GROUPNAME: &'static str = "RULE_GROUP";
const CMD_ARG_RULENAME: &'static str = "RULE_NAME";
const CMD_ARG_RULETYPE: &'static str = "RULE_TYPE";
const CMD_ARG_EXCLUDE_ALERTS: &'static str = "EXCLUDE_ALERTS";

pub fn parse_alert_rules_filter(args: Vec<ValkeyString>) -> ValkeyResult<RulesFilter> {
    let mut args = args.into_iter().skip(1).peekable();
    fn is_cmd_token(token: &str) -> bool {
        const TOKENS: [&str; 4] = [
            CMD_ARG_GROUPNAME,
            CMD_ARG_RULENAME,
            CMD_ARG_RULETYPE,
            CMD_ARG_EXCLUDE_ALERTS,
        ];
        TOKENS.contains(&token)
    }

    let mut filter = RulesFilter::default();

    while let Ok(arg) = args.next_str() {
        let token = arg.to_ascii_uppercase();
        match token.as_str() {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_GROUPNAME) => {
                filter.group_names = parse_label_list(&mut args, is_cmd_token)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_RULENAME) => {
                filter.rule_names = parse_label_list(&mut args, is_cmd_token)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_RULETYPE) => {
                let rule_type = args.next_str()?;
                match rule_type {
                    arg if arg.eq_ignore_ascii_case("alert") => filter.rule_type = Some(RuleType::Alerting),
                    arg if arg.eq_ignore_ascii_case("record") => filter.rule_type = Some(RuleType::Recording),
                    _ => return Err(ValkeyError::Str("ERR invalid rule type")),
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_EXCLUDE_ALERTS) => {
                filter.exclude_alerts = Some(true);
            }
            _ => return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT)),
        }
    }

    Ok(filter)
}