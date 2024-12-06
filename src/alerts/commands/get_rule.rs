use crate::alerts::commands::rule_to_api;
use crate::alerts::meta::with_rule_group;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString};
use valkey_module_macros::command;

/// VM.GET_ALERT groupId alertId [EXCLUDE_ALERTS]
#[command(
    {
        name: "VM.GET-RULE",
        flags: [ReadOnly, NoMandatoryKeys],
        arity: -3,
        summary: "Get an rule by id",
        key_spec: [
            {
                flags: [ReadOnly, Access],
                begin_search: Index({ index : 0 }),
                find_keys: Range({ last_key : 0, steps : 0, limit : 0 }),
            }
        ]
    }
)]
pub fn get_rule(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let group_id = args.next_u64()?;
    let rule_id = args.next_u64()?;

    let mut exclude_alerts = false;
    if let Some(arg) = args.peek() {
        let arg = arg.to_string_lossy();
        if arg.eq_ignore_ascii_case("EXCLUDE_ALERTS") {
            exclude_alerts = true;
        }
    }

    with_rule_group(ctx, group_id, |group| {
        if let Some(rule) = group.get_rule_by_id(rule_id) {
            return Ok(rule_to_api(group, rule, exclude_alerts));
        }
        Err(ValkeyError::Str("ERR: rule not found"))
    })?
}
