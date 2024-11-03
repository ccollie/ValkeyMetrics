use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};
use crate::alerts::rule::Group;
use crate::alerts::utils::with_group_mut;

/// VM.DELETE_RULE groupKey ruleName
pub fn delete_rule(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    if args.len()!= 2 {
        return Err(ValkeyError::WrongArity);
    }
    let group_key = args.next_arg()?;
    let rule_name = args.next_str()?;
    
    with_group_mut(ctx, &group_key, |group| {
        if !handle_delete(ctx, group, rule_name, false) {
            return Err(ValkeyError::Str("Err rule does not exist"));
        }
        VALKEY_OK
    })
}

fn handle_delete(ctx: &Context, group: &mut Group, name: &str, remove_dest: bool) -> bool {
    // if it's a recording rule and remove_dest is true, we also remove the key
    if remove_dest {
        if let Some(rule) = group.recording_rules.iter().find(|r| r.name == name) {
            if !rule.dest_key.is_empty() {
                let _ = ctx.call("DEL", &[&rule.dest_key]);
            }
        }
    }
    
    group.remove_rule(name)
}