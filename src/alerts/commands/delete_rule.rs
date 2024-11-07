use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};
use valkey_module_macros::command;
use crate::alerts::rules::{Group, MetricRule};
use crate::alerts::utils::with_group_mut;

/// VM.DELETE-RULE groupKey ruleName
#[command(
    {
        name: "VM.DELETE-RULE",
        flags: [Write],
        arity: 3,
        key_spec: [
            {
                notes: "Delete a rules",
                flags: [Delete],
                begin_search: Index({ index : 1 }),
                find_keys: Range({ last_key : 0, steps : 1, limit : 0 }),
            }
        ]
    }
)]
pub fn delete_rule(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    if args.len()!= 2 {
        return Err(ValkeyError::WrongArity);
    }
    let group_key = args.next_arg()?;
    let rule_name = args.next_str()?;
    
    with_group_mut(ctx, &group_key, |group| {
        if !handle_delete(ctx, group, rule_name, false) {
            return Err(ValkeyError::Str("Err rules does not exist"));
        }
        VALKEY_OK
    })
}

fn handle_delete(ctx: &Context, group: &mut Group, name: &str, remove_dest: bool) -> bool {
    // if it's a recording rules and remove_dest is true, we also remove the key
    if remove_dest {
        if let Some(MetricRule::RecordingRule(rr)) = group.get_rule_by_name(name) {
            if !rr.dest_key.is_empty() {
                let _ = ctx.call("DEL", &[&rr.dest_key]);
            }
        }
    }
    // todo: emit event, replicate
    
    group.remove_rule(name)
}