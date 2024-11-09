use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};
use valkey_module_macros::command;
use crate::alerts::rules::{Group, MetricRule};
use crate::alerts::utils::with_group_mut;

/// VM.DELETE-RULE groupKey ruleId
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
    let rule_id = args.next_u64()?;
    
    with_group_mut(ctx, &group_key, move |group| {
        if !handle_delete(ctx, group, rule_id, false) {
            return Err(ValkeyError::Str("Err rules does not exist"));
        }
        VALKEY_OK
    })
}

fn handle_delete(_ctx: &Context, group: &mut Group, id: u64, remove_dest: bool) -> bool {
    // if it's a recording rules and remove_dest is true, we also remove the key
    if remove_dest {
        if let Some(MetricRule::RecordingRule(_rr)) = group.get_rule_by_id(id) {
            // todo: remove keys
        }
    }
    // todo: emit event, replicate
    
    group.remove_rule_by_id(id)
}