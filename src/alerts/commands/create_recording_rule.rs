use crate::alerts::meta::with_group_mut;
use crate::alerts::notifications::validate_templates;
use crate::alerts::rules::{calc_rule_hash, MetricRule, RecordingRule, RuleState};
use crate::error_consts;
use crate::module::arg_parse::{
    parse_key_value_pairs, parse_promql_vector_expr, CommandArgIterator, CMD_ARG_EXPR,
    CMD_ARG_LABELS,
};
use metricsql_parser::prelude::is_valid_identifier;
use valkey_module::{
    Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK,
};
use valkey_module_macros::command;

const CMD_ARG_MAX_ENTRIES: &str = "MAX_ENTRIES";

/// VM.CREATE-RECORDING-RULE groupKey ruleName
///  EXPR expression
///  [LABELS label value ...]
///  [MAX_ENTRIES alertDuration]
#[command(
    {
        name: "VM.CREATE-RECORDING-RULE",
        flags: [Write],
        arity: -4,
        key_spec: [
            {
                notes: "Create a rules based on PromQL to precompute expressions and save their result as a new set of time series..",
                flags: [ReadWrite],
                begin_search: Index({ index : 1 }),
                find_keys: Range({ last_key : 1, steps : 0, limit : 1 }),
            }
        ]
    }
)]
pub fn create_recording_rule(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let group_key = args.next_arg()?;

    with_group_mut(ctx, &group_key.clone(), move |group| {
        let rule = parse_rule_config(args)?;
        let to_add = MetricRule::RecordingRule(rule);
        group
            .add_rule(to_add)
            .map_err(|_e| ValkeyError::Str(error_consts::ALERTS_DUPLICATE_RULE))?;

        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "VM.CREATE-RECORDING-RULE", &group_key);

        VALKEY_OK
    })
}

fn parse_rule_config(mut args: CommandArgIterator) -> ValkeyResult<RecordingRule> {
    fn is_cmd_token(token: &str) -> bool {
        const TOKENS: [&str; 3] = [CMD_ARG_EXPR, CMD_ARG_LABELS, CMD_ARG_MAX_ENTRIES];
        TOKENS.iter().any(|x| x.eq_ignore_ascii_case(token))
    }

    let name = args.next_string()?;
    if !is_valid_identifier(&name) {
        return Err(ValkeyError::Str("ERR invalid rules name"));
    }

    let mut rule = RecordingRule {
        name,
        ..Default::default()
    };

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_EXPR) => {
                rule.expr = parse_promql_vector_expr(&mut args)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_LABELS) => {
                let labels = parse_key_value_pairs(&mut args, is_cmd_token)?;
                validate_templates(&labels)
                    .map_err(|_err| ValkeyError::Str("ERR error parsing label templates"))?;
                rule.labels = labels;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_MAX_ENTRIES) => {
                let max_entries = args.next_u64()? as usize;
                // todo: limit
                rule.state = RuleState::with_capacity(max_entries);
            }
            _ => return Err(ValkeyError::Str("ERR invalid argument")),
        }
    }

    if rule.expr.is_empty() {
        return Err(ValkeyError::Str("ERR missing expression"));
    }

    rule.rule_id = calc_rule_hash(&rule).map_err(|_err| ValkeyError::Str("ERR hashing rule"))?;

    Ok(rule)
}
