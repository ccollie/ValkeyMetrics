use crate::alerts::rules::{MetricRule, RecordingRule};
use crate::alerts::utils::with_group_mut;
use crate::module::arg_parse::{
    parse_key_value_pairs,
    parse_promql_vector_expr,
    CommandArgIterator,
    CMD_ARG_EXPR,
    CMD_ARG_LABELS,
};
use metricsql_parser::parser::is_valid_identifier;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};
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
                flags: [Insert, Access],
                begin_search: Index({ index : 1 }),
                find_keys: Range({ last_key : 0, steps : 1, limit : 0 }),
            }
        ]
    }
)]
pub fn create_recording_rule(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let group_key = args.next_arg()?;

    with_group_mut(ctx, &group_key, move |group| {
        let rule = parse_rule_config(args)?;
        if group.contains_rule(&rule.name) {
            return Err(ValkeyError::Str("Err rules already exists"));
        }
        group.rules.push(MetricRule::RecordingRule(rule));
        VALKEY_OK
    })
    
}

fn parse_rule_config(mut args: CommandArgIterator) -> ValkeyResult<RecordingRule> {
    fn is_cmd_token(token: &str) -> bool {
        const TOKENS: [&str; 3] = [
            CMD_ARG_EXPR,
            CMD_ARG_LABELS,
            CMD_ARG_MAX_ENTRIES,
        ];
        TOKENS.contains(&token)
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
                rule.labels = parse_key_value_pairs(&mut args, is_cmd_token)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_MAX_ENTRIES) => {
                let max_entries = args.next_u64()? as usize;
                rule.max_entries_limit = Some(max_entries);
            }
            _ => {
                return Err(ValkeyError::Str("ERR invalid argument"))
            }
        }
    }

    if rule.expr.is_empty() {
        return Err(ValkeyError::Str("ERR missing expression"));
    }
    
    Ok(rule)
}