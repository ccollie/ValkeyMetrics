use metricsql_parser::parser::is_valid_identifier;
use crate::alerts::rule::RecordingRule;
use crate::alerts::utils::with_group_mut;
use crate::module::arg_parse::{
    parse_key_value_pairs,
    parse_promql_expr, 
    CommandArgIterator,
    CMD_ARG_EXPR, 
    CMD_ARG_LABELS, 
    CMD_ARG_NAME
};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};

const CMD_ARG_MAX_ENTRIES: &'static str = "MAX_ENTRIES";
const CMD_ARG_RECORD: &'static str = "RECORD";
const CMD_ARG_SELECTED_LABELS: &'static str = "SELECTED_LABELS";
const CMD_ARG_KEY: &'static str = "KEY";


pub fn create_recording_rule(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let group_key = args.next_arg()?;

    with_group_mut(ctx, &group_key, move |group| {
        let rule = parse_rule_config(args)?;
        if group.contains_rule(&rule.name) {
            return Err(ValkeyError::Str("Err rule already exists"));
        }
        group.recording_rules.push(rule);
        VALKEY_OK
    })
    
}

fn parse_rule_config(mut args: CommandArgIterator) -> ValkeyResult<RecordingRule> {
    fn is_cmd_token(token: &str) -> bool {
        const TOKENS: [&str; 6] = [
            CMD_ARG_NAME,
            CMD_ARG_KEY,
            CMD_ARG_EXPR,
            CMD_ARG_LABELS,
            CMD_ARG_MAX_ENTRIES,
            CMD_ARG_RECORD,
        ];
        TOKENS.contains(&token)
    }
    
    let mut rule = RecordingRule::default();
    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_NAME) => {
                let name = args.next_string()?;
                if !is_valid_identifier(&name) {
                    return Err(ValkeyError::Str("Err invalid rule name"));
                }
                rule.name = name; // todo:
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_KEY) => {
                let key = args.next_string()?;
                // todo: validate
                rule.key = key;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_EXPR) => {
                rule.expr = parse_promql_expr(&mut args)?;
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
    
    Ok(rule)
}