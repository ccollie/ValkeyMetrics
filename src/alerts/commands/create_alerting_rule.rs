use crate::alerts::rule::{AlertingRule, MetricRule};
use crate::alerts::utils::with_group_mut;
use crate::module::arg_parse::{
    parse_duration, 
    parse_key_value_pairs, 
    parse_promql_expr,
    CommandArgIterator,
    CMD_ARG_EXPR,
    CMD_ARG_LABELS,
    CMD_ARG_NAME
};
use metricsql_parser::parser::is_valid_identifier;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};

const CMD_ARG_EVAL_INTERVAL: &'static str = "EVAL_INTERVAL";
const CMD_ARG_ALERT_FOR: &'static str = "ALERT_FOR";
const CMD_ARG_ANNOTATIONS: &'static str = "ANNOTATIONS";
const CMD_ARG_KEEP_FIRING_FOR: &'static str = "KEEP_FIRING_FOR";


pub fn create_alerting_rule(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let group_key = args.next_arg()?;

    with_group_mut(ctx, &group_key, move |group| {
        let rule = parse_alerting_rule_config(args)?;
        if group.contains_rule(&rule.name) {
            return Err(ValkeyError::Str("Err rule already exists"));
        }
        group.rules.push(MetricRule::AlertingRule(rule));
        
        // todo: Replicate
        VALKEY_OK
    })
}

fn parse_alerting_rule_config(mut args: CommandArgIterator) -> ValkeyResult<AlertingRule> {
    fn is_cmd_token(token: &str) -> bool {
        const TOKENS: [&str; 6] = [
            CMD_ARG_NAME,
            CMD_ARG_EXPR,
            CMD_ARG_LABELS,
            CMD_ARG_EVAL_INTERVAL,
            CMD_ARG_ALERT_FOR,
            CMD_ARG_ANNOTATIONS,
        ];
        TOKENS.contains(&token)
    }
    
    let mut rule = AlertingRule::default();
    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_NAME) => {
                let name = args.next_string()?;
                if !is_valid_identifier(&name) {
                    return Err(ValkeyError::Str("Err invalid rule name"));
                }
                rule.name = name; // todo:
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_EVAL_INTERVAL) => {
                rule.eval_interval = parse_duration(args.next_str()?)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_EXPR) => {
                rule.expr = parse_promql_expr(&mut args)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_LABELS) => {
                rule.labels = parse_key_value_pairs(&mut args, is_cmd_token)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_ALERT_FOR) => {
                rule.r#for = parse_duration(args.next_str()?)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_ANNOTATIONS) => {
                rule.annotations = parse_key_value_pairs(&mut args, is_cmd_token)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_KEEP_FIRING_FOR) => {
                rule.keep_firing_for = parse_duration(args.next_str()?)?;
            }
            _ => {
                return Err(ValkeyError::Str("ERR invalid argument"))
            }
        }
    }
    
    Ok(rule)
}