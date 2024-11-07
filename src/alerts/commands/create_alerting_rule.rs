use crate::alerts::notifications::validate_templates;
use crate::alerts::rules::{AlertingRule, MetricRule};
use crate::alerts::utils::with_group_mut;
use crate::module::arg_parse::{
    parse_duration,
    parse_key_value_pairs,
    parse_promql_expr,
    CommandArgIterator,
    CMD_ARG_ANNOTATIONS,
    CMD_ARG_EXPR,
    CMD_ARG_LABELS,
};
use metricsql_parser::parser::is_valid_identifier;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};
use valkey_module_macros::command;

const CMD_ARG_ALERT_FOR: &str = "FOR"; // todo: rename to THRESHOLD
const CMD_ARG_KEEP_FIRING_FOR: &str = "KEEP_FIRING_FOR";
const CMD_ARG_EVAL_INTERVAL: &str = "EVAL_INTERVAL";


/// VM.CREATE-ALERTING-RULE groupKey ruleName
///  EXPR expression
///  [LABELS label value ...]
///  [ANNOTATIONS label value ...]
///  [ALERT_FOR thresholdDuration]
///  [KEEP_FIRING_FOR alertDuration]
#[command(
    {
        name: "VM.CREATE-ALERTING-RULE",
        flags: [Write],
        arity: -4,
        key_spec: [
            {
                notes: "Create an Alerting rules to define alert conditions based on PromQL expressions and to send notifications about firing alerts through PUBSUB or streams.",
                flags: [Insert, Access],
                begin_search: Index({ index : 1 }),
                find_keys: Range({ last_key : 0, steps : 1, limit : 0 }),
            }
        ]
    }
)]
pub fn create_alerting_rule(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let group_key = args.next_arg()?;

    with_group_mut(ctx, &group_key, move |group| {
        let rule = parse_alerting_rule_config(args)?;
        if group.contains_rule(&rule.name) {
            return Err(ValkeyError::Str("Err rules already exists"));
        }
        group.rules.push(MetricRule::AlertingRule(rule));
        
        // todo: Replicate
        VALKEY_OK
    })
}

fn parse_alerting_rule_config(mut args: CommandArgIterator) -> ValkeyResult<AlertingRule> {
    fn is_cmd_token(token: &str) -> bool {
        const TOKENS: [&str; 6] = [
            CMD_ARG_EXPR,
            CMD_ARG_LABELS,
            CMD_ARG_ALERT_FOR,
            CMD_ARG_KEEP_FIRING_FOR,
            CMD_ARG_ANNOTATIONS,
            CMD_ARG_EVAL_INTERVAL,
        ];
        TOKENS.contains(&token)
    }

    let name = args.next_string()?;
    
    if name.is_empty() {
        return Err(ValkeyError::Str("ERR missing rules name"));
    }
    if !is_valid_identifier(&name) {
        return Err(ValkeyError::Str("ERR invalid rules name"));
    }

    let mut rule = AlertingRule {
        name,
       ..Default::default()
    };

    while let Ok(arg) = args.next_str() {
        match arg {
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
                let annotations = parse_key_value_pairs(&mut args, is_cmd_token)?;
                validate_templates(&annotations)
                    .map_err(|_err| ValkeyError::Str("ERR error parsing annotations"))?;
                rule.annotations = annotations;
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