use crate::alerts::meta::with_group_mut;
use crate::alerts::notifications::validate_templates;
use crate::alerts::rules::{
    calc_rule_hash, validate_alert_expr, AlertingRule, MetricRule, RuleState,
};
use crate::error_consts;
use crate::module::arg_parse::{
    parse_duration, parse_key_value_pairs, CommandArgIterator, CMD_ARG_ANNOTATIONS, CMD_ARG_EXPR,
    CMD_ARG_LABELS,
};
use metricsql_parser::parser::is_valid_identifier;
use valkey_module::{Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};
use valkey_module_macros::command;

const CMD_ARG_ALERT_FOR: &str = "FOR"; // todo: rename to THRESHOLD
const CMD_ARG_KEEP_FIRING_FOR: &str = "KEEP_FIRING_FOR";
const CMD_ARG_EVAL_INTERVAL: &str = "EVAL_INTERVAL";
const CMD_ARG_MAX_ENTRIES: &str = "MAX_ENTRIES";

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
        summary: "Create an Alerting rules to define alert conditions based on PromQL expressions and to send notifications about firing alerts through PUBSUB or streams.",
        key_spec: [
            {
                flags: [ReadWrite],
                begin_search: Index({ index : 1 }),
                find_keys: Range({ last_key : 1, steps : 0, limit : 1 }),
            }
        ]
    }
)]
pub fn create_alerting_rule(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let group_key = args.next_arg()?;

    with_group_mut(ctx, &group_key.clone(), move |group| {
        let rule = parse_alerting_rule_config(args)?;

        let to_add = MetricRule::AlertingRule(Box::new(rule));
        group
            .add_rule(to_add)
            .map_err(|_e| ValkeyError::Str(error_consts::ALERTS_DUPLICATE_RULE))?;

        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "VM.CREATE-ALERTING-RULE", &group_key);

        VALKEY_OK
    })
}

fn parse_alerting_rule_config(mut args: CommandArgIterator) -> ValkeyResult<AlertingRule> {
    fn is_cmd_token(token: &str) -> bool {
        const TOKENS: [&str; 7] = [
            CMD_ARG_EXPR,
            CMD_ARG_LABELS,
            CMD_ARG_ALERT_FOR,
            CMD_ARG_KEEP_FIRING_FOR,
            CMD_ARG_ANNOTATIONS,
            CMD_ARG_EVAL_INTERVAL,
            CMD_ARG_MAX_ENTRIES,
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
                let expr = args.next_string()?;
                validate_alert_expr(&expr)?;
                rule.expr = expr;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_LABELS) => {
                let labels = parse_key_value_pairs(&mut args, is_cmd_token)?;
                validate_templates(&labels)
                    .map_err(|_err| ValkeyError::Str("ERR error parsing label templates"))?;
                rule.labels = labels;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_ALERT_FOR) => {
                rule.r#for = parse_duration(args.next_str()?)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_ANNOTATIONS) => {
                let annotations = parse_key_value_pairs(&mut args, is_cmd_token)?;
                validate_templates(&annotations)
                    .map_err(|_err| ValkeyError::Str("ERR error parsing annotation templates"))?;
                rule.annotations = annotations;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_KEEP_FIRING_FOR) => {
                rule.keep_firing_for = parse_duration(args.next_str()?)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_MAX_ENTRIES) => {
                let max_entries = args.next_u64()? as u16;
                // todo: limit
                rule.state = RuleState::with_capacity(max_entries as usize);
            }
            _ => return Err(ValkeyError::Str("ERR invalid argument")),
        }
    }

    rule.rule_id = calc_rule_hash(&rule).map_err(|_err| ValkeyError::Str("ERR hashing rule"))?;

    Ok(rule)
}
