use crate::alerts::meta::with_group_mut;
use crate::alerts::notifications::validate_templates;
use crate::alerts::rules::{
    calc_rule_hash, validate_alert_expr, AlertingRule, MetricRule, RuleState,
};
use crate::error_consts;
use crate::module::arg_parse::{
    parse_command_arg_token, 
    parse_duration, 
    parse_key_value_pairs,
    CommandArgIterator,
    CommandArgToken
};
use metricsql_parser::prelude::is_valid_identifier;
use valkey_module::{
    Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK,
};
use valkey_module_macros::command;


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
    fn is_cmd_token(token: CommandArgToken) -> bool {
        const TOKENS: [CommandArgToken; 7] = [
            CommandArgToken::Expr,
            CommandArgToken::Labels,
            CommandArgToken::AlertFor,
            CommandArgToken::KeepFiringFor,
            CommandArgToken::Annotations,
            CommandArgToken::EvalInterval,
            CommandArgToken::MaxEntries,
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

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::Annotations => {
                let annotations = parse_key_value_pairs(&mut args, is_cmd_token)?;
                validate_templates(&annotations)
                    .map_err(|_err| ValkeyError::Str("ERR error parsing annotation templates"))?;
                rule.annotations = annotations;
            }
            CommandArgToken::EvalInterval => {
                rule.eval_interval = parse_duration(args.next_str()?)?;
            }
            CommandArgToken::Expr => {
                let expr = args.next_string()?;
                validate_alert_expr(&expr)?;
                rule.expr = expr;
            }
            CommandArgToken::Labels => {
                let labels = parse_key_value_pairs(&mut args, is_cmd_token)?;
                validate_templates(&labels)
                    .map_err(|_err| ValkeyError::Str("ERR error parsing label templates"))?;
                rule.labels = labels;
            }
            CommandArgToken::AlertFor  => {
                rule.r#for = parse_duration(args.next_str()?)?;
            }
            CommandArgToken::KeepFiringFor => {
                rule.keep_firing_for = parse_duration(args.next_str()?)?;
            }
            CommandArgToken::MaxEntries => {
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
