use std::collections::HashMap;
use crate::alerts::utils::with_group_mut;
use crate::error_consts;
use crate::module::arg_parse::*;
use metricsql_parser::parser::is_valid_identifier;
use std::time::Duration;
use valkey_module::{
    Context,
    NextArg,
    NotifyEvent,
    ValkeyError,
    ValkeyResult,
    ValkeyString,
    VALKEY_OK
};
use valkey_module_macros::command;
use crate::alerts::group_manager::GROUP_MANAGER;
use crate::error_consts::EVAL_OFFSET_EXCEEDS_INTERVAL;

const INTERVAL: &str = "INTERVAL";
const EVAL_OFFSET: &str = "EVAL_OFFSET";
const EVAL_DELAY: &str = "EVAL_DELAY";
const EVAL_ALIGNMENT: &str = "EVAL_ALIGNMENT";


#[derive(Default)]
pub struct AlterGroupOptions {
    name: Option<String>,
    eval_offset: Option<Duration>,
    eval_delay: Option<Duration>,
    eval_alignment: Option<bool>,
    disabled: Option<bool>,
    labels: Option<HashMap<String, String>>,
    interval: Option<Duration>,
    limit: Option<usize>,
}

/// Alter a Group
///
/// VM.ALTER-RULE-GROUP groupKey
///   [NAME groupName]
///   [INTERVAL interval]
///   [EVAL_OFFSET evalOffset]
///   [EVAL_DELAY evalDelay]
///   [EVAL_ALIGNMENT isAligned]
///   [LIMIT limit]
///   [LABELS name value ...]
///   [DISABLED true|false]
#[command(
    {
        name: "VM.ALTER-RULE-GROUP",
        flags: [Write],
        arity: -2,
        summary: "Updates a rules group",
        key_spec: [
            {
                flags: [Update, Access],
                begin_search: Index({ index : 1 }),
                find_keys: Range({ last_key : 0, steps : 1, limit : 0 }),
            }
        ]
    }
)]
pub fn alter_group_function(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let (parsed_key, options, changed) = parse_alter_options(args)?;

    if changed {
        update_group(ctx, &parsed_key, options)?;
    }
    
    VALKEY_OK
}

pub fn parse_alter_options(args: Vec<ValkeyString>) -> ValkeyResult<(ValkeyString, AlterGroupOptions, bool)> {
    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next().ok_or(ValkeyError::Str("Err missing key argument"))?;

    const CREATE_TOKENS: [&str; 8] = [
        EVAL_ALIGNMENT,
        EVAL_DELAY,
        EVAL_OFFSET,
        INTERVAL,
        CMD_ARG_LIMIT,
        CMD_ARG_LABELS,
        CMD_ARG_DISABLED,
        CMD_ARG_NAME
    ];

    fn is_command_keyword(arg: &str) -> bool {
        CREATE_TOKENS.contains(&arg)
    }
    
    let mut config = AlterGroupOptions::default();
    let mut changed = false;

    while let Ok(arg) = args.next_str() {
        let arg_upper = arg.to_ascii_uppercase();
        match arg_upper.as_str() {
            CMD_ARG_NAME => {
                let name = args.next_string()?;
                if !is_valid_identifier(&name) {
                    return Err(ValkeyError::Str("Err invalid group name"));
                }
                config.name = Some(name);
                changed = true;
            }
            EVAL_OFFSET => {
                config.eval_offset = Some(parse_duration(args.next_str()?)?);
                changed = true;
            }
            EVAL_DELAY => {
                config.eval_delay = Some(parse_duration(args.next_str()?)?);
                changed = true;
            }
            EVAL_ALIGNMENT => {
                let is_aligned = parse_boolean(args.next_str()?)?;
                config.eval_alignment = Some(is_aligned);
                changed = true;
            }
            CMD_ARG_LIMIT => {
                let value = args.next_u64()?;
                // TODO
                config.limit = Some(value as usize);
                changed = true;
            }
            CMD_ARG_LABELS => {
                config.labels = Some(parse_key_value_pairs(&mut args, is_command_keyword)?);
                changed = true;
            }
            CMD_ARG_DISABLED => {
                if let Some(value) = args.peek() {
                    let value = value.to_string_lossy();
                    config.disabled = Some(parse_boolean(&value)?);
                    args.next();
                } else {
                    config.disabled = Some(true);
                }
                changed = true;
            }
            _ => {
                return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
            }
        };
    }
    

    Ok((key, config, changed))
}


pub(crate) fn update_group(ctx: &Context, key: &ValkeyString, options: AlterGroupOptions) -> ValkeyResult<()> {
    
    let changed = with_group_mut(ctx, key, |group| {
        let mut changed = false;
        if let Some(name) = options.name {
            if group.name != name {
                group.name = name;
                changed = true;
            }
        }
        
        if options.eval_offset.is_some() || options.interval.is_some() {
            let offset = options.eval_offset.unwrap_or(group.eval_offset);
            let interval = options.interval.unwrap_or(group.interval);

            if !offset.is_zero() && !interval.is_zero() {
                // if `eval_offset` is set, interval won't use global evaluationInterval flag and
                // must be bigger than offset.
                if group.eval_offset > group.interval {
                    return Err(ValkeyError::Str(EVAL_OFFSET_EXCEEDS_INTERVAL));
                }
            }
        }
        
        if let Some(eval_offset) = options.eval_offset {
            if group.eval_offset != eval_offset {
                group.eval_offset = eval_offset;
                changed = true;
            }
        }
        
        if let Some(interval) = options.interval {
            group.interval = interval;
        }
        if options.eval_delay.is_some() && group.eval_delay != options.eval_delay {
            group.eval_delay = options.eval_delay;
            changed = true;
        }
        if options.eval_alignment.is_some() && group.eval_alignment != options.eval_alignment {
            group.eval_alignment = options.eval_alignment;
            changed = true;
        }
        if let Some(disabled) = options.disabled {
            if group.disabled != disabled {
                group.disabled = disabled;
                changed = true;
            }
        }
        if let Some(labels) = options.labels {
            if group.labels!= labels {
                group.labels = labels;
                changed = true;
            }
        }
        if let Some(limit) = options.limit {
            if group.limit!= limit {
                group.limit = limit;
                changed = true;
            }
        }
        
        if changed {
            GROUP_MANAGER.update_group(ctx, group, key);
        }
        
        Ok(changed)
    })?;

    if changed {
        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "VM.ALTER-RULE-GROUP", key);
        ctx.log_verbose("group updated");
    }

    Ok(())
}