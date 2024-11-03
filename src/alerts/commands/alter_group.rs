use crate::alerts::utils::with_group_mut;
use crate::error_consts;
use crate::module::arg_parse::*;
use ahash::AHashMap;
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

const INTERVAL: &str = "INTERVAL";
const EVAL_OFFSET: &str = "EVAL_OFFSET";
const EVAL_DELAY: &str = "EVAL_DELAY";
const EVAL_ALIGNMENT: &str = "EVAL_ALIGNMENT";


#[derive(Default)]
struct AlterGroupOptions {
    name: Option<String>,
    eval_offset: Option<Duration>,
    eval_delay: Option<Duration>,
    eval_alignment: Option<bool>,
    disabled: Option<bool>,
    labels: Option<AHashMap<String, String>>,
    interval: Option<Duration>,
    limit: Option<usize>,
}

/// Alter a Group
///
/// VM.ALTER-RULE-GROUP groupKey name
///   [INTERVAL interval]
///   [EVAL_OFFSET evalOffset]
///   [EVAL_DELAY evalDelay]
///   [EVAL_ALIGNMENT isAligned]
///   [LIMIT limit]
///   [LABELS name value ...]
///   [DISABLED true|false]
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
                    return Err(ValkeyError::Str("Err invalid rule name"));
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
        if let Some(eval_offset) = options.eval_offset {
            if group.eval_offset != eval_offset {
                group.eval_offset = eval_offset;
                changed = true;
            }
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
        if let Some(interval) = options.interval {
            group.interval = interval;
        }
        if let Some(limit) = options.limit {
            if group.limit!= limit {
                group.limit = limit;
                changed = true;
            }
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