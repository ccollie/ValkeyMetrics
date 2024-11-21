use crate::alerts::group_data_type::VKM_RULE_GROUP;
use crate::alerts::rules::{validate_offset_and_interval, Group, GroupConfig};
use crate::alerts::meta::with_group_manager;
use crate::error_consts;
use crate::module::arg_parse::*;
use metricsql_parser::parser::is_valid_identifier;
use std::time::Duration;
use valkey_module::key::ValkeyKeyWritable;
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

const INTERVAL: &str = "INTERVAL";
const EVAL_OFFSET: &str = "EVAL_OFFSET";
const EVAL_DELAY: &str = "EVAL_DELAY";
const EVAL_ALIGNMENT: &str = "EVAL_ALIGNMENT";


/// Create a new Group
///
/// VM.CREATE-RULE-GROUP groupKey groupName
///   [INTERVAL interval]
///   [EVAL_OFFSET evalOffset]
///   [EVAL_DELAY evalDelay]
///   [EVAL_ALIGNMENT isAligned]
///   [LIMIT limit]
///   [LABELS name value ...]
#[command(
    {
        name: "VM.CREATE-RULE-GROUP",
        flags: [Write],
        arity: -3,
        summary: "Create a new rules group",
        key_spec: [
            {
                flags: [ReadWrite],
                begin_search: Index({ index : 1 }),
                find_keys: Range({ last_key : 1, steps : 0, limit : 1 }),
            }
        ]
    }
)]
pub fn create_group_function(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let (parsed_key, options) = parse_create_options(args)?;

    create_group(ctx, &parsed_key, options)?;

    VALKEY_OK
}

pub fn parse_create_options(args: Vec<ValkeyString>) -> ValkeyResult<(ValkeyString, GroupConfig)> {
    let mut args = args.into_iter().skip(1).peekable();

    let mut options = GroupConfig::default();

    let key = args.next().ok_or(ValkeyError::Str("Err missing key argument"))?;
    let name = args.next_string()?;
    
    if !is_valid_identifier(&name) {
        return Err(ValkeyError::Str("ERR invalid group name"));
    }

    if name.is_empty() {
        return Err(ValkeyError::Str("ERR missing group name"));
    }
    options.name = name;
    
    const CREATE_TOKENS: [&str; 6] = [
        EVAL_ALIGNMENT,
        EVAL_DELAY,
        EVAL_OFFSET,
        INTERVAL,
        CMD_ARG_LIMIT,
        CMD_ARG_LABELS
    ];

    fn is_command_keyword(arg: &str) -> bool {
        CREATE_TOKENS.contains(&arg)
    }

    while let Ok(arg) = args.next_str() {
        let arg_upper = arg.to_ascii_uppercase();
        match arg_upper.as_str() {
            EVAL_OFFSET => {
                options.eval_offset = Some(parse_duration(args.next_str()?)?);
            }
            EVAL_DELAY => {
                options.eval_delay = Some(parse_duration(args.next_str()?)?);
            }
            EVAL_ALIGNMENT => {
                let is_aligned = parse_boolean(args.next_str()?)?;
                options.eval_alignment = Some(is_aligned);
            }
            CMD_ARG_LIMIT => {
                let value = args.next_u64()?;
                // TODO
                options.limit = value as usize;
            }
            CMD_ARG_LABELS => {
                options.labels = parse_key_value_pairs(&mut args, is_command_keyword)?;
            }
            _ => {
                return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
            }
        };
    }

    validate_offset_and_interval(options.eval_offset, options.eval_delay)?;
    
    Ok((key, options))
}


pub(crate) fn create_group(ctx: &Context, key: &ValkeyString, options: GroupConfig) -> ValkeyResult<()> {
    let _key = ValkeyKeyWritable::open(ctx.ctx, key);
    // check if this refers to an existing series
    if !_key.is_empty() {
        return Err(ValkeyError::Str("ERR: the key already exists"));
    }
    let mut group = Group::from_config(options, Duration::from_millis(0), vec![]);
    _key.set_value(&VKM_RULE_GROUP, group.clone())?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "VM.CREATE-RULE-GROUP", key);
    ctx.log_verbose("group created");

    // todo: handle errors
    let _ = with_group_manager(ctx, |manager| manager.add_group(ctx, &mut group, key));
    
    Ok(())
}