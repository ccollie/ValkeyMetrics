use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};
use crate::alerts::notifier::{AlertNotifier, PubSubNotifier, StreamNotifier};
use crate::alerts::utils::with_group_mut;
use crate::module::arg_parse::{parse_boolean, CommandArgIterator};

const CMD_ARG_KEY: &'static str = "KEY";
const CMD_ARG_COMPACT: &'static str = "COMPACT";
const CMD_ARG_TYPE: &'static str = "TYPE";
const CMD_ARG_LIMIT: &'static str = "LIMIT";


/// VM.CREATE-ALERT-NOTIFIER groupKey [PUBSUB key|STREAM key]
fn add_group_notifier(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let group_key = args.next_arg()?;

    with_group_mut(ctx, &group_key, move |group| {
        let notifier = parse_notifier(&mut args)?;
        if group.contains_rule(&rule.name) {
            return Err(ValkeyError::Str("Err notification already exists"));
        }
        group.alerting_rules.push(rule);

        // todo: Replicate
        VALKEY_OK
    })
}

fn parse_notifier(args: &mut CommandArgIterator) -> ValkeyResult<AlertNotifier> {
    while let Ok(arg) = args.next_str() {
        match arg.as_str() {
            arg if arg.eq_ignore_ascii_case("TYPE") => {
                let notifier_type = args.next_arg()?;
                match notifier_type.as_str() {
                    "pubsub" => parse_pubsub(args),
                    "stream" => parse_stream_notifier(args),
                    _ => Err(ValkeyError::Str("ERR: Invalid notification type")),
                }
            }
        }
    }
    Ok(())
}

fn parse_pubsub(args: &mut CommandArgIterator) -> ValkeyResult<PubSubNotifier> {
    let key = if let Some(k) = args.next() {
        k  
    } else {
        return Err(ValkeyError::Str("ERR: Missing pubsub key"));
    };
    
    let mut notifier: PubSubNotifier = PubSubNotifier::new(key);
    
    while let Ok(arg) = args.next_str() {
        match arg {
            _ => {
                return Err(ValkeyError::Str("ERR: Invalid pubsub key"));
            }
        }
    }
    
    Ok(notifier)   
}

fn parse_stream_notifier(args: &mut CommandArgIterator) -> ValkeyResult<StreamNotifier> {
    let mut name: String = String::new();
    let mut compact: bool = false;
    let mut max_messages: Option<usize> = None;
    
    let key = if let Some(k) = args.next() {
      k  
    } else {
        return Err(ValkeyError::Str("ERR: Missing stream key"));
    };
    
    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_COMPACT) => {
                compact = true;
                // peek at next token to see if it's a truthy 
                if let Ok(next_arg) = args.peek_mut() {
                    if let Ok(bool_value) = parse_boolean(next_arg.as_str()) {
                        compact = bool_value;
                        args.next_arg()?; // consume the "true"
                    }
                }
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_LIMIT) => {
                max_messages = Some(args.next_u64()? as usize);
            }
            _ => {
                return Err(ValkeyError::Str("ERR: Invalid stream key"));
            }
        }
    }
    
    if name.is_empty() {
        return Err(ValkeyError::Str("ERR: Missing stream name"));
    }
    
    let mut notifier: StreamNotifier = StreamNotifier::new(key, max_messages);
    notifier.compact = compact;
    
    Ok(notifier)
}