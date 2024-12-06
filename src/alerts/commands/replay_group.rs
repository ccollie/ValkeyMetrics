use crate::alerts::datasource::{AlertDatasource, WriteQueue};
use crate::alerts::meta::get_group_manager_for_db;
use crate::alerts::replay::{replay, ReplayOptions};
use crate::alerts::rules::{merge_hashes, Group, MetricRule, Rule};
use crate::alerts::GROUP_MANAGERS;
use crate::common::get_current_db;
use crate::error_consts;
use crate::module::arg_parse::*;
use crate::module::group_data_type::VKM_RULE_GROUP;
use std::sync::Arc;
use std::thread;
use valkey_module::{
    logging, Context, NextArg, NotifyEvent, ThreadSafeContext, ValkeyError, ValkeyResult,
    ValkeyString, ValkeyValue,
};
use valkey_module_macros::command;

const RULES_DELAY: &str = "RULES_DELAY";
const RULES_RETRIES: &str = "RULES_RETRIES";
const MAX_DATAPOINTS: &str = "MAX_DATAPOINTS";
const LABELS: &str = "LABELS";

struct ParsedOptions {
    group: Group,
    options: ReplayOptions,
    key_buf: Vec<u8>,
    data_source: AlertDatasource,
    write_queue: Arc<WriteQueue>,
}

/// Replay the rules of a group
///
/// VM.REPLAY-GROUP groupKey from to
///   [RULES_DELAY evalDelay]
///   [MAX_DATAPOINTS maxDataPoints]
///   [RULE_RETRIES ruleRetries]
///   [LABELS label value ...]
#[command(
    {
        name: "VM.REPLAY-GROUP",
        flags: [Write],
        arity: -4,
        summary: "Backfill alerting and recording rules against the current db by replaying the rules in a group",
        key_spec: [
            {
                flags: [ReadWrite],
                begin_search: Index({ index : 1 }),
                find_keys: Range({ last_key : 1, steps : 0, limit : 1 }),
            }
        ]
    }
)]
pub fn replay_group_function(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let blocked_client = ctx.block_client();
    let mut options = parse_replay_options(ctx, args)?;

    // todo: run on a thread from rayon thread pool
    thread::spawn(move || {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);

        let res = replay(
            &options.data_source,
            &mut options.group,
            &options.options,
            &options.write_queue,
        );
        match res {
            Ok(val) => {
                let ctx = thread_ctx.lock();
                let key = ctx.create_string(options.key_buf);
                ctx.replicate_verbatim();
                ctx.notify_keyspace_event(NotifyEvent::MODULE, "VM.REPLAY-GROUP", &key);
                ctx.log_verbose("group replayed");
                thread_ctx.reply(Ok(val.into()));
            }
            Err(e) => {
                logging::log_warning(format!("group replay failed: {:?}", e));
                thread_ctx.reply(Err(ValkeyError::Str("ERR: group replay failed")));
            }
        }
    });

    Ok(ValkeyValue::NoReply)
}

fn parse_replay_options(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult<ParsedOptions> {
    let mut args = args.into_iter().skip(1).peekable();

    let mut options = ReplayOptions::default();

    let key = args
        .next()
        .ok_or(ValkeyError::Str("Err missing key argument"))?;
    let date_range = parse_timestamp_range(&mut args)?;
    let (start, end) = date_range.get_timestamps();
    options.from = start;
    options.to = end;

    const TOKENS: [&str; 4] = [RULES_DELAY, MAX_DATAPOINTS, RULES_RETRIES, LABELS];
    fn is_cmd_token(token: &str) -> bool {
        TOKENS.contains(&token)
    }

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(RULES_DELAY) => {
                options.rules_delay = parse_duration(args.next_str()?)?;
            }
            arg if arg.eq_ignore_ascii_case(MAX_DATAPOINTS) => {
                options.max_data_points = args.next_u64()? as usize;
                if options.max_data_points < 1 {
                    return Err(ValkeyError::Str(
                        "replay.max_data_points can't be lower than 1",
                    ));
                }
            }
            arg if arg.eq_ignore_ascii_case(RULES_RETRIES) => {
                options.rule_retry_attempts = args.next_u64()? as usize;
            }
            arg if arg.eq_ignore_ascii_case(LABELS) => {
                let labels = parse_key_value_pairs(&mut args, is_cmd_token)?;
                options.extra_labels = labels;
            }
            _ => {
                return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
            }
        };
    }

    let db = get_current_db(ctx);

    let mut cloned_group = ctx
        .open_key(&key)
        .get_value::<Group>(&VKM_RULE_GROUP)?
        .ok_or(ValkeyError::Str(error_consts::GROUP_NOT_FOUND))?
        .clone();

    if !options.extra_labels.is_empty() {
        for rule in cloned_group.rules.iter_mut() {
            let name = rule.name().to_string();
            match rule {
                MetricRule::AlertingRule(ar) => {
                    merge_hashes(
                        &cloned_group.name,
                        &name,
                        &mut ar.labels,
                        &options.extra_labels,
                    );
                }
                MetricRule::RecordingRule(rr) => {
                    merge_hashes(
                        &cloned_group.name,
                        &name,
                        &mut rr.labels,
                        &options.extra_labels,
                    );
                }
            }
        }
    }

    let guard = GROUP_MANAGERS.guard();
    let manager = get_group_manager_for_db(ctx, db, &guard);
    let res = manager.with_group_meta(cloned_group.id, |group_meta| {
        let data_source = group_meta.executor.querier;
        let write_queue = manager.write_queue.clone();
        (data_source, write_queue)
    })?;

    let (data_source, write_queue) = res;
    Ok(ParsedOptions {
        group: cloned_group,
        options,
        key_buf: key.to_vec(),
        data_source,
        write_queue,
    })
}
