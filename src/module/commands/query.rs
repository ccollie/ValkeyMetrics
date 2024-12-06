use crate::config::{QUERY_DEFAULT_STEP, QUERY_ROUND_DIGITS};
use crate::error_consts;
use crate::module::arg_parse::{parse_duration_arg, parse_timestamp_range};
use crate::module::parse_timestamp_arg;
use crate::module::result::{to_instant_vector_result, to_matrix_result};
use crate::query::{run_instant_query, run_range_query, QueryParams};
use std::thread;
use std::time::Duration;
use valkey_module::{
    Context, NextArg, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

const CMD_ARG_STEP: &str = "STEP";
const CMD_ARG_ROUNDING: &str = "ROUNDING";

///
/// VM.QUERY-RANGE fromTimestamp toTimestamp query
///     [STEP duration]
///     [ROUNDING digits]
///
pub(crate) fn query_range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let time_range = parse_timestamp_range(&mut args)?;

    let query = args.next_string()?;

    let mut step_value: Option<Duration> = None;

    let mut round_digits: u8 = QUERY_ROUND_DIGITS.unwrap_or(100);

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_STEP) => {
                let next = args.next_arg()?;
                step_value = Some(parse_step(&next)?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_ROUNDING) => {
                round_digits = args.next_u64()?.max(100) as u8;
            }
            _ => {
                let msg = format!("ERR invalid argument '{}'", arg);
                return Err(ValkeyError::String(msg));
            }
        };
    }

    let (start, end) = time_range.get_timestamps();

    let step = normalize_step(step_value);

    let mut query_params: QueryParams = get_default_query_params();
    query_params.query = query.to_string();
    query_params.start = start;
    query_params.end = end;
    query_params.step = step;
    query_params.round_digits = round_digits;

    // queries take indeterminate time. We should not block the main thread
    let blocked_client = ctx.block_client();

    // todo: run on a thread from rayon thread pool
    thread::spawn(move || {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        match run_range_query(&query_params) {
            Ok(result) => {
                let ctx = thread_ctx.lock();
                ctx.reply(ValkeyResult::from(to_matrix_result(result)));
            }
            Err(e) => {
                let ctx = thread_ctx.lock();
                ctx.reply(Err(e));
            }
        }
    });

    Ok(ValkeyValue::NoReply)
}

///
/// VKM.QUERY timestamp query
///         [TIMEOUT duration]
///         [ROUNDING digits]
///
/// Execute an instant query
pub fn query(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);

    let ts_arg = args.next_str()?;
    let time_value = parse_timestamp_arg(ts_arg, "timestamp")?;

    let query = args.next_string()?;

    let mut round_digits: u8 = QUERY_ROUND_DIGITS.unwrap_or(100);

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_ROUNDING) => {
                round_digits = args.next_u64()?.max(100) as u8;
            }
            _ => {
                return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
            }
        };
    }

    let start = time_value.as_timestamp();

    let mut query_params: QueryParams = get_default_query_params();
    query_params.query = query.to_string();
    query_params.start = start;
    query_params.end = start;
    query_params.round_digits = round_digits;

    let blocked_client = ctx.block_client();
    // todo: run on a thread from rayon thread pool
    thread::spawn(move || {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        match run_instant_query(&query_params) {
            Ok(result) => {
                let ctx = thread_ctx.lock();
                ctx.reply(ValkeyResult::from(to_instant_vector_result(result)));
            }
            Err(e) => {
                let ctx = thread_ctx.lock();
                ctx.reply(Err(e));
            }
        }
    });

    Ok(ValkeyValue::NoReply)
}

fn parse_step(arg: &ValkeyString) -> ValkeyResult<Duration> {
    parse_duration_arg(arg).map_err(|_| ValkeyError::Str(error_consts::INVALID_STEP_DURATION))
}

fn normalize_step(step: Option<Duration>) -> Duration {
    step.unwrap_or(*QUERY_DEFAULT_STEP)
}

fn get_default_query_params() -> QueryParams {
    let mut result = QueryParams::default();
    if let Some(rounding) = *QUERY_ROUND_DIGITS {
        result.round_digits = rounding;
    }
    result
}
