use crate::common::types::TimestampRangeValue;
use crate::common::{current_time_millis, duration_to_chrono, parse_duration};
use crate::config::get_global_settings;
use crate::globals::get_query_context;
use crate::module::result::to_matrix_result;
use crate::module::{normalize_range_args, parse_timestamp_arg};
use metricsql_engine::execution::query::{
    query as engine_query, query_range as engine_query_range,
};
use metricsql_engine::prelude::query::QueryParams;
use metricsql_engine::{QueryResult, RuntimeResult};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString};

const CMD_ARG_FORMAT: &str = "FORMAT";
const CMD_ARG_START: &str = "START";
const CMD_ARG_END: &str = "END";
const CMD_ARG_TIME: &str = "TIME";
const CMD_ARG_STEP: &str = "STEP";
const CMD_ARG_ROUNDING: &str = "ROUNDING";

/// const CMD_ARG_CACHE: &str = "CACHE";

///
/// PROM.QUERY_RANGE <query>
///     [START rfc3339 | unix_timestamp | + | - | * ]
///     [END rfc3339 | unix_timestamp | + | - | * ]
///     [STEP duration]
///     [ROUNDING digits]
///
pub(crate) fn prom_query_range(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let query = args.next_string()?;
    let mut start_value: Option<TimestampRangeValue> = None;
    let mut end_value: Option<TimestampRangeValue> = None;
    let mut step_value: Option<chrono::Duration> = None;

    let config = get_global_settings();
    let mut round_digits: u8 = config.round_digits.unwrap_or(100);

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_START) => {
                let next = args.next_str()?;
                start_value = Some(parse_timestamp_arg(ctx, &next, "START")?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_END) => {
                let next = args.next_str()?;
                end_value = Some(parse_timestamp_arg(ctx, &next, "END")?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_STEP) => {
                let next = args.next_str()?;
                step_value = Some(parse_step(next)?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_ROUNDING) => {
                round_digits = args.next_u64()?.max(100) as u8;
            }
            _ => {
                let msg = format!("ERR invalid argument '{}'", arg);
                return Err(RedisError::String(msg));
            }
        };
    }

    let (start, end) = normalize_range_args(start_value, end_value)?;

    let step = normalize_step(step_value)?;

    let mut query_params: QueryParams = get_default_query_params();
    query_params.query = query.to_string();
    query_params.start = start;
    query_params.end = end;
    query_params.step = step;
    query_params.round_digits = round_digits;

    let query_context = get_query_context();
    handle_query_result(engine_query_range(query_context, &query_params))
}

///
/// PROM.QUERY <query>
///         [TIME rfc3339 | unix_timestamp | * | + ]
///         [TIMEOUT duration]
///         [ROUNDING digits]
///
pub fn prom_query(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let query = args.next_string()?;
    let mut time_value: Option<TimestampRangeValue> = None;

    let config = get_global_settings();
    let mut round_digits: u8 = config.round_digits.unwrap_or(100);

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_TIME) => {
                let next = args.next_str()?;
                time_value = Some(parse_timestamp_arg(ctx, &next, "TIME")?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_ROUNDING) => {
                round_digits = args.next_u64()?.max(100) as u8;
            }
            _ => {
                let msg = format!("ERR invalid argument '{}'", arg);
                return Err(RedisError::String(msg));
            }
        };
    }

    let start = if let Some(val) = time_value {
        val.to_timestamp()
    } else {
        current_time_millis()
    };

    let mut query_params: QueryParams = get_default_query_params();
    query_params.query = query.to_string();
    query_params.start = start;
    query_params.end = start;
    query_params.round_digits = round_digits;

    let query_context = get_query_context();
    handle_query_result(engine_query(query_context, &query_params))
}

fn parse_step(arg: &str) -> RedisResult<chrono::Duration> {
    return if let Ok(duration) = parse_duration(arg) {
        Ok(duration_to_chrono(duration))
    } else {
        Err(RedisError::Str("ERR invalid STEP duration"))
    };
}

fn normalize_step(step: Option<chrono::Duration>) -> RedisResult<chrono::Duration> {
    let config = get_global_settings();
    if let Some(val) = step {
        Ok(val)
    } else {
        chrono::Duration::from_std(config.default_step)
            .map_err(|_| RedisError::Str("ERR invalid STEP duration"))
    }
}

fn get_default_query_params() -> QueryParams {
    let config = get_global_settings();
    let mut result = QueryParams::default();
    if let Some(rounding) = config.round_digits {
        result.round_digits = rounding;
    }
    result
}

fn handle_query_result(result: RuntimeResult<Vec<QueryResult>>) -> RedisResult {
    match result {
        Ok(result) => Ok(to_matrix_result(result)),
        Err(e) => {
            let err_msg = format!("PROM: Error: {:?}", e);
            return Err(RedisError::String(err_msg.to_string()));
        }
    }
}
