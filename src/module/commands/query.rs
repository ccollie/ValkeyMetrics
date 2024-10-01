use crate::common::{current_time_millis, duration_to_chrono};
use crate::config::get_global_settings;
use crate::globals::get_query_context;
use crate::module::result::to_matrix_result;
use crate::module::{normalize_range_args, parse_timestamp_arg};
use metricsql_runtime::execution::query::{
    query as engine_query, query_range as engine_query_range,
};
use metricsql_runtime::prelude::query::QueryParams;
use metricsql_runtime::{QueryResult, RuntimeResult};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString};
use crate::module::arg_parse::{parse_duration_arg};
use crate::module::types::TimestampRangeValue;

const CMD_ARG_START: &str = "START";
const CMD_ARG_END: &str = "END";
const CMD_ARG_TIME: &str = "TIME";
const CMD_ARG_STEP: &str = "STEP";
const CMD_ARG_ROUNDING: &str = "ROUNDING";


///
/// VM.QUERY-RANGE <query>
///     [START rfc3339 | unix_timestamp | + | - | * ]
///     [END rfc3339 | unix_timestamp | + | - | * ]
///     [STEP duration]
///     [ROUNDING digits]
///
pub(crate) fn query_range(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
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
                start_value = Some(parse_timestamp_arg(next, "START")?);
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_END) => {
                let next = args.next_str()?;
                end_value = Some(parse_timestamp_arg(next, "END")?);
            }
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
/// VKM.QUERY <query>
///         [TIME rfc3339 | unix_timestamp | * | + ]
///         [TIMEOUT duration]
///         [ROUNDING digits]
///
pub fn query(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    let query = args.next_string()?;
    let mut time_value: Option<TimestampRangeValue> = None;

    let config = get_global_settings();
    let mut round_digits: u8 = config.round_digits.unwrap_or(100);

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_TIME) => {
                let next = args.next_str()?;
                time_value = Some(parse_timestamp_arg(next, "TIME")?);
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

    let start = if let Some(val) = time_value {
        val.as_timestamp()
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

fn parse_step(arg: &ValkeyString) -> ValkeyResult<chrono::Duration> {
    if let Ok(duration) = parse_duration_arg(arg) {
        Ok(duration_to_chrono(duration))
    } else {
        Err(ValkeyError::Str("ERR invalid STEP duration"))
    }
}

fn normalize_step(step: Option<chrono::Duration>) -> ValkeyResult<chrono::Duration> {
    let config = get_global_settings();
    if let Some(val) = step {
        Ok(val)
    } else {
        chrono::Duration::from_std(config.default_step)
            .map_err(|_| ValkeyError::Str("ERR invalid STEP duration"))
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

fn handle_query_result(result: RuntimeResult<Vec<QueryResult>>) -> ValkeyResult {
    match result {
        Ok(result) => Ok(to_matrix_result(result)),
        Err(e) => {
            let err_msg = format!("ERR: {:?}", e);
            Err(ValkeyError::String(err_msg.to_string()))
        }
    }
}
