use crate::common::duration_to_chrono;
use crate::config::get_global_settings;
use crate::globals::get_query_context;
use crate::module::arg_parse::{parse_duration_arg, parse_timestamp_range};
use crate::module::parse_timestamp_arg;
use crate::module::result::to_matrix_result;
use metricsql_runtime::execution::query::{
    query as engine_query, query_range as engine_query_range,
};
use metricsql_runtime::prelude::query::QueryParams;
use metricsql_runtime::{QueryResult, RuntimeResult};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString};

const CMD_ARG_STEP: &str = "STEP";
const CMD_ARG_ROUNDING: &str = "ROUNDING";


///
/// VM.QUERY-RANGE fromTimestamp toTimestamp query
///     [STEP duration]
///     [ROUNDING digits]
///
pub(crate) fn query_range(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let time_range = parse_timestamp_range(&mut args)?;

    let query = args.next_string()?;

    let mut step_value: Option<chrono::Duration> = None;

    let config = get_global_settings();
    let mut round_digits: u8 = config.round_digits.unwrap_or(100);

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
/// VKM.QUERY timestamp query
///         [TIMEOUT duration]
///         [ROUNDING digits]
///
pub fn query(_ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);

    let ts_arg = args.next_str()?;
    let time_value = parse_timestamp_arg(ts_arg, "timestamp")?;

    let query = args.next_string()?;

    let config = get_global_settings();
    let mut round_digits: u8 = config.round_digits.unwrap_or(100);

    while let Ok(arg) = args.next_str() {
        match arg {
            arg if arg.eq_ignore_ascii_case(CMD_ARG_ROUNDING) => {
                round_digits = args.next_u64()?.max(100) as u8;
            }
            _ => {
                let msg = format!("ERR invalid argument '{}'", arg);
                return Err(ValkeyError::String(msg));
            }
        };
    }

    let start = time_value.as_timestamp();

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
