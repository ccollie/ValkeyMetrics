use crate::aggregators::Aggregator;
use crate::common::get_current_time_millis;
use crate::common::rounding::{RoundingStrategy, MAX_DECIMAL_DIGITS, MAX_SIGNIFICANT_DIGITS};
use crate::common::types::{Label, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::join::join_reducer::JoinReducer;
use crate::series::types::*;
use crate::series::{ChunkCompression, DuplicatePolicy, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use crate::series::{TimestampRange, TimestampValue};
use metricsql_parser::common::{Value, ValueType};
use metricsql_parser::{
    parse as parse_expr,
    parse_duration_value,
    parse_metric_name as parse_metric,
    parse_metric_selector,
    parse_number,
    parse_timestamp as parse_timestamp_internal
};
use metricsql_parser::prelude::Matchers;
use std::collections::{BTreeSet, HashMap};
use std::iter::{Peekable, Skip};
use std::time::Duration;
use std::vec::IntoIter;
use valkey_module::{NextArg, ValkeyError, ValkeyResult, ValkeyString};

const MAX_TS_VALUES_FILTER: usize = 16;
const CMD_ARG_AGGREGATION: &str = "AGGREGATION";
const CMD_ARG_ALERT_FOR: &str = "ALERT_FOR";
const CMD_ARG_ALIGN: &str = "ALIGN";
const CMD_ARG_ANNOTATIONS: &str = "ANNOTATIONS";
const CMD_ARG_ASOF: &str = "ASOF";
const CMD_ARG_BUCKET_TIMESTAMP: &str = "BUCKETTIMESTAMP";
const CMD_ARG_CHUNK_SIZE: &str = "CHUNK_SIZE";
const CMD_ARG_COMPRESSION: &str = "COMPRESSION";
const CMD_ARG_COUNT: &str = "COUNT";
const CMD_ARG_DECIMAL_DIGITS: &str = "DECIMAL_DIGITS";
const CMD_ARG_DEDUPE_INTERVAL: &str = "DEDUPE_INTERVAL";
const CMD_ARG_DISABLED: &str = "DISABLED";
const CMD_ARG_DUPLICATE_POLICY: &str = "DUPLICATE_POLICY";
const CMD_ARG_EMPTY: &str = "EMPTY";
const CMD_ARG_END: &str = "END";
const CMD_ARG_EXCLUSIVE: &str = "EXCLUSIVE";
const CMD_ARG_EXPR: &str = "EXPR";
const CMD_ARG_FILTER: &str = "FILTER";
const CMD_ARG_FILTER_BY_TS: &str = "FILTER_BY_TS";
const CMD_ARG_FILTER_BY_VALUE: &str = "FILTER_BY_VALUE";
const CMD_ARG_FOR: &str = "FOR";
const CMD_ARG_FULL: &str = "FULL";
const CMD_ARG_GROUP_BY: &str = "GROUPBY";
const CMD_ARG_INNER: &str = "INNER";
const CMD_ARG_INTERVAL: &str = "INTERVAL";
const CMD_ARG_EVAL_OFFSET: &str = "EVAL_OFFSET";
const CMD_ARG_EVAL_DELAY: &str = "EVAL_DELAY";
const CMD_ARG_EVAL_ALIGNMENT: &str = "EVAL_ALIGNMENT";
const CMD_ARG_EVAL_INTERVAL: &str = "EVAL_INTERVAL";
const CMD_ARG_EXCLUDE_ALERTS: &str = "EXCLUDE_ALERTS";
const CMD_ARG_LABELS: &str = "LABELS";
const CMD_ARG_LEFT: &str = "LEFT";
const CMD_ARG_KEEP_FIRING_FOR: &str = "KEEP_FIRING_FOR";
const CMD_ARG_LIMIT: &str = "LIMIT";
const CMD_ARG_MATCH: &str = "MATCH";
const CMD_ARG_MAX_ENTRIES: &str = "MAX_ENTRIES";
const CMD_ARG_METRIC: &str = "METRIC";
const CMD_ARG_NAME: &str = "NAME";
const MAX_DATAPOINTS: &str = "MAX_DATAPOINTS";
const CMD_ARG_NEXT: &str = "NEXT";
const CMD_ARG_PRIOR: &str = "PRIOR";
const CMD_ARG_REDUCE: &str = "REDUCE";
const CMD_ARG_RETENTION: &str = "RETENTION";
const CMD_ARG_RIGHT: &str = "RIGHT";
const CMD_ARG_ROUNDING: &str = "ROUNDING";
const CMD_ARG_RULE_GROUP: &str = "RULE_GROUP";
const CMD_ARG_RULE_NAME: &str = "RULE_NAME";
const CMD_ARG_RULE_TYPE: &str = "RULE_TYPE";
const RULES_DELAY: &str = "RULES_DELAY";
const RULES_RETRIES: &str = "RULES_RETRIES";
const CMD_ARG_SELECTED_LABELS: &str = "SELECTED_LABELS";
const CMD_ARG_STEP: &str = "STEP";
const CMD_ARG_SIGNIFICANT_DIGITS: &str = "SIGNIFICANT_DIGITS";
const CMD_ARG_START: &str = "START";
const CMD_ARG_WITH_LABELS: &str = "WITHLABELS";

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum CommandArgToken {
    AsOf,
    Aggregation,
    AlertFor,
    Align,
    Annotations,
    BucketTimestamp,
    ChunkSize,
    Compression,
    Count,
    DecimalDigits,
    DedupeInterval,
    Disabled,
    DuplicatePolicy,
    Empty,
    End,
    EvalAlignment,
    EvalDelay,
    EvalInterval,
    EvalOffset,
    Expr,
    ExcludeAlerts,
    Exclusive,
    Filter,
    FilterByTs,
    FilterByValue,
    For,
    Full,
    GroupBy,
    Inner,
    Interval,
    KeepFiringFor,
    Labels,
    Left,
    Limit,
    Match,
    MaxEntries,
    MaxDataPoints,
    Metric,
    Name,
    Next,
    Prior,
    Reduce,
    Retention,
    Right,
    Rounding,
    RuleGroup,
    RuleName,
    RuleType,
    RulesDelay,
    RulesRetries,
    SelectedLabels,
    SignificantDigits,
    Start,
    Step,
    WithLabels,
    #[default]
    Invalid
}

impl CommandArgToken {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            CommandArgToken::AsOf => CMD_ARG_ASOF,
            CommandArgToken::Aggregation => CMD_ARG_AGGREGATION,
            CommandArgToken::AlertFor => CMD_ARG_ALERT_FOR,
            CommandArgToken::Align => CMD_ARG_ALIGN,
            CommandArgToken::Annotations => CMD_ARG_ANNOTATIONS,
            CommandArgToken::BucketTimestamp => CMD_ARG_BUCKET_TIMESTAMP,
            CommandArgToken::ChunkSize => CMD_ARG_CHUNK_SIZE,
            CommandArgToken::Compression => CMD_ARG_COMPRESSION,
            CommandArgToken::Count => CMD_ARG_COUNT,
            CommandArgToken::DecimalDigits => CMD_ARG_DECIMAL_DIGITS,
            CommandArgToken::DedupeInterval => CMD_ARG_DEDUPE_INTERVAL,
            CommandArgToken::Disabled => CMD_ARG_DISABLED,
            CommandArgToken::DuplicatePolicy => CMD_ARG_DUPLICATE_POLICY,
            CommandArgToken::Empty => CMD_ARG_EMPTY,
            CommandArgToken::End => CMD_ARG_END,
            CommandArgToken::EvalAlignment => CMD_ARG_EVAL_ALIGNMENT,
            CommandArgToken::EvalDelay => CMD_ARG_EVAL_DELAY,
            CommandArgToken::EvalInterval => CMD_ARG_EVAL_INTERVAL,
            CommandArgToken::EvalOffset => CMD_ARG_EVAL_OFFSET,
            CommandArgToken::Expr => CMD_ARG_EXPR,
            CommandArgToken::ExcludeAlerts => CMD_ARG_EXCLUDE_ALERTS,
            CommandArgToken::Exclusive => CMD_ARG_EXCLUSIVE,
            CommandArgToken::Filter => CMD_ARG_FILTER,
            CommandArgToken::FilterByTs => CMD_ARG_FILTER_BY_TS,
            CommandArgToken::FilterByValue => CMD_ARG_FILTER_BY_VALUE,
            CommandArgToken::For => CMD_ARG_FOR,
            CommandArgToken::Full => CMD_ARG_FULL,
            CommandArgToken::GroupBy => CMD_ARG_GROUP_BY,
            CommandArgToken::Inner => CMD_ARG_INNER,
            CommandArgToken::Interval => CMD_ARG_INTERVAL,
            CommandArgToken::KeepFiringFor => CMD_ARG_KEEP_FIRING_FOR,
            CommandArgToken::Labels => CMD_ARG_LABELS,
            CommandArgToken::Left => CMD_ARG_LEFT,
            CommandArgToken::Limit => CMD_ARG_LIMIT,
            CommandArgToken::Match => CMD_ARG_MATCH,
            CommandArgToken::MaxDataPoints => MAX_DATAPOINTS,
            CommandArgToken::MaxEntries => CMD_ARG_MAX_ENTRIES,
            CommandArgToken::Metric => CMD_ARG_METRIC,
            CommandArgToken::Name => CMD_ARG_NAME,
            CommandArgToken::Next => CMD_ARG_NEXT,
            CommandArgToken::Prior => CMD_ARG_PRIOR,
            CommandArgToken::Reduce => CMD_ARG_REDUCE,
            CommandArgToken::Retention => CMD_ARG_RETENTION,
            CommandArgToken::Right => CMD_ARG_RIGHT,
            CommandArgToken::Rounding => CMD_ARG_ROUNDING,
            CommandArgToken::RuleGroup => CMD_ARG_RULE_GROUP,
            CommandArgToken::RuleName => CMD_ARG_RULE_NAME,
            CommandArgToken::RuleType => CMD_ARG_RULE_TYPE,
            CommandArgToken::RulesDelay => RULES_DELAY,
            CommandArgToken::RulesRetries => RULES_RETRIES,
            CommandArgToken::SelectedLabels => CMD_ARG_SELECTED_LABELS,
            CommandArgToken::SignificantDigits => CMD_ARG_SIGNIFICANT_DIGITS,
            CommandArgToken::Start => CMD_ARG_START,
            CommandArgToken::Step => CMD_ARG_STEP,
            CommandArgToken::WithLabels => CMD_ARG_WITH_LABELS,
            CommandArgToken::Invalid => "INVALID COMMAND ARG",
        }
    }
}

pub(crate) fn parse_command_arg_token(arg: &[u8]) -> Option<CommandArgToken> {
    hashify::tiny_map_ignore_case! {
        arg,
        "ASOF" => CommandArgToken::AsOf,
        "AGGREGATION" => CommandArgToken::Aggregation,
        "ALERT_FOR" => CommandArgToken::AlertFor,
        "ALIGN" => CommandArgToken::Align,
        "ANNOTATIONS" => CommandArgToken::Annotations,
        "BUCKET_TIMESTAMP" => CommandArgToken::BucketTimestamp,
        "CHUNK_SIZE" => CommandArgToken::ChunkSize,
        "COMPRESSION" => CommandArgToken::Compression,
        "COUNT" => CommandArgToken::Count,
        "DECIMAL_DIGITS" => CommandArgToken::DecimalDigits,
        "DEDUPE_INTERVAL" => CommandArgToken::DedupeInterval,
        "DISABLED" => CommandArgToken::Disabled,
        "DUPLICATE_POLICY" => CommandArgToken::DuplicatePolicy,
        "EMPTY" => CommandArgToken::Empty,
        "END" => CommandArgToken::End,
        "EVAL_ALIGNMENT" => CommandArgToken::EvalAlignment,
        "EVAL_DELAY" => CommandArgToken::EvalDelay,
        "EVAL_INTERVAL" => CommandArgToken::EvalInterval,
        "EVAL_OFFSET" => CommandArgToken::EvalOffset,
        "EXCLUDE_ALERTS" => CommandArgToken::ExcludeAlerts,
        "EXCLUSIVE" => CommandArgToken::Exclusive,
        "EXPR" => CommandArgToken::Expr,
        "FILTER" => CommandArgToken::Filter,
        "FILTER_BY_TS" => CommandArgToken::FilterByTs,
        "FILTER_BY_VALUE" => CommandArgToken::FilterByValue,
        "FOR" => CommandArgToken::For,
        "FULL" => CommandArgToken::Full,
        "GROUP_BY" => CommandArgToken::GroupBy,
        "INNER" => CommandArgToken::Inner,
        "INTERVAL" => CommandArgToken::Interval,
        "KEEP_FIRING_FOR" => CommandArgToken::KeepFiringFor,
        "LABELS" => CommandArgToken::Labels,
        "LEFT" => CommandArgToken::Left,
        "LIMIT" => CommandArgToken::Limit,
        "MATCH" => CommandArgToken::Match,
        "MAX_DATAPOINTS" => CommandArgToken::MaxDataPoints,
        "MAX_ENTRIES" => CommandArgToken::MaxEntries,
        "METRIC" => CommandArgToken::Metric,
        "NAME" => CommandArgToken::Name,
        "NEXT" => CommandArgToken::Next,
        "PRIOR" => CommandArgToken::Prior,
        "REDUCE" => CommandArgToken::Reduce,
        "RETENTION" => CommandArgToken::Retention,
        "RIGHT" => CommandArgToken::Right,
        "ROUNDING" => CommandArgToken::Rounding,
        "RULE_GROUP" => CommandArgToken::RuleGroup,
        "RULE_NAME" => CommandArgToken::RuleName,
        "RULE_TYPE" => CommandArgToken::RuleType,
        "RULES_DELAY" => CommandArgToken::RulesDelay,
        "RULES_RETRIES" => CommandArgToken::RulesRetries,
        "SELECTED_LABELS" => CommandArgToken::SelectedLabels,
        "SIGNIFICANT_DIGITS" => CommandArgToken::SignificantDigits,
        "START" => CommandArgToken::Start,
        "STEP" => CommandArgToken::Step,
        "WITHLABELS" => CommandArgToken::WithLabels,
    }
}

pub type CommandArgIterator = Peekable<Skip<IntoIter<ValkeyString>>>;

pub fn parse_number_arg(arg: &ValkeyString, name: &str) -> ValkeyResult<f64> {
    if let Ok(value) = arg.parse_float() {
        return Ok(value);
    }
    let arg_str = arg.to_string_lossy();
    parse_number_with_unit(&arg_str).map_err(|_| {
        let msg = format!("ERR invalid number parsing {name}");
        ValkeyError::String(msg)
    })
}

pub fn parse_integer_arg(
    arg: &ValkeyString,
    name: &str,
    allow_negative: bool,
) -> ValkeyResult<i64> {
    let value = if let Ok(val) = arg.parse_integer() {
        val
    } else {
        let num = parse_number_arg(arg, name)?;
        if num != num.floor() {
            return Err(ValkeyError::Str(error_consts::INVALID_INTEGER));
        }
        if num > i64::MAX as f64 {
            return Err(ValkeyError::Str("ERR: value is too large"));
        }
        num as i64
    };
    if !allow_negative && value < 0 {
        let msg = format!("ERR: {} must be a non-negative integer", name);
        return Err(ValkeyError::String(msg));
    }
    Ok(value)
}

pub fn parse_timestamp(arg: &str) -> ValkeyResult<Timestamp> {
    // todo: handle +,
    if arg == "*" {
        return Ok(get_current_time_millis());
    }
    parse_timestamp_internal(arg).map_err(|_| ValkeyError::Str(error_consts::INVALID_TIMESTAMP))
}

pub fn parse_timestamp_range_value(arg: &str) -> ValkeyResult<TimestampValue> {
    TimestampValue::try_from(arg)
}

pub fn parse_duration_arg(arg: &ValkeyString) -> ValkeyResult<Duration> {
    if let Ok(value) = arg.parse_integer() {
        if value < 0 {
            return Err(ValkeyError::Str(
                "ERR: invalid duration, must be a non-negative integer",
            ));
        }
        return Ok(Duration::from_millis(value as u64));
    }
    let value_str = arg.to_string_lossy();
    parse_duration(&value_str)
}

pub fn parse_duration(arg: &str) -> ValkeyResult<Duration> {
    parse_duration_ms(arg).map(|d| Duration::from_millis(d as u64))
}

pub fn parse_duration_ms(arg: &str) -> ValkeyResult<i64> {
    parse_duration_value(arg, 1).map_err(|_| ValkeyError::Str(error_consts::INVALID_DURATION))
}

pub fn parse_number_with_unit(arg: &str) -> TsdbResult<f64> {
    parse_number(arg).map_err(|_e| TsdbError::InvalidNumber(arg.to_string()))
}

pub fn parse_boolean(arg: &str) -> ValkeyResult<bool> {
    match arg {
        arg if arg.eq_ignore_ascii_case("true") => Ok(true),
        arg if arg.eq_ignore_ascii_case("false") => Ok(false),
        "1" => Ok(true),
        "0" => Ok(false),
        _ => Err(ValkeyError::Str("ERR: invalid boolean value")),
    }
}

pub fn parse_series_selector(arg: &str) -> TsdbResult<Matchers> {
    parse_metric_selector(arg).map_err(|_e| TsdbError::InvalidSeriesSelector(arg.to_string()))
}

pub fn parse_metric_name(arg: &str) -> TsdbResult<Vec<Label>> {
    parse_metric(arg).map_err(|_e| TsdbError::InvalidMetric(arg.to_string()))
}

pub fn parse_operator(arg: &str) -> ValkeyResult<JoinReducer> {
    JoinReducer::try_from(arg)
}

pub fn parse_chunk_size(args: &mut CommandArgIterator) -> ValkeyResult<usize> {
    let arg = args.next_str()?;
    fn get_error_result() -> ValkeyResult<usize> {
        let msg = format!("TSDB: CHUNK_SIZE value must be an integer multiple of 2 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]");
        Err(ValkeyError::String(msg))
    }

    let chunk_size = parse_number_with_unit(arg)
        .map_err(|_e| ValkeyError::Str(error_consts::INVALID_CHUNK_SIZE))?;

    if chunk_size != chunk_size.floor() {
        return get_error_result();
    }
    if chunk_size < MIN_CHUNK_SIZE as f64 || chunk_size > MAX_CHUNK_SIZE as f64 {
        return get_error_result();
    }
    let chunk_size = chunk_size as usize;
    if chunk_size % 2 != 0 {
        return get_error_result();
    }
    Ok(chunk_size)
}

pub fn parse_chunk_compression(args: &mut CommandArgIterator) -> ValkeyResult<ChunkCompression> {
    args.next_str().and_then(|next| {
        ChunkCompression::try_from(next)
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_CHUNK_COMPRESSION))
    })
}

pub fn parse_duplicate_policy(args: &mut CommandArgIterator) -> ValkeyResult<DuplicatePolicy> {
    args.next_str().and_then(|next| {
        DuplicatePolicy::try_from(next)
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY))
    })
}

pub fn parse_timestamp_range(args: &mut CommandArgIterator) -> ValkeyResult<TimestampRange> {
    let first_arg = args.next_str()?;
    let start = parse_timestamp_range_value(first_arg)?;
    let end_value = if let Ok(arg) = args.next_str() {
        parse_timestamp_range_value(arg)
            .map_err(|_e| ValkeyError::Str("ERR invalid end timestamp"))?
    } else {
        TimestampValue::Latest
    };
    TimestampRange::new(start, end_value)
}

pub fn parse_retention(args: &mut CommandArgIterator) -> ValkeyResult<Duration> {
    if let Ok(next) = args.next_str() {
        parse_duration(next).map_err(|_e| ValkeyError::Str(error_consts::INVALID_DURATION))
    } else {
        Err(ValkeyError::Str("ERR missing RETENTION value"))
    }
}

pub fn parse_timestamp_filter(
    args: &mut CommandArgIterator,
    is_valid_arg: fn(CommandArgToken) -> bool,
) -> ValkeyResult<Vec<Timestamp>> {
    // FILTER_BY_TS already seen
    let mut values: Vec<Timestamp> = Vec::new();
    loop {
        if is_token_or_end(args, is_valid_arg) {
            break;
        }
        let arg = args.next_str()?;
        if let Ok(timestamp) = parse_timestamp(arg) {
            values.push(timestamp);
        } else {
            return Err(ValkeyError::Str(error_consts::INVALID_TIMESTAMP));
        }
        if values.len() == MAX_TS_VALUES_FILTER {
            break;
        }
    }
    if values.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: FILTER_BY_TS one or more arguments are missing",
        ));
    }
    values.sort();
    values.dedup();
    Ok(values)
}

pub fn parse_value_filter(args: &mut CommandArgIterator) -> ValkeyResult<ValueFilter> {
    let min = parse_number_with_unit(args.next_str()?)
        .map_err(|_| ValkeyError::Str("ERR cannot parse filter min parameter"))?;
    let max = parse_number_with_unit(args.next_str()?)
        .map_err(|_| ValkeyError::Str("ERR cannot parse filter max parameter"))?;
    if max < min {
        return Err(ValkeyError::Str(
            "ERR filter min parameter is greater than max",
        ));
    }
    ValueFilter::new(min, max)
}

pub fn parse_count(args: &mut CommandArgIterator) -> ValkeyResult<usize> {
    let next = args.next_arg()?;
    let count = parse_integer_arg(&next, CMD_ARG_COUNT, false)
        .map_err(|_| ValkeyError::Str(error_consts::NEGATIVE_COUNT))?;
    if count > usize::MAX as i64 {
        return Err(ValkeyError::Str("ERR COUNT value is too large"));
    }
    Ok(count as usize)
}

pub(crate) fn advance_if_next_token(args: &mut CommandArgIterator, token: CommandArgToken) -> bool {
    if let Some(next) = args.peek() {
        if let Some(tok) = parse_command_arg_token(next.as_slice()) {
            if tok == token {
                args.next();
                return true;
            }
        }
    }
    false
}

pub(crate) fn advance_if_next_token_one_of(
    args: &mut CommandArgIterator,
    tokens: &[CommandArgToken],
) -> Option<CommandArgToken> {
    if let Some(next) = args.peek() {
        if let Some(token) = parse_command_arg_token(next.as_slice()) {
            if tokens.contains(&token) {
                args.next();
                return Some(token)
            }
        }
    } 
    None
}

pub(crate) fn expect_one_of(args: &mut CommandArgIterator, tokens: &[CommandArgToken]) -> ValkeyResult<CommandArgToken> {
    if let Some(next) = args.next() {
        if let Some(token) = parse_command_arg_token(next.as_slice()) {
            if tokens.contains(&token) {
                return Ok(token)
            }
        }
    }
    let msg = format!("ERR: expected one of: {:?}", tokens);
    Err(ValkeyError::String(msg))
}

fn is_token_or_end(args: &mut CommandArgIterator, is_cmd_token: fn(CommandArgToken) -> bool) -> bool {
    if let Some(next) = args.peek() {
        match parse_command_arg_token(next.as_slice()) {
            Some(token) => {
                args.next();
                is_cmd_token(token)
            }
            None => false,
        }
    } else {
        false
    }
}

pub fn parse_label_list(
    args: &mut CommandArgIterator,
    is_cmd_token: fn(CommandArgToken) -> bool,
) -> ValkeyResult<Vec<String>> {
    let mut labels: BTreeSet<String> = BTreeSet::new();

    loop {
        if is_token_or_end(args, is_cmd_token) {
            break;
        }
        let label = args.next_str()?;
        if labels.contains(label) {
            let msg = format!("ERR: duplicate label: {label}");
            return Err(ValkeyError::String(msg));
        }
        labels.insert(label.to_string());
    }

    let temp = labels.into_iter().collect();
    Ok(temp)
}

pub fn parse_key_value_pairs(
    args: &mut CommandArgIterator,
    is_cmd_token: fn(CommandArgToken) -> bool,
) -> ValkeyResult<HashMap<String, String>> {
    let mut labels: HashMap<String, String> = HashMap::new();

    loop {
        let label = args.next_string()?;

        if label.is_empty() {
            return Err(ValkeyError::Str("ERR invalid label key"));
        }

        if labels.contains_key(&label) {
            let msg = format!("ERR: duplicate label: {label}");
            return Err(ValkeyError::String(msg));
        }

        // todo: regex validation

        let value = args
            .next_string()
            .map_err(|_| ValkeyError::Str("ERR invalid label value"))?;

        labels.insert(label, value);

        if is_token_or_end(args, is_cmd_token) {
            break;
        }
    }

    Ok(labels)
}

pub fn parse_dedupe_interval(args: &mut CommandArgIterator) -> ValkeyResult<Duration> {
    let next = args.next_arg()?;
    parse_duration_arg(&next).map_err(|_e| ValkeyError::Str("ERR invalid DEDUPE_INTERVAL value"))
}

pub fn parse_series_selector_list(
    args: &mut CommandArgIterator,
    is_cmd_token: fn(CommandArgToken) -> bool,
) -> ValkeyResult<Vec<Matchers>> {
    let mut matchers = vec![];

    while let Some(next) = args.peek() {
        if let Some(token) = parse_command_arg_token(next.as_slice()) {
            if is_cmd_token(token) {
                break;
            }
        } else {
            return Err(ValkeyError::Str("ERR: Invalid series selector"));
        }
        let arg = next.try_as_str()?;

        if let Ok(selector) = parse_series_selector(arg) {
            matchers.push(selector);
        } else {
            return Err(ValkeyError::Str(error_consts::INVALID_SERIES_SELECTOR));
        }
    }

    Ok(matchers)
}

pub fn parse_aggregation_options(
    args: &mut CommandArgIterator,
) -> ValkeyResult<AggregationOptions> {
    // AGGREGATION token already seen
    let agg_str = args
        .next_str()
        .map_err(|_e| ValkeyError::Str("ERR: Error parsing AGGREGATION"))?;
    let aggregator = Aggregator::try_from(agg_str)?;
    let bucket_duration = parse_duration_arg(&args.next_arg()?)
        .map_err(|_e| ValkeyError::Str("Error parsing bucketDuration"))?;

    let mut aggr: AggregationOptions = AggregationOptions {
        aggregator,
        bucket_duration,
        timestamp_output: BucketTimestamp::Start,
        alignment: RangeAlignment::default(),
        time_delta: 0,
        empty: false,
    };

    let mut arg_count: usize = 0;

    let valid_tokens = [
        CommandArgToken::Align,
        CommandArgToken::Empty,
        CommandArgToken::BucketTimestamp
    ];

    while let Some(token) = advance_if_next_token_one_of(args, &valid_tokens) {
        match token {
            CommandArgToken::Empty => {
                aggr.empty = true;
                arg_count += 1;
            }
            CommandArgToken::BucketTimestamp => {
                let next = args.next_str()?;
                arg_count += 1;
                aggr.timestamp_output = BucketTimestamp::try_from(next)?;
            }
            CommandArgToken::Align => {
                let next = args.next_str()?;
                aggr.alignment = parse_alignment(next)?;
            }
            _ => break,
        }
        if arg_count == 3 {
            break;
        }
    }

    Ok(aggr)
}

fn parse_alignment(align: &str) -> ValkeyResult<RangeAlignment> {
    let alignment = match align {
        arg if arg.eq_ignore_ascii_case("start") => RangeAlignment::Start,
        arg if arg.eq_ignore_ascii_case("end") => RangeAlignment::End,
        arg if arg.len() == 1 => {
            let c = arg.chars().next().unwrap();
            match c {
                '-' => RangeAlignment::Start,
                '+' => RangeAlignment::End,
                _ => return Err(ValkeyError::Str(error_consts::INVALID_ALIGN)),
            }
        }
        _ => {
            let timestamp = parse_timestamp(align)
                .map_err(|_| ValkeyError::Str(error_consts::INVALID_ALIGN))?;
            RangeAlignment::Timestamp(timestamp)
        }
    };
    Ok(alignment)
}

pub fn parse_grouping_params(args: &mut CommandArgIterator) -> ValkeyResult<RangeGroupingOptions> {
    // GROUPBY token already seen
    let label = args.next_str()?;
    let token = args
        .next_str()
        .map_err(|_| ValkeyError::Str("ERR: missing REDUCE"))?;
    if !token.eq_ignore_ascii_case(CMD_ARG_REDUCE) {
        let msg = format!("ERR: expected \"{CMD_ARG_REDUCE}\", found \"{token}\"");
        return Err(ValkeyError::String(msg));
    }
    let agg_str = args
        .next_str()
        .map_err(|_e| ValkeyError::Str("ERR: Error parsing grouping reducer"))?;

    let aggregator = Aggregator::try_from(agg_str).map_err(|_| {
        let msg = format!("ERR: invalid grouping aggregator \"{}\"", agg_str);
        ValkeyError::String(msg)
    })?;

    Ok(RangeGroupingOptions {
        group_label: label.to_string(),
        aggregator,
    })
}

pub fn parse_significant_digit_rounding(
    args: &mut CommandArgIterator,
) -> ValkeyResult<RoundingStrategy> {
    let next = args.next_u64()?;
    if next > MAX_SIGNIFICANT_DIGITS as u64 {
        let msg = format!("ERR SIGNIFICANT_DIGITS must be between 0 and {MAX_SIGNIFICANT_DIGITS}");
        return Err(ValkeyError::String(msg));
    }
    Ok(RoundingStrategy::SignificantDigits(next as i32))
}

pub fn parse_decimal_digit_rounding(
    args: &mut CommandArgIterator,
) -> ValkeyResult<RoundingStrategy> {
    let next = args.next_u64()?;
    if next > MAX_DECIMAL_DIGITS as u64 {
        let msg = format!("ERR DECIMAL_DIGITS must be between 0 and {MAX_DECIMAL_DIGITS}");
        return Err(ValkeyError::String(msg));
    }
    Ok(RoundingStrategy::DecimalDigits(next as i32))
}

pub fn parse_promql_vector_expr(args: &mut CommandArgIterator) -> ValkeyResult<String> {
    const ERROR_MSG: &str = "ERR: invalid PromQL vector expression";

    let expr = args.next_string()?;
    match parse_expr(&expr) {
        Ok(candidate) => {
            if candidate.value_type() == ValueType::InstantVector {
                Ok(expr)
            } else {
                Err(ValkeyError::Str(ERROR_MSG))
            }
        }
        Err(_) => Err(ValkeyError::Str(ERROR_MSG)),
    }
}
