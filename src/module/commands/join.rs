use crate::join::asof::AsOfJoinStrategy;
use crate::join::{process_join, JoinOptions, JoinResultType, JoinType, JoinValue};
use crate::module::arg_parse::*;
use crate::module::result::sample_to_value;
use crate::module::{invalid_series_key_error, VKM_SERIES_TYPE};
use crate::series::time_series::TimeSeries;
use joinkit::EitherOrBoth;
use std::time::Duration;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};


/// VM.JOIN key1 key2 fromTimestamp toTimestamp
///   [[INNER] | [FULL] | [LEFT [EXCLUSIVE]] | [RIGHT [EXCLUSIVE]] | [ASOF [PRIOR | NEXT] tolerance]]
///   [FILTER_BY_TS ts...]
///   [FILTER_BY_VALUE min max]
///   [COUNT count]
///   [REDUCE op]
///   [AGGREGATION aggregator bucketDuration [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
pub fn join(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let left_key = args.next_arg()?;
    let right_key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;

    let mut options = JoinOptions {
        join_type: Default::default(),
        date_range,
        count: Default::default(),
        timestamp_filter: Default::default(),
        value_filter: Default::default(),
        reducer: None,
        aggregation: None,
    };

    parse_join_args(&mut args, &mut options)?;

    if left_key == right_key {
        return Err(ValkeyError::Str("VM: JOIN keys must be different"));
    }

    let left_db_key = ctx.open_key(&left_key);
    let right_db_key = ctx.open_key(&right_key);

    let left_series = left_db_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)?;
    let right_series = right_db_key.get_value::<TimeSeries>(&VKM_SERIES_TYPE)?;

    match (left_series, right_series) {
        (Some(left_series), Some(right_series)) => {
            Ok(join_internal(left_series, right_series, &options))
        }
        (Some(_), None) => Err(invalid_series_key_error(&right_key)),
        (None, Some(_)) => Err(invalid_series_key_error(&left_key)),
        _ => Err(ValkeyError::Str("VM: Invalid JOIN key")),
    }
}

fn parse_asof(args: &mut CommandArgIterator) -> ValkeyResult<JoinType> {
    use CommandArgToken::*;
    
    // ASOF already seen
    let mut tolerance = Duration::default();
    let mut direction = AsOfJoinStrategy::Prior;

    // ASOF [PRIOR | NEXT] [tolerance]
    if let Some(next) = advance_if_next_token_one_of(args, &[Prior, Next]) {
        if next == Prior {
            direction = AsOfJoinStrategy::Prior;
        } else if next == Next {
            direction = AsOfJoinStrategy::Next;
        }
    }

    if let Some(next_arg) = args.peek() {
        if let Ok(arg_str) = next_arg.try_as_str() {
            // see if we have a duration expression
            // durations in all cases start with an ascii digit, e.g. 1000 or 40ms
            let ch = arg_str.chars().next().unwrap();
            if ch.is_ascii_digit() {
                let tolerance_ms = parse_duration_ms(arg_str)?;
                if tolerance_ms < 0 {
                    return Err(ValkeyError::Str("ERR: negative ASOF tolerance not valid"));
                }
                tolerance = Duration::from_millis(tolerance_ms as u64);
                let _ = args.next_arg()?;
            }
        }
    }

    Ok(JoinType::AsOf(direction, tolerance))
}

fn possibly_parse_exclusive(args: &mut CommandArgIterator) -> bool {
    advance_if_next_token(args, CommandArgToken::Exclusive)
}

fn parse_join_args(args: &mut CommandArgIterator, options: &mut JoinOptions) -> ValkeyResult<()> {
    let mut join_type_set = false;

    fn check_join_type_set(is_set: &mut bool) -> ValkeyResult<()> {
        if *is_set {
            Err(ValkeyError::Str("ERR join type already set"))
        } else {
            *is_set = true;
            Ok(())
        }
    }

    fn is_arg_valid(arg: CommandArgToken) -> bool {
        use CommandArgToken::*;

        const VALID_TOKEN_ARGS: &[CommandArgToken] = &[
            AsOf,
            FilterByValue,
            FilterByTs,
            Count,
            Full,
            Left,
            Right,
            Inner,
            Reduce
        ];
        
        VALID_TOKEN_ARGS.contains(&arg)
    }

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::Aggregation => options.aggregation = Some(parse_aggregation_options(args)?),
            CommandArgToken::AsOf => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = parse_asof(args)?;
            }
            CommandArgToken::Count => {
                options.count = Some(parse_count(args)?);
            }
            CommandArgToken::FilterByValue => {
                options.value_filter = Some(parse_value_filter(args)?);
            }
            CommandArgToken::FilterByTs => {
                options.timestamp_filter = Some(parse_timestamp_filter(args, is_arg_valid)?);
            }
            CommandArgToken::Full => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = JoinType::Full;
            }
            CommandArgToken::Inner => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = JoinType::Inner;
            }
            CommandArgToken::Left => {
                check_join_type_set(&mut join_type_set)?;
                let exclusive = possibly_parse_exclusive(args);
                options.join_type = JoinType::Left(exclusive);
            }
            CommandArgToken::Right => {
                check_join_type_set(&mut join_type_set)?;
                let exclusive = possibly_parse_exclusive(args);
                options.join_type = JoinType::Left(exclusive);
            }
            CommandArgToken::Reduce => {
                let arg = args.next_str()?;
                options.reducer = Some(parse_operator(arg)?);
            }
            _ => return Err(ValkeyError::Str("ERR: invalid JOIN command argument")),
        }
    }

    // aggregations are only valid when there is a transform
    if options.aggregation.is_some() && options.reducer.is_none() {
        return Err(ValkeyError::Str("ERR: join aggregation requires a reducer"));
    }

    Ok(())
}

fn join_internal(left: &TimeSeries, right: &TimeSeries, options: &JoinOptions) -> ValkeyValue {
    let result = process_join(left, right, options);
    let arr = match result {
        JoinResultType::Samples(samples) => samples.into_iter().map(sample_to_value).collect(),
        JoinResultType::Values(values) => values
            .into_iter()
            .map(|x| join_value_to_valkey_value(x, false))
            .collect(),
    };

    ValkeyValue::Array(arr)
}

fn join_value_to_valkey_value(row: JoinValue, is_transform: bool) -> ValkeyValue {
    let timestamp = ValkeyValue::from(row.timestamp);

    match row.value {
        EitherOrBoth::Both(left, right) => {
            let r_value = ValkeyValue::from(right);
            let l_value = ValkeyValue::from(left);
            let res = if let Some(other_timestamp) = row.other_timestamp {
                vec![
                    timestamp,
                    ValkeyValue::from(other_timestamp),
                    l_value,
                    r_value,
                ]
            } else {
                vec![timestamp, l_value, r_value]
            };
            ValkeyValue::Array(res)
        }
        EitherOrBoth::Left(left) => {
            let value = ValkeyValue::from(left);
            if is_transform {
                ValkeyValue::Array(vec![timestamp, value])
            } else {
                ValkeyValue::Array(vec![timestamp, value, ValkeyValue::Null])
            }
        }
        EitherOrBoth::Right(right) => ValkeyValue::Array(vec![
            timestamp,
            ValkeyValue::Null,
            ValkeyValue::Float(right),
        ]),
    }
}

#[cfg(test)]
mod tests {}
