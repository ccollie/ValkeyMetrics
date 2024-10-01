use crate::arg_parse::parse_timestamp;
use crate::module::with_timeseries_mut;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use crate::common::types::Timestamp;

pub fn madd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let arg_count = args.len() - 1;
    let mut args = args.into_iter().skip(1);

    if arg_count < 3 {
        return Err(ValkeyError::WrongArity);
    }

    if arg_count % 3 != 0 {
        return Err(ValkeyError::Str("ERR TSDB: wrong number of arguments for 'VM.MADD' command"));
    }

    let sample_count = arg_count / 3;

    let mut values: Vec<ValkeyValue> = Vec::with_capacity(sample_count);
    let mut inputs: Vec<(ValkeyString, Timestamp, f64)> = Vec::with_capacity(sample_count);

    while let Some(key) = args.next() {
        let timestamp = parse_timestamp(args.next_str()?)?;
        let value = args.next_f64()?;
        inputs.push((key, timestamp, value));
    }

    for (key, timestamp, value) in inputs {
        let value = with_timeseries_mut(ctx, &key, |series| {
            if series.add(timestamp, value, None).is_ok() {
                Ok(ValkeyValue::from(timestamp))
            } else {
                // todo !!!!!
                Ok(ValkeyValue::SimpleString("ERR".to_string()))
            }
        });
        match value {
            Ok(value) => values.push(value),
            Err(err) => values.push(
                ValkeyValue::SimpleString(format!("ERR TSDB: {}", err)),
            ),
        }
    }

    Ok(ValkeyValue::Array(values))

}
