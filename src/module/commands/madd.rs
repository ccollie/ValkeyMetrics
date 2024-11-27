use crate::arg_parse::parse_timestamp;
use crate::common::get_current_time_millis;
use crate::common::types::Sample;
use crate::common::types::Timestamp;
use crate::module::get_timeseries_mut;
use smallvec::SmallVec;
use std::collections::HashMap;
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

struct ParsedInput<'a> {
    key: &'a ValkeyString,
    raw_timestamp: &'a ValkeyString,
    raw_value: &'a ValkeyString,
    timestamp: Timestamp,
    value: f64,
}

pub fn madd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let arg_count = args.len() - 1;

    if arg_count < 3 {
        return Err(ValkeyError::WrongArity);
    }

    if arg_count % 3 != 0 {
        return Err(ValkeyError::WrongArity);
    }

    let sample_count = arg_count / 3;

    let current_ts = ctx.create_string(get_current_time_millis().to_string());

    let mut inputs: Vec<ParsedInput> = Vec::with_capacity(sample_count);

    let mut index: usize = 1;
    while index <= arg_count {
        let key = &args[index];
        let mut raw_timestamp = &args[index + 1];
        let raw_value = &args[index + 2];
        let timestamp_str = raw_timestamp.try_as_str()?;
        let timestamp = parse_timestamp(timestamp_str)?;
        let value = raw_value.parse_float()?;

        if timestamp_str == "*" {
            raw_timestamp = &current_ts;
        }

        inputs.push(ParsedInput {
            key,
            raw_timestamp,
            raw_value,
            timestamp,
            value,
        });

        index += 3;
    }

    let grouped_inputs = group(inputs.into_iter().map(|input| (input.key, input)));
    // in the general case, most series will be using compressed chunks, so rayon should help
    // greatly with latency

    let mut results: SmallVec<ValkeyValue, 10> = SmallVec::new();

    // todo!!
    Ok(ValkeyValue::Array(vec![]))
}

fn add_sample_internal(ctx: &Context, key: &ValkeyString, input: &Vec<ParsedInput>) {
    if let Ok(Some(series)) = get_timeseries_mut(ctx, key, true) {
        let samples = input.iter()
            .map(|input| Sample { timestamp: input.timestamp, value: input.value} )
            .collect::<Vec<Sample>>();

        series.merge_samples(&samples, None).expect("TODO: panic message");
    } else {
        // todo: return null entries
    }
}

fn replicate_and_notify(ctx: &Context, parsed_input: &ParsedInput) {
    let args = &[
        parsed_input.key,
        parsed_input.raw_timestamp,
        parsed_input.raw_value,
    ];
    ctx.replicate("VM.MADD", args);
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "VM.MADD", parsed_input.key);
}


fn group<K, V, I>(iter: I) -> HashMap<K, Vec<V>>
where
    K: Eq + std::hash::Hash,
    I: Iterator<Item = (K, V)>,
{
    let mut hash_map = match iter.size_hint() {
        (_, Some(len)) => HashMap::with_capacity(len),
        (len, None) => HashMap::with_capacity(len)
    };

    for (key, value) in iter {
        hash_map.entry(key).or_insert_with(|| Vec::with_capacity(1)).push(value)
    }

    hash_map
}