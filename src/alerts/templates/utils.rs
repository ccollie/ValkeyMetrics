use super::models::Metric;
use gtmpl_value::{FuncError, Value};
use std::collections::{BTreeMap, HashMap};

pub(super) fn ensure_single_arg<'a>(args: &'a [Value], name: &str) -> Result<&'a Value, FuncError> {
    if args.len() != 1 {
        return Err(FuncError::ExactlyXArgs(name.to_string(), 1));
    }
    Ok(&args[0])
}

pub(super) fn ensure_single_string_arg<'a>(
    args: &'a [Value],
    name: &str,
) -> Result<&'a String, FuncError> {
    if args.len() != 1 {
        return Err(FuncError::ExactlyXArgs(name.to_string(), 1));
    }
    match &args[0] {
        Value::String(s) => Ok(s),
        _ => Err(FuncError::Generic(format!(
            "{name} expects a string for arg 0"
        ))),
    }
}

pub(super) fn expect_number_value(val: &Value, name: &str) -> Result<f64, FuncError> {
    match val {
        Value::Number(n) => {
            let mut val = n.as_f64();
            if val.is_none() {
                val = n.as_u64().map(|n| n as f64);
            }
            if val.is_none() {
                val = n.as_i64().map(|n| n as f64);
            }
            Ok(val.unwrap_or_default())
        }
        Value::String(s) => {
            let n: f64 = s
                .parse()
                .map_err(|_| FuncError::Generic(format!("could not parse {} as a number", s)))?;
            Ok(n)
        }
        _ => Err(FuncError::Generic(format!(
            "expected number for {name}, got {}",
            val
        ))),
    }
}

pub(super) fn ensure_single_f64(args: &[Value], name: &str) -> Result<f64, FuncError> {
    let v = ensure_single_arg(args, name)?;
    expect_number_value(v, name)
}

pub(super) fn ensure_string_arg<'a>(
    args: &'a [Value],
    idx: usize,
    name: &str,
) -> Result<&'a String, FuncError> {
    match args.get(idx) {
        Some(Value::String(s)) => Ok(s),
        _ => Err(FuncError::Generic(format!(
            "{name} expects a string for arg {idx}"
        ))),
    }
}

pub(super) fn ensure_number_arg(args: &[Value], idx: usize, name: &str) -> Result<f64, FuncError> {
    if let Some(arg) = args.get(idx) {
        expect_number_value(arg, name)
    } else {
        Err(FuncError::Generic(format!(
            "{name} expects a value for arg {idx}"
        )))
    }
}

pub(super) fn get_metric_arg(args: &[Value], idx: usize, func: &str) -> Result<Metric, FuncError> {
    if let Some(val) = args.get(idx) {
        let metric: Metric = val.try_into()?;
        Ok(metric)
    } else {
        Err(FuncError::Generic(format!(
            "{func} expects an Metric for arg {idx}"
        )))
    }
}

pub(super) fn get_array_arg<'a>(
    args: &'a [Value],
    idx: usize,
    func: &str,
) -> Result<&'a Vec<Value>, FuncError> {
    match args.get(idx) {
        Some(Value::Array(arr)) => Ok(arr),
        _ => Err(FuncError::Generic(format!(
            "{func} expects an array for arg {idx}"
        ))),
    }
}

pub(super) fn get_hash_value<'a>(
    val: &'a Value,
    key: &str,
    must_exist: bool,
) -> Result<Option<&'a Value>, FuncError> {
    match val {
        Value::Object(hash) | Value::Map(hash) => {
            let result = hash.get(key);
            if must_exist && result.is_none() {
                return Err(FuncError::Generic(format!("missing key {key} in hash")));
            }
            Ok(result)
        }
        _ => Err(FuncError::Generic(format!(
            "expected object for getHashValue, got {val}"
        ))),
    }
}

pub(super) fn get_hash_string_value<'a>(
    val: &'a Value,
    key: &str,
    must_exist: bool,
) -> Result<Option<&'a String>, FuncError> {
    match get_hash_value(val, key, must_exist)? {
        Some(Value::String(s)) => Ok(Some(s)),
        _ => Err(FuncError::Generic(format!(
            "expected string property for {key} in hash"
        ))),
    }
}

pub(super) fn get_hash_float_value(
    val: &Value,
    key: &str,
    must_exist: bool,
) -> Result<Option<f64>, FuncError> {
    match get_hash_value(val, key, must_exist)? {
        Some(Value::Number(n)) => Ok(n.as_f64()),
        Some(Value::String(s)) => Ok(s.parse().ok()),
        _ => Err(FuncError::Generic(format!(
            "expected float property for {key} in hash"
        ))),
    }
}

pub(super) fn get_hash_array_value<'a>(
    val: &'a Value,
    key: &str,
    must_exist: bool,
) -> Result<Option<&'a Vec<Value>>, FuncError> {
    match get_hash_value(val, key, must_exist)? {
        Some(Value::Array(n)) => Ok(Some(n)),
        _ => Err(FuncError::Generic(format!(
            "expected array for property {key} in hash"
        ))),
    }
}

pub(super) fn get_hash_bool_value(
    val: &Value,
    key: &str,
    must_exist: bool,
) -> Result<Option<bool>, FuncError> {
    match get_hash_value(val, key, must_exist)? {
        Some(Value::Bool(b)) => Ok(Some(*b)),
        _ => Err(FuncError::Generic(format!(
            "expected bool property for {key} in hash"
        ))),
    }
}

pub(super) fn btree_map_to_template_value(map: &BTreeMap<String, String>) -> Value {
    let mut m = HashMap::new();
    for (k, v) in map {
        m.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(m)
}
