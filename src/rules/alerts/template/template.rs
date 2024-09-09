// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::rules::alerts::{AlertsError, AlertsResult, DatasourceMetric};
use crate::rules::template::models::Metric;
use crate::rules::template::utils::{ensure_single_arg, ensure_single_f64, ensure_single_string_arg, ensure_string_arg, get_array_arg, get_metric_arg};
use chrono::{DateTime, Duration, Utc};
use enquote::enquote;
use gtmpl::{gtmpl_fn, Func, FuncError, Template, Value};
use htmlescape::encode_minimal;
use metricsql_common::humanize::humanize_bytes;
use metricsql_runtime::METRIC_NAME_LABEL;
use regex::Regex;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use titlecase::titlecase;
use url::Url;

pub type FuncMap = HashMap<String, Func>;

// go template execution fails when it's tree is empty
const DEFAULT_TEMPLATE: &str = r##"{{- define "default.template" -}}{{- end -}}"##;

#[derive(Default)]
pub(crate) struct TextTemplate {
    pub(crate) current:     Template,
    pub(crate) replacement: Template
}

impl Clone for TextTemplate {
    fn clone(&self) -> Self {
        TextTemplate {
            current: clone_template(&self.current),
            replacement: clone_template(&self.replacement)
        }
    }
}

fn clone_base_template(tpl: &Template) -> Template {
    let mut result = Template::default();
    result.name = tpl.name.clone();
    result.tree_set = tpl.tree_set.clone();
    result.funcs = tpl.funcs.clone();
    result.text = tpl.text.clone();
    result
}

static MASTER_TEMPLATE: OnceLock<RwLock<TextTemplate>> = OnceLock::new();

fn get_master_template_ref() -> &'static RwLock<TextTemplate> {
    MASTER_TEMPLATE.get_or_init(|| {
        create_master_template()
    })
}
fn create_master_template() -> RwLock<TextTemplate> {
    RwLock::new(TextTemplate {
        current:     Template::default(),
        replacement: Template::default()
    })
}
pub(crate) fn new_template() -> AlertsResult<Template> {
    let mut tmpl = Template::default();
    tmpl.add_funcs(&template_funcs);
    tmpl.parse(DEFAULT_TEMPLATE)
        .map_err(|e| AlertsError::TemplateParseError(e.to_string()))?;
    Ok(tmpl)
}
pub(crate) fn clone_template(tpl: &Template) -> Template {
    let mut result = Template::default();
    result.name = tpl.name.clone();
    result.tree_set = tpl.tree_set.clone();
    result.funcs = tpl.funcs.clone();
    result.text = tpl.text.clone();
    result
}

/// Reload func replaces current template with a replacement template which was set by load with
/// override=false
pub fn reload() {
    let master_template = get_master_template_ref();
    let mut writer = master_template.write().unwrap();
    if !writer.replacement.text.is_empty() {
        writer.current = std::mem::take(&mut writer.replacement);
        writer.replacement.text.clear();
    }
}



/// converts Metrics from provider package to private copy for templating.
fn datasource_metrics_to_template_metrics(ms: &[DatasourceMetric]) -> Vec<Metric> {
    let mut mss = Vec::with_capacity(ms.len());
    for m in ms.iter() {
        mss.push(Metric {
            labels: m.labels.clone(),
            timestamp: m.timestamps[0],
            value:     m.values[0]
        })
    }
    mss
}

/// QueryFn is used to wrap a call to provider into simple-to-use function for templating functions.
pub type QueryFn = fn(query: &str) -> AlertsResult<Vec<DatasourceMetric>>;

pub static QUERY_FUNCTION: QueryFn = |query: &str| -> AlertsResult<Vec<DatasourceMetric>> {
    Err(AlertsError::Generic(format!("query function is not set: {}", query)))
};

/// update_with_funcs updates existing or sets a new function map for a template
pub(crate) fn update_with_funcs(funcs: &FuncMap) {
    let master_template = get_master_template_ref();
    let mut writer = master_template.write().unwrap();
    writer.current.funcs = funcs.clone();
}

/// returns a copy of current template with additional FuncMap provided with funcs argument
pub(crate) fn get_with_funcs(funcs: FuncMap) -> AlertsResult<Template> {
    let master_template = get_master_template_ref();
    let mut reader = master_template.read().unwrap();
    let mut tmpl = clone_template(&reader.current);

    tmpl.funcs = funcs;
    Ok(tmpl)
}

/// returns a copy of a template
pub(crate) fn get_template() -> Template {
    let master_template = get_master_template_ref();
    let reader = master_template.read().unwrap();
    clone_template(&reader.current)
}

pub(crate) fn make_query_fn(query: QueryFn) -> Func {
    |args: &[Value]| -> Result<Value, FuncError> {
        let arg = ensure_single_arg(args, "query")?;
        if let Value::String(q) = arg {
            let result = query(&q)
                .map_err(|e| FuncError::Generic(format!("query failed: {}", e)))?;
            let mss = datasource_metrics_to_template_metrics(&result).into();
            Ok(Value::Array(mss))
        } else {
            Err(FuncError::Generic(format!("expected string argument, got {}", arg)))
        }
    }
}

pub fn make_const_function<T: Into<Value>>(val: T) -> Func {
    move |_args: &[Value]| -> Result<Value, FuncError> {
        Ok(val.into())
    }
}

/// returns a function map that depends on metric data
pub(crate) fn funcs_with_query(query: QueryFn) -> FuncMap {
    let mut map = FuncMap::new();
    map.insert("query".to_string(), make_query_fn(query));
    map
}

/// returns a function map that depends on external_url value
pub(crate) fn funcs_with_external_url(external_url: Url) -> FuncMap {
    let mut funcs = FuncMap::new();
    funcs
}

// title returns a copy of the string s with all Unicode letters
// that begin words mapped to their Unicode title case.
// alias for https://golang.org/pkg/strings/#Title
fn title_case(args: &[Value]) -> Result<Value, FuncError> {
    let s = ensure_single_arg(args, "titleCase")?.to_string();
    Ok(Value::from(titlecase(&s)))
}

// crlf_escape replaces '\n' and '\r' chars with `\\n` and `\\r`.
// This function is deprecated.
//
// It is better to use quotesEscape, jsonEscape, queryEscape or pathEscape instead -
// these functions properly escape `\n` and `\r` chars according to their purpose.
fn crlf_escape(args: &[Value]) -> Result<Value, FuncError>  {
    let q = ensure_single_arg(args, "crlfEscape")?.to_string();
    let q = q.replace( "\\n", "\n");
    Ok(q.replace( "\\r", "\r").into())
}

// to_upper returns s with all Unicode letters mapped to their upper case.
fn to_upper(args: &[Value]) -> Result<Value, FuncError> {
    let s = ensure_single_arg(args, "toUpper")?.to_string();
    Ok(s.to_uppercase().into())
}

// to_lower returns s with all Unicode letters mapped to their lower case.
fn to_lower(args: &[Value]) -> Result<Value, FuncError> {
    let s = ensure_single_arg(args, "toLower")?.to_string();
    Ok(s.to_lowercase().into())
}

fn trim_spaces(args: &[Value]) -> Result<Value, FuncError> {
    let s = ensure_single_arg(args, "trimSpaces")?;
    match s {
        Value::String(s) => Ok(s.trim().into()),
        _ => Err(FuncError::Generic(format!("expected string for trimSpaces, got {s}")))
    }
}

// parseDuration parses a duration string such as "1h" into the number of seconds it represents
fn parse_duration(args: &[Value]) -> Result<Value, FuncError> {
    let s = ensure_single_arg(args, "parseDuration")?.to_string();
    match metricsql_parser::prelude::parse_duration_value(&s, 1) {
        Ok(d) => Ok(((d / 1000) as f64).into()),
        Err(_e) => Ok(Value::from(0f64))
    }
}

// same with parseDuration but returns a std::time::Duration
fn parse_duration_time(args: &[Value]) -> Result<Duration, FuncError> {
    let s = ensure_single_arg(args, "parseDurationTime")?.to_string();
    match metricsql_parser::prelude::parse_duration_value(&s, 1) {
        Ok(d) => Ok(Duration::milliseconds(d)),
        Err(_e) => Ok(Duration::milliseconds(0))
    }
}

// re_replace_all returns a copy of src, replacing matches of the Regexp with
// the replacement string repl. Inside repl, $ signs are interpreted as in Expand,
// so for instance $1 represents the text of the first submatch.
// alias for https://golang.org/pkg/regexp/#Regexp.ReplaceAllString
fn re_replace_all(args: &[Value]) -> Result<Value, FuncError> {
    let pattern = ensure_string_arg(args, 0, "reReplaceAll")?;
    let repl = ensure_string_arg(args, 1, "reReplaceAll")?;
    let text = ensure_string_arg(args, 2, "reReplaceAll")?;
    let re = Regex::new(pattern)
        .map_err(|e| FuncError::Generic(format!("Invalid regex {pattern}")))?;
    Ok(re.replace_all(text, repl).into())
}


// first returns the first by order element from the given metrics list.
// usually used alongside with `query` template function.
fn first(args: &[Value]) -> Result<Value, FuncError> {
    if let Value::Array(metrics) = ensure_single_arg(args, "first")? {
        if !metrics.is_empty() {
            return Ok(metrics[0].clone())
        }
        Err(FuncError::Generic("first() called on vector with no elements".to_string()))
    } else {
        Err(FuncError::Generic("first() called on non-array".to_string()))
    }
}

// toTime converts given timestamp to a time.Time.
gtmpl_fn!(fn to_time(v: u64) -> Result<DateTime<Utc>, FuncError> {
    // v here is seconds
    match DateTime::from_timestamp_millis((v * 1000) as i64) {
        Some(t) => Ok(t),
        None => Err( FuncError::Generic(format!("cannot convert {} to Time", v)))
    }
});

// match reports whether the string s
// contains any match of the regular expression pattern.
// alias for https://golang.org/pkg/regexp/#MatchString
fn regex_match(args: &[Value]) -> Result<Value, FuncError> {
    if args.len() != 2 {
        return Err(FuncError::ExactlyXArgs("match".to_string(), 2))
    }
    let pattern = ensure_string_arg(args, 0, "match")?;
    let text = ensure_string_arg(args, 1, "match")?;

    let re = Regex::new(&pattern)
        .map_err(|_e| FuncError::Generic(format!("Invalid regex {pattern}")))?;
    Ok(re.is_match(&text).into())
}

// quotesEscape escapes the string, so it can be safely put inside JSON string.
//
// See also jsonEscape.
fn quotes_escape(args: &[Value]) -> Result<Value, FuncError> {
    let s = ensure_single_arg(args, "quotesEscape")?.to_string();
    Ok(enquote('"', &s).into())
}

static EMPTY_STRING: &str = "";
fn get_metric_label_value<'a>(metric: &'a Metric, label: &str) -> &'a str {
    metric.labels.iter().find(|l| l.name == label)
        .map_or(EMPTY_STRING, |s| &s.value)
}

// returns metric name.
fn str_value(args: &[Value]) -> Result<Value, FuncError> {
    match get_metric_arg(args, 0, "strValue") {
        Ok(metric) => Ok(get_metric_label_value(&metric, METRIC_NAME_LABEL).into()),
        _ => Ok(Value::NoValue)
    }
}

/// label returns the value of the given label name for the given metric.
/// usually used alongside with `query` template function.
fn get_label(args: &[Value]) -> Result<Value, FuncError> {
    if args.len() != 2 {
        return Err(FuncError::ExactlyXArgs("getLabel".to_string(), 2))
    }
    let label = if let Ok(s) = ensure_string_arg(args, 0, "getLabel") {
        s
    } else {
        return Ok(Value::NoValue)
    };
    match get_metric_arg(args, 1, "getLabel") {
        Ok(metric) => Ok(get_metric_label_value(&metric, label).into()),
        _=> Ok(Value::NoValue)
    }
}

// value returns the value of the given metric.
// usually used alongside with `query` template function.
fn get_value(args: &[Value]) -> Result<Value, FuncError> {
    let m = ensure_single_arg(args, "value")?;
    if let Value::Map(m) = m {
        if let Some(v) = m.get("value") {
            return Ok(v.clone())
        }
    }
    Ok(Value::NoValue)
}

/// sortByLabel sorts the given metrics by provided label key
fn sort_by_label(args: &[Value]) -> Result<Value, FuncError> {
    if args.len() != 2 {
        return Err(FuncError::ExactlyXArgs("sortByLabel".to_string(), 2))
    }
    let label = if let Ok(s) = ensure_string_arg(args, 0, "sortByLabel") {
        s
    } else {
        return Ok(Value::NoValue)
    };
    let arr = get_array_arg(args, 1, "sortByLabel")?;
    let mut metrics = Vec::with_capacity(arr.len());
    for m in arr.iter() {
        let metric = m.try_into()?;
        metrics.push(metric)
    }
    metrics.sort_by(|a, b| {
        let a_value = get_metric_label_value(a, &label);
        let b_value = get_metric_label_value(b, &label);
        a_value.cmp(&b_value)
    });

    let values = metrics.iter().map(|m| m.into()).collect();
    Ok(Value::Array(values))
}

// Converts a list of objects to a map with keys arg0, arg1 etc.
// This is intended to allow multiple arguments to be passed to templates.
fn args(args: &[Value]) -> Result<Value, FuncError> {
    let mut result = HashMap::with_capacity(args.len());
    for (i, a) in args.iter().enumerate() {
        result.insert(format!("arg{}", i), a.clone());
    }
    Ok(Value::Map(result))
}


// pathEscape escapes the string, so it can be safely placed inside a URL path segment.
//
// See also queryEscape.
fn path_escape(s: &[Value]) -> Result<Value, FuncError> {
    let mut s = ensure_single_arg(s, "pathEscape")?.to_string();
    let base = "example.com";
    let mut url = parse_url(base)?;
    url.set_path(&mut s);
    let result = url.path();
    Ok(result.into())
}

fn query_escape(args: &[Value]) -> Result<Value, FuncError> {
    let s = ensure_single_arg(args, "queryEscape")?.to_string();
    let base = "example.com";
    let mut url = parse_url(base)?;
    url.set_query(Some(&s));
    let result = url.query().unwrap_or("");
    Ok(result.into())
}

fn parse_url(s: &str) -> Result<Url, FuncError> {
    Url::parse(s)
        .map_err(|e| FuncError::Generic(format!("Invalid URL {s}: {e}")))
}


// stripPort splits the url and returns only the host.
fn strip_port(args: &[Value]) -> Result<Value, FuncError> {
    let host_port = ensure_single_arg(args, "stripPort")?.to_string();
    let url = parse_url(&host_port)?;
    let host = url.host_str().unwrap_or("");
    Ok(host.to_string().into())
}

// strip_domain removes the domain part of a FQDN. Leaves port untouched.
fn strip_domain(args: &[Value]) -> Result<Value, FuncError> {
    let host_port = ensure_single_arg(args, "stripDomain")?.to_string();
    let mut url = parse_url(&host_port)?;
    let domain = url.domain();
    if domain.is_none() {
        return Ok(host_port.into())
    }
    let domain = domain.unwrap();
    let port = url.port();
    let host = url.host_str().unwrap_or("");
    let host = host.split('.').next().unwrap_or(host);
    if port.is_some() {
        return Ok(format!("{}:{}", host, port.unwrap()).into())
    }
    Ok(host.to_string().into())
}

// html_escape applies html-escaping to q, so it can be safely embedded as plaintext into html.
//
// See also safeHtml.
fn html_escape(args: &[Value]) -> Result<Value, FuncError> {
    let q = ensure_single_arg(args, "htmlEscape")?.to_string();
    Ok( encode_minimal(&q).into() )
}


// jsonEscape converts the string to properly encoded JSON string.
//
// See also quotesEscape.
fn json_escape(args: &[Value]) -> Result<Value, FuncError> {
    let s = ensure_single_arg(args, "jsonEscape")?.to_string();
    let value = serde_json::to_string(&s)
        .map_err(|e| FuncError::Generic(format!("cannot convert {s} to JSON: {e}")))?;
    Ok(Value::from(value))
}

// converts given number to a human-readable format
// by adding metric prefixes https://en.wikipedia.org/wiki/Metric_prefix
fn humanize(args: &[Value]) -> Result<Value, FuncError> {
    match ensure_single_f64(args, "humanize") {
        Ok(n) => Ok(humanize_bytes(n).into()),
        Err(_e) => Ok(Value::NoValue)
    }
}

/// humanize1024 converts given number to a human-readable format with 1024 as base
fn humanize1024(args: &[Value]) -> Result<Value, FuncError> {
    match ensure_single_f64(args, "humanize1024") {
        Ok(v) => {
            if v.abs() <= 1.0 || v.is_nan() || v.is_infinite() {
                return Ok(format!("{:.4}", v).into())
            }
            Ok(humanize_bytes(v).into())
        },
        Err(_e) => Ok(Value::NoValue)
    }
}

// humanize_duration converts given seconds to a human-readable duration
fn humanize_duration(args: &[Value]) -> Result<Value, FuncError> {
    let mut v = if let n = ensure_single_f64(args, "humanizeDuration")? {
        n
    } else {
        return Ok(Value::NoValue)
    };
    if v.is_nan() || v.is_infinite() {
        return Ok(format!("{:.4}", v).into());
    }
    if v == 0.0 {
        return Ok(format!("{:.4}s", v).into());
    }
    if v.abs() >= 1.0 {
        let mut sign = "";
        if v < 0.0 {
            v = -v;
            sign = "-";
        }
        let v_int = v as i64;
        let seconds = v_int % 60;
        let minutes = (v_int / 60) % 60;
        let hours = (v_int / 60 / 60) % 24;
        let days = v_int / 60 / 60 / 24;
        // For days to minutes, we display seconds as an integer.
        if days != 0 {
            return Ok(format!("{sign}{days}d {hours}h {minutes}m {seconds}s").into());
        }
        if hours != 0 {
            return Ok(format!("{sign}{hours}h {minutes}m {seconds}s").into());
        }
        if minutes != 0 {
            return Ok(format!("{sign}{minutes}m {seconds}s").into());
        }
        // For seconds, we display 4 significant digits.
        return Ok(format!("{sign}{:.4}s", v).into())
    }

    let mut prefix = "";
    for p in ["m", "u", "n", "p", "f", "a", "z", "y"] {
        if v.abs() >= 1.0 {
            break
        }
        prefix = p;
        v *= 1000.0
    }
    Ok(format!("{:.4}{prefix}s", v).into())
}

// humanize_percentage converts given ratio value to a fraction of 100
fn humanize_percentage(args: &[Value]) -> Result<Value, FuncError> {
    if let v = ensure_single_f64(args, "humanizePercentage")? {
        return Ok(format!("{:.4}%", v*100.0).into())
    }
    Ok(Value::NoValue)
}

// humanize_timestamp converts given timestamp to a human readable time equivalent
fn humanize_timestamp(args: &[Value]) -> Result<Value, FuncError> {
    if let v= ensure_single_f64(args, "humanizeTimestamp")? {
        let v = v as i64;
        if v == i64::MAX || v == i64::MIN {
            return Ok(format!("{:.4}", v).into())
        }
        if let Some(t) = DateTime::from_timestamp(v, 0) {
            Ok(t.to_string().into())
        } else {
            Ok("".to_string().into())
        }
    } else {
        Ok(Value::NoValue)
    }
}

// query executes the MetricsQL/PromQL query against
// configured `provider.url` address.
// For example, {{ query "foo" | first | value }} will
// execute "/api/v1/query?query=foo" request and will return
// the first value in response.
fn query(args: &[Value]) -> Result<Value, FuncError> {
    let query = ensure_single_string_arg(args, "query")?;
    let result = query(&query)
        .map_err(|e| FuncError::Generic(format!("query failed: {}", e)))?;
    let mss = datasource_metrics_to_template_metrics(&result).into();
    Ok(Value::Array(mss))
}

/// template_funcs initiates template helper functions
pub fn template_funcs() -> FuncMap {
// See https://prometheus.io/docs/prometheus/latest/configuration/template_reference/
// and https://github.com/prometheus/prometheus/blob/fa6e05903fd3ce52e374a6e1bf4eb98c9f1f45a7/template/template.go#L150
    let mut funcs = FuncMap::new();
    /* Strings */
    funcs.insert("title".to_string(), title_case);
    funcs.insert("toUpper".to_string(), to_upper);
    funcs.insert("toLower".to_string(), to_lower);
    funcs.insert("crlfEscape".to_string(), crlf_escape);
    funcs.insert("quotesEscape".to_string(), quotes_escape);
    funcs.insert("jsonEscape".to_string(), json_escape);
    funcs.insert("htmlEscape".to_string(), html_escape);
    funcs.insert("trimSpaces".to_string(), trim_spaces);

    funcs.insert("stripPort".to_string(), strip_port);
    funcs.insert("stripDomain".to_string(), strip_domain);
    funcs.insert("match".to_string(), regex_match);
    funcs.insert("reReplaceAll".to_string(), re_replace_all);

    funcs.insert("parseDuration".to_string(), parse_duration);
    funcs.insert("parseDurationTime".to_string(), parse_duration_time);

    /* Number */
    funcs.insert("humanize".to_string(), humanize);
    funcs.insert("humanize1024".to_string(), humanize1024);
    funcs.insert("humanizeDuration".to_string(), humanize_duration);
    funcs.insert("humanizePercentage".to_string(), humanize_percentage);
    funcs.insert("humanizeTimestamp".to_string(), humanize_timestamp);
    funcs.insert("toTime".to_string(), to_time);

    /* URLs */
    funcs.insert("pathEscape".to_string(), path_escape);
    funcs.insert("queryEscape".to_string(), query_escape);
    funcs.insert("query".to_string(), query);

    funcs.insert("first".to_string(), first);
    funcs.insert("label".to_string(), get_label);
    funcs.insert("value".to_string(), get_value);
    funcs.insert("strValue".to_string(), str_value);
    funcs.insert("sortByLabel".to_string(), sort_by_label);

    /* Helpers */
    funcs.insert("args".to_string(), args);

    funcs
}
