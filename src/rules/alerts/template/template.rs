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

use std::sync::{OnceLock, RwLock};
use std::collections::HashMap;
use chrono::{Duration, NaiveDateTime};
use enquote::enquote;
use crate::rules::alerts::{AlertsError, AlertsResult, DatasourceMetric};
use gtmpl::{FuncError, gtmpl_fn, Template, Func, Value};
use gtmpl_derive::Gtmpl;
use metricsql_engine::METRIC_NAME_LABEL;
use regex::Regex;
use url::Url;
use crate::common;
use crate::common::humanize::humanize_bytes;
use crate::ts::Labels;
use htmlescape::encode_minimal;

pub type FuncMap = HashMap<String, Func>;

// go template execution fails when it's tree is empty
const DEFAULT_TEMPLATE: &str = r##"{{- define "default.template" -}}{{- end -}}"##;

#[derive(Clone, Default)]
pub(crate) struct TextTemplate {
    pub(crate) current:     Template,
    pub(crate) replacement: Template
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
    tmpl.funcs(template_funcs());
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

/// metric is private copy of provider.Metric,
/// it is used for templating annotations,
/// Labels as map simplifies templates evaluation.
#[derive(Gtmpl)]
struct Metric {
    labels: Labels,
    timestamp: i64,
    value: f64,
}

/// converts Metrics from provider package to private copy for templating.
fn datasource_metrics_to_template_metrics(ms: &[DatasourceMetric]) -> Vec<Metric> {
    let mut mss = Vec::with_capacity(ms.len());
    for m in ms.iter() {
        let labels_map = Labels::with_capacity(m.labels.len());
        for label in m.labels.iter() {
            labels_map.insert(label.name.clone(), label.value.clone());
        }
        mss.push(Metric {
            labels: labels_map,
            timestamp: m.timestamps[0],
            value:     m.values[0]
        })
    }
    return mss
}

/// QueryFn is used to wrap a call to provider into simple-to-use function for templating functions.
pub type QueryFn = fn(query: &str) -> AlertsResult<Vec<DatasourceMetric>>;

/// update_with_funcs updates existing or sets a new function map for a template
pub(crate) fn update_with_funcs(funcs: &FuncMap) {
    let master_template = get_master_template_ref();
    let mut writer = master_template.write().unwrap();
    writer.current = writer.current.Funcs(funcs);
}

/// get_with_funcs returns a copy of current template with additional FuncMap
/// provided with funcs argument
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
    move |args: &[Value]| -> Result<Value, FuncError> {
        if args.len() != 1 {
            return Err(FuncError::ExactlyXArgs("query".to_string(), 1))
        }
        let q = args[0].as_str()
            .ok_or_else(|| FuncError::Generic(format!("expected string argument, got {}", args[0].as_str().unwrap_or(""))))?;
        let result = query(q)?;
        let mss = datasource_metrics_to_template_metrics(&result).into();
        Ok(Value::Array(mss))
    }
}

pub fn make_const_function<T: Into<Value>>(val: T) -> Func {
    move |_args: &[Value]| -> Result<Value, FuncError> {
        Ok(val.into())
    }
}

/// funcs_with_query returns a function map that depends on metric data
pub(crate) fn funcs_with_query(query: QueryFn) -> FuncMap {
    let mut map = FuncMap::new();
    map.insert("query".to_string(), make_query_fn(query));
    map
}

/// funcs_with_external_url returns a function map that depends on external_url value
pub(crate) fn funcs_with_external_url(external_url: Url) -> FuncMap {
    let mut funcs = FuncMap::new();
    funcs.insert("external_url".to_string(), make_const_function(external_url.to_string()));
    funcs.insert("pathPrefix".to_string(), make_const_function(external_url.path()));
    funcs
}

// title returns a copy of the string s with all Unicode letters
// that begin words mapped to their Unicode title case.
// alias for https://golang.org/pkg/strings/#Title
gtmpl_fn!(fn title_case(s: &str) -> Result<String, FuncError> {
    Ok(s.to_title_case())
});

// crlf_escape replaces '\n' and '\r' chars with `\\n` and `\\r`.
// This function is deprecated.
//
// It is better to use quotesEscape, jsonEscape, queryEscape or pathEscape instead -
// these functions properly escape `\n` and `\r` chars according to their purpose.
gtmpl_fn!(
fn crlf_escape(q: &str) -> Result<String, FuncError>  {
    let q = q.replace( "\\n", "\n");
    Ok(q.replace( "\\r", "\r"))
});


// to_upper returns s with all Unicode letters mapped to their upper case.
gtmpl_fn!(fn to_upper(s: &str) -> Result<String, FuncError> {
    Ok(s.to_uppercase())
});

// to_lower returns s with all Unicode letters mapped to their lower case.
gtmpl_fn!(fn to_lower(s: &str) -> Result<String, FuncError> {
    Ok(s.to_lowercase())
});

// parseDuration parses a duration string such as "1h" into the number of seconds it represents
gtmpl_fn!(fn parse_duration(s: &str) -> Result<f64, FuncError> {
    match metricsql_parser::prelude::parse_duration_value(s, 1) {
        Ok(d) => Ok((d / 1000) as f64),
        Err(e) => Ok(0f64)
    }
});

// re_replace_all returns a copy of src, replacing matches of the Regexp with
// the replacement string repl. Inside repl, $ signs are interpreted as in Expand,
// so for instance $1 represents the text of the first submatch.
// alias for https://golang.org/pkg/regexp/#Regexp.ReplaceAllString
gtmpl_fn!(
    fn re_replace_all(pattern: &str, repl: &str, text: &str) -> Result<String, FuncError> {
    let re = Regex::new(pattern)
        .map_err(|e| FuncError::Generic(format!("Invalid regex {pattern}")))?;
    Ok(re.replace_all(text, repl).to_string())
});

// first returns the first by order element from the given metrics list.
// usually used alongside with `query` template function.
gtmpl_fn!(fn first(metrics: &Vec<Metric>) -> Result<Metric, FuncError> {
    if !metrics.is_empty() {
        return Ok(metrics[0].clone())
    }
    Err(FuncError::Generic("first() called on vector with no elements".to_string()))
});

// toTime converts given timestamp to a time.Time.
gtmpl_fn!(fn to_time(v: u64) -> Result<NaiveDateTime, FuncError> {
    // v here is seconds
    if v.is_nan() || v.is_infinite() {
        return Err( FuncError::Generic(format!("cannot convert {} to Time", v)));
    }
    let t = NaiveDateTime::from_timestamp_millis(v * 1000);
    Ok(t)
});

// match reports whether the string s
// contains any match of the regular expression pattern.
// alias for https://golang.org/pkg/regexp/#MatchString
gtmpl_fn!(fn regex_match(pattern: &str, text: &str) -> Result<bool, FuncError> {
    let re = Regex::new(pattern)
        .map_err(|e| FuncError::Generic(format!("Invalid regex {pattern}")))?;
    Ok(re.is_match(text))
});

// quotesEscape escapes the string, so it can be safely put inside JSON string.
//
// See also jsonEscape.
gtmpl_fn!(fn quotes_escape(s: &str) -> Result<String, FuncError> {
    Ok(enquote('"', s))
});

static EMPTY_STRING: &str = "";
fn get_metric_label_value<'a>(metric: &'a Metric, label: &str) -> &'a str {
    metric.labels
        .get(label)
        .map_or(EMPTY_STRING, |s| &s)
}

// returns metric name.
gtmpl_fn!(fn str_value(m: &Metric) -> Result<String, FuncError> {
    let value = get_metric_label_value(m, METRIC_NAME_LABEL);
    Ok(value.to_string())
});

// label returns the value of the given label name for the given metric.
// usually used alongside with `query` template function.
gtmpl_fn!(fn get_label(label: &str, m: &Metric) -> Result<String, FuncError> {
    let value = get_metric_label_value(m, label);
    Ok(value.to_string())
});

// value returns the value of the given metric.
// usually used alongside with `query` template function.
gtmpl_fn!(fn get_value(m: &Metric) -> Result<f64, FuncError> {
    return Ok(m.value)
});

// sortByLabel sorts the given metrics by provided label key
gtmpl_fn!(fn sort_by_label(label: &str, metrics: &[Metric]) -> Result<Vec<Metric>, FuncError> {
    let mut metrics = metrics.to_vec();
    metrics.sort_by(|a, b| {
        let a_value = get_metric_label_value(a, label);
        let b_value = get_metric_label_value(b, label);
        a_value.cmp(&b_value)
    });
    Ok(metrics)
});

// same with parseDuration but returns a time.Duration
gtmpl_fn!(fn parse_duration_time(s: &str) -> Result<Duration, FuncError> {
    let d = parse_duration(s)?;
    Ok(Duration::milliseconds(d as i64))
});

// Converts a list of objects to a map with keys arg0, arg1 etc.
// This is intended to allow multiple arguments to be passed to templates.
gtmpl_fn!(fn args(args: &[Value]) -> Result<Value, FuncError> {
    let mut result = HashMap::with_capacity(args.len());
    for (i, a) in args.iter().enumerate() {
        result.insert(format!("arg{}", i), a.clone());
    }
    Ok(Value::Map(result))
});


// pathEscape escapes the string so it can be safely placed inside a URL path segment.
//
// See also queryEscape.
gtmpl_fn!(fn path_escape(s: &str) -> Result<String, FuncError> {
    let base = "example.com";
    let mut url = parse_url(base)?;
    url.set_path(Some(s));
    let result = url.path();
    Ok(result.to_string())
});

gtmpl_fn!(fn query_escape(s: &str) -> Result<String, FuncError> {
    let base = "example.com";
    let mut url = parse_url(base)?;
    url.set_query(Some(s));
    let result = url.query().unwrap_or("");
    Ok(result.to_string())
});

fn parse_url(s: &str) -> Result<Url, FuncError> {
    Url::parse(s)
        .map_err(|e| FuncError::Generic(format!("Invalid URL {s}: {e}")))
}

// path_prefix returns a Path segment from the URL value in `external.url` flag
gtmpl_fn!(fn path_prefix() -> Result<String, FuncError> {
    // pathPrefix function supposed to be substituted at FuncsWithExteralURL().
    // it is present here only for validation purposes, when there is no
    // provided provider.
    //
    // return non-empty slice to pass validation with chained functions in template
    Ok("".to_string())
});

// stripPort splits the url and returns only the host.
gtmpl_fn!(fn strip_port(host_port: &str) -> Result<String, FuncError> {
    // todo:
    let url = parse_url(host_port)?;
    let host = url.host_str().unwrap_or("");
    Ok(host.to_string())
});

// strip_domain removes the domain part of a FQDN. Leaves port untouched.
gtmpl_fn!(fn strip_domain(host_port: &str) -> Result<String, FuncError> {
    let mut url = parse_url(host_port)?;
    let domain = url.domain();
    if domain.is_none() {
        return Ok(host_port.to_string())
    }
    let domain = domain.unwrap();
    let port = url.port();
    let host = url.host_str().unwrap_or("");
    let host = host.split('.').next().unwrap_or(host);
    if port.is_some() {
        return Ok(format!("{}:{}", host, port.unwrap()))
    }
    Ok(host.to_string())
});

// html_escape applies html-escaping to q, so it can be safely embedded as plaintext into html.
//
// See also safeHtml.
gtmpl_fn!(fn html_escape(q: &str) -> Result<String, FuncError> {
    Ok( encode_minimal(q) )
});


// jsonEscape converts the string to properly encoded JSON string.
//
// See also quotesEscape.
gtmpl_fn!(fn json_escape(s: &str) -> Result<String, FuncError> {
    Ok(serde_json::to_string(s)?)
});

// externalURL returns value of `external.url` flag
gtmpl_fn!(fn external_url() -> Result<String, FuncError> {
    // externalURL function supposed to be substituted at FuncsWithExteralURL().
    // it is present here only for validation purposes, when there is no
    // provided provider.
    //
    // return non-empty slice to pass validation with chained functions in template
    Ok("".to_string())
});

// humanize converts given number to a human readable format
// by adding metric prefixes https://en.wikipedia.org/wiki/Metric_prefix
gtmpl_fn!(fn humanize(v: f64) -> Result<String, FuncError> {
    Ok(common::humanize::humanize(v))
});

// humanize1024 converts given number to a human readable format with 1024 as base
gtmpl_fn!(fn humanize1024(v: f64) -> Result<String, FuncError> {
    if v.abs() <= 1.0 || v.is_nan() || v.is_infinite() {
        return format!("{:.4}", v)
    }
    Ok(humanize_bytes(v))
});

// humanize_duration converts given seconds to a human-readable duration
gtmpl_fn!(fn humanize_duration(v: f64) -> Result<String, FuncError> {
    let mut v = v;
    if v.is_nan() || v.is_infinite() {
        return Ok(format!("{:.4}", v));
    }
    if v == 0 {
        return Ok(format!("{:.4}s", v));
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
            return Ok(format!("{sign}{days}d {hours}h {minutes}m {seconds}s"));
        }
        if hours != 0 {
            return Ok(format!("{sign}{hours}h {minutes}m {seconds}s"));
        }
        if minutes != 0 {
            return Ok(format!("{sign}{minutes}m {seconds}s"));
        }
        // For seconds, we display 4 significant digits.
        return Ok(format!("{sign}{:.4}s", v))
    }

    let mut prefix = "";
    for p in ["m", "u", "n", "p", "f", "a", "z", "y"] {
        if v.abs() >= 1.0 {
            break
        }
        prefix = p;
        v *= 1000
    }
    Ok(format!("{:.4}{prefix}s", v))
});

// humanize_percentage converts given ratio value to a fraction of 100
gtmpl_fn!(fn humanize_percentage(v: f64) -> Result<String, FuncError> {
    Ok(format!("{:.4}%", v*100))
});

// humanize_timestamp converts given timestamp to a human readable time equivalent
gtmpl_fn!(fn humanize_timestamp(v: i64) -> Result<String, FuncError> {
    if v.is_nan() || v.is_infinite() {
        return Ok(format!("{:.4}", v))
    }
    if let Some(t) = NaiveDateTime::from_timestamp_millis(v * 1000) {
        Ok(t.to_string())
    } else {
        Ok("".to_string())
    }
});

// query executes the MetricsQL/PromQL query against
// configured `provider.url` address.
// For example, {{ query "foo" | first | value }} will
// execute "/api/v1/query?query=foo" request and will return
// the first value in response.
gtmpl_fn!(fn query(_q: &str) -> Result<Value, FuncError> {
    // query function supposed to be substituted at funcs_with_query().
    // it is present here only for validation purposes, when there is no
    // provided provider.
    //
    // return non-empty slice to pass validation with chained functions in template
    // see issue #989 for details
    Ok(Value::Array(vec![]))
});

// template_funcs initiates template helper functions
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
    funcs.insert("jsonEscape".to_string(), json_escape);
    funcs.insert("htmlEscape".to_string(), html_escape);

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
    funcs.insert("externalURL".to_string(), external_url);
    funcs.insert("pathPrefix".to_string(), path_prefix);
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
