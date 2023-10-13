use std::fmt;
use std::fmt::Display;
use std::sync::OnceLock;

use enquote::enquote;
use metricsql_engine::METRIC_NAME_LABEL;
use regex::Regex;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;
use crate::common::FastStringTransformer;

use crate::common::regex_util::PromRegex;
use crate::common::types::Label;
use crate::rules::relabel::{defaultRegexForRelabelConfig, GraphiteLabelRule, GraphiteMatchTemplate, IfExpression};
use crate::rules::relabel::relabel_config::{RelabelAction};

/// DebugStep contains debug information about a single relabeling rule step
#[derive(Debug, Clone, PartialEq, Default, Deserialize, Serialize)]
pub struct DebugStep {
    /// rule contains string representation of the rule step
    pub(crate) rule: String,

    /// In contains the input labels before the execution of the rule step
    pub(crate) r#in: String,

    /// Out contains the output labels after the execution of the rule step
    pub(crate) out: String,
}


/// ParsedRelabelConfig contains parsed `relabel_config`.
///
/// See https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config
#[derive(Debug, Clone, PartialEq, Default, Deserialize, Serialize)]
pub struct ParsedRelabelConfig {
    /// rule_original contains the original relabeling rule for the given ParsedRelabelConfig.
    pub rule_original: String,

    pub source_labels: Vec<String>,
    pub separator: String,
    pub target_label: String,
    pub regex_anchored: Regex,
    pub modulus: u64,
    pub replacement: String,
    pub action: RelabelAction,
    pub r#if: IfExpression,

    pub regex: PromRegex,
    pub regex_original: Regex,

    pub has_capture_group_in_target_label: bool,
    pub has_capture_group_in_replacement: bool,
    pub has_label_reference_in_replacement: bool,

    pub string_replacer: FastStringTransformer,
    pub submatch_replacer: FastStringTransformer,
    pub graphite_match_template: Option<GraphiteMatchTemplate>,
    pub graphite_label_rules: Vec<GraphiteLabelRule>,
}

impl ParsedRelabelConfig {
    pub fn apply_debug(&mut self, labels: &[Label], _labels_offset: usize) -> (Vec<Label>, DebugStep) {
        self.apply_internal(labels, 0, true)
    }

    /// apply applies relabeling according to prc.
    ///
    /// See https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config
    pub fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        use RelabelAction::*;
        let src = &labels[labels_offset..];
        if !self.r#if.is_match(src) {
            if self.action == Keep {
                // Drop the target on `if` mismatch for `action: keep`
                labels.truncate(labels_offset);
                return;
            }
            // Do not apply prc actions on `if` mismatch.
            return;
        }
        match &self.action {
            Replace => handle_replace(self, labels, labels_offset),
            ReplaceAll => handle_replace_all(self, labels, labels_offset),
            KeepIfEqual => handle_keep_if_equal(self, labels, labels_offset),
            DropIfEqual => handle_drop_if_equal(self, labels, labels_offset),
            KeepEqual => handle_keep_equal(self, labels, labels_offset),
            DropEqual => handle_drop_equal(self, labels, labels_offset),
            Keep => handle_keep(self, labels, labels_offset),
            Drop => handle_drop(self, labels, labels_offset),
            HashMod => handle_hashmod(self, labels, labels_offset),
            LabelMap => handle_label_map(self, labels, labels_offset),
            LabelMapAll => handle_label_map_all(self, labels, labels_offset),
            LabelDrop => handle_label_drop(self, labels, labels_offset),
            LabelKeep => handle_label_keep(self, labels, labels_offset),
            Uppercase => handle_uppercase(self, labels, labels_offset),
            Lowercase => handle_lowercase(self, labels, labels_offset),
            Graphite => handle_graphite(self, labels, labels_offset),
            _ => panic!("BUG: unknown action={}", &self.action),
        }
    }

    /// replaces s with the replacement if s matches '^regex$'.
    ///
    /// s is returned as is if it doesn't match '^regex$'.
    pub(crate) fn replace_full_string_fast(&self, s: &str) -> String {
        // todo: use a COW here
        let (prefix, complete) = self.regex_original.LiteralPrefix();
        let replacement = &self.replacement;
        if complete && !self.has_capture_group_in_replacement {
            if s == prefix {
                // Fast path - s matches literal regex
                return replacement.clone();
            }
            // Fast path - s doesn't match literal regex
            return s.to_string();
        }
        if !s.starts_with(prefix) {
            // Fast path - s doesn't match literal prefix from regex
            return s.to_string();
        }
        if replacement == "$1" {
            // Fast path for commonly used rule for deleting label prefixes such as:
            //
            // - action: labelmap
            //   regex: __meta_kubernetes_node_label_(.+)
            //
            let re_str = self.regex_original.to_string();
            if re_str.starts_with(prefix) {
                let suffix = s[prefix.len()..];
                let re_suffix = re_str[prefix.len()..];
                if re_suffix == "(.*)" {
                    return suffix;
                } else if re_suffix == "(.+)" {
                    if !suffix.is_empty() {
                        return suffix;
                    }
                    return s.to_string();
                }
            }
        }
        if !self.regex.is_match(s) {
            // Fast path - regex mismatch
            return s.to_string();
        }
        // Slow path - handle the rest of cases.
        return self.string_replacer.transform(s);
    }

    /// replaces s with the replacement if s matches '^regex$'.
    ///
    /// s is returned as is if it doesn't match '^regex$'.
    pub fn replace_full_string_slow(&self, s: &str) -> String {
        // Slow path - regexp processing
        self.expand_capture_groups(&self.replacement, s)
    }

    /// replaces all the regex matches with the replacement in s.
    pub(crate) fn replace_string_submatches_fast(&self, s: &str) -> String {
        let (prefix, complete) = self.regex_original.LiteralPrefix();
        if complete && !self.has_capture_group_in_replacement && !s.contains(prefix) {
            // Fast path - zero regex matches in s.
            return s.to_string();
        }
        // Slow path - replace all the regex matches in s with the replacement.
        self.submatch_replacer.transform(s)
    }

    /// replaces all the regex matches with the replacement in s.
    pub(crate) fn replace_string_submatches_slow(&self, s: &str) -> String {
        let res = self.regex_original.replace_all(s, &self.replacement);
        res.to_string()
    }

    fn expand_capture_groups(&self, template: &str, source: &str) -> String {
        if let Some(captures) = self.regex_anchored.captures(source) {
            let mut s = String::with_capacity(template.len() + 16);
            captures.expand(template, &mut s);
            s
        }
        source.to_string()
    }
}

impl Display for ParsedRelabelConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.rule_original)
    }
}


fn handle_graphite(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    let metric_name = get_label_value(&labels, METRIC_NAME_LABEL);
    if let Some(gmt) = &prc.graphite_match_template {
        // todo: use pool
        let mut matches: Vec<String> = Vec::with_capacity(4);
        if !gmt.is_match(&mut matches, metric_name) {
            // Fast path - name mismatch
            return;
        }
        // Slow path - extract labels from graphite metric name
        for gl in prc.graphite_label_rules.iter() {
            let value_str = gl.grt.expand(&matches);
            set_label_value(labels, labels_offset, &gl.target_label, value_str)
        }
    } else {
        return
    }
}

fn handle_replace(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    // Store `replacement` at `target_label` if the `regex` matches `source_labels` joined with `separator`
    let mut replacement = if prc.has_label_reference_in_replacement {
        let mut buf: String = String::with_capacity(128);
        // Fill {{labelName}} references in the replacement
        fill_label_references(&mut buf, &prc.replacement, &labels[labels_offset..]);
        buf
    } else {
        prc.replacement.clone()
    };

    let buf = concat_label_values(labels, &prc.source_labels, &prc.separator);
    if prc.regex_anchored == *defaultRegexForRelabelConfig && !prc.has_capture_group_in_target_label {
        if replacement == "$1" {
            // Fast path for the rule that copies source label values to destination:
            // - source_labels: [...]
            //   target_label: foobar
            let value_str = buf;
            return set_label_value(labels, labels_offset, &prc.target_label, value_str);
        }
        if !prc.has_label_reference_in_replacement {
            // Fast path for the rule that sets label value:
            // - target_label: foobar
            //   replacement: something-here
            set_label_value(labels, labels_offset, &prc.target_label, replacement);
            return;
        }
    }
    let source_str = &buf;
    if !prc.regex.match_string(source_str) {
        // Fast path - regexp mismatch.
        return;
    }
    let value_str = if replacement == prc.replacement {
        // Fast path - the replacement wasn't modified, so it is safe calling stringReplacer.Transform.
        prc.string_replacer.transform(source_str)
    } else {
        // Slow path - the replacement has been modified, so the valueStr must be calculated
        // from scratch based on the new replacement value.
        prc.expand_capture_groups(&replacement, source_str)
    };
    let mut name_str = &prc.target_label;
    if prc.has_capture_group_in_target_label {
        // Slow path - target_label contains regex capture groups, so the target_label
        // must be calculated from the regex match.
        name_str = &prc.expand_capture_groups(name_str, source_str);
    }

    set_label_value(labels, labels_offset, name_str, value_str)
}

/// Replace all the occurrences of `regex` at `source_labels` joined with `separator` with the `replacement`
/// and store the result at `target_label`
/// todo: use buffer pool
fn handle_replace_all(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, label_offset: usize) {
    let buf = concat_label_values(labels, &prc.source_labels, &prc.separator);
    let value_str = prc.replace_string_submatches_fast(&buf);
    if value_str != buf {
        set_label_value(labels, label_offset, &prc.target_label, value_str)
    }
}

fn handle_keep_if_equal(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    // Keep the entry if all the label values in source_labels are equal.
    // For example:
    //
    //   - source_labels: [foo, bar]
    //     action: keep_if_equal
    //
    // Would leave the entry if `foo` value equals `bar` value
    if !are_equal_label_values(labels, &prc.source_labels) {
        labels.truncate(labels_offset);
    }
}

/// Drop the entry if all the label values in source_labels are equal.
/// For example:
///
///   - source_labels: [foo, bar]
///     action: drop_if_equal
///
/// Would drop the entry if `foo` value equals `bar` value
fn handle_drop_if_equal(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    if are_equal_label_values(labels, &prc.source_labels) {
        labels.truncate(labels_offset);
    }
}

/// keep the entry if `source_labels` joined with `separator` matches `target_label`
fn handle_keep_equal(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    let buf = concat_label_values(&labels, &prc.source_labels, &prc.separator);
    let target_value = get_label_value(&labels[labels_offset..], &prc.target_label);
    let keep = buf == target_value;
    if keep {
        return;
    }
    labels.truncate(labels_offset);
}

fn handle_drop_equal(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    // Drop the entry if `source_labels` joined with `separator` matches `target_label`
    let buf = concat_label_values(&labels, &prc.source_labels, &prc.separator);
    let target_value = get_label_value(&labels[labels_offset..], &prc.target_label);
    let drop = buf == target_value;
    if !drop {
        return;
    }
    labels.truncate(labels_offset);
}

fn handle_keep(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    // Keep the entry if `source_labels` joined with `separator` matches `regex`
    if prc.regex_anchored == *defaultRegexForRelabelConfig {
        // Fast path for the case with `if` and without explicitly set `regex`:
        //
        // - action: keep
        //   if: 'some{label=~"filters"}'
        //
        return;
    }
    let buf = concat_label_values(&labels, &prc.source_labels, &prc.separator);
    let keep = prc.regex.match_string(&buf);
    if !keep {
        labels.truncate(labels_offset);
    }
}

/// Drop the entry if `source_labels` joined with `separator` matches `regex`
fn handle_drop(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    if prc.regex_anchored == *defaultRegexForRelabelConfig {
        // Fast path for the case with `if` and without explicitly set `regex`:
        //
        // - action: drop
        //   if: 'some{label=~"filters"}'
        //
        return;
    }

    let buf = concat_label_values(&labels, &prc.source_labels, &prc.separator);
    let drop = prc.regex.match_string(&buf);
    if drop {
        labels.truncate(labels_offset);
    }
}

/// Store the hashmod of `source_labels` joined with `separator` at `target_label`
fn handle_hashmod(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    let buf = concat_label_values(&labels, &prc.source_labels, &prc.separator);
    let hash_mod = xxh3_64(&buf.as_bytes()) % prc.modulus;
    let value_str = hash_mod.to_string();
    set_label_value(labels, labels_offset, &prc.target_label, value_str)
}

fn handle_label_map(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    // Copy `source_labels` to `target_label`
    // Replace label names with the `replacement` if they match `regex`
    for label in labels.iter() {
        let label_name = prc.replace_full_string_fast(&label.name);
        if label_name != label.name {
            let value_str = label.value.clone();
            set_label_value(labels, labels_offset, &label_name, value_str)
        }
    }
}

/// replace all the occurrences of `regex` at label names with `replacement`
fn handle_label_map_all(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, _labels_offset: usize) {
    for label in labels.iter_mut() {
        label.name = prc.replace_string_submatches_fast(&label.name)
    }
}

fn handle_label_drop(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, _labels_offset: usize) {
    // Drop all the labels matching `regex`
    labels.retain(|label| !prc.regex.match_string(&label.name))
}

fn handle_label_keep(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, _labels_offset: usize) {
    // Keep all the labels matching `regex`
    labels.retain(|label| prc.regex.match_string(&label.name))
}

fn handle_uppercase(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    let buf = concat_label_values(&labels, &prc.source_labels, &prc.separator);
    let value_str = buf.to_uppercase();
    set_label_value(labels, labels_offset, &prc.target_label, value_str)
}

fn handle_lowercase(prc: &ParsedRelabelConfig, labels: &mut Vec<Label>, labels_offset: usize) {
    let buf = concat_label_values(&labels, &prc.source_labels, &prc.separator);
    let value_str = buf.to_uppercase();
    set_label_value(labels, labels_offset, &prc.target_label, value_str)
}

fn remove_empty_labels(labels: &[Label], labels_offset: usize) -> Vec<Label> {
    let src = &labels[labels_offset..];
    src.iter().filter(|label| !label.name.is_empty() && !label.value.is_empty()).collect()
}

/// removes labels with "__" in the beginning (except "__name__").
pub(super) fn finalize_labels(dst: &mut Vec<Label>) {
    dst.retain(|label| !label.name.starts_with("__") || label.name == METRIC_NAME_LABEL);
}


fn are_equal_label_values(labels: &[Label], label_names: &[String]) -> bool {
    if label_names.len() < 2 {
        // logger.Panicf("BUG: expecting at least 2 label_names; got {}", label_names.len());
        return false;
    }
    let label_value = get_label_value(labels, &label_names[0]);
    for labelName in &label_names[1..] {
        let v = get_label_value(labels, labelName);
        if v != label_value {
            return false;
        }
    }
    return true;
}

fn concat_label_values(labels: &[Label], label_names: &[String], separator: &str) -> String {
    if label_names.is_empty() {
        return "".to_string();
    }
    let mut need_truncate = false;
    let mut dst = String::with_capacity(64); // todo: get from pool
    for label_name in label_names.iter() {
        if let Some(label) = labels.iter().find(|lbl| lbl.name == label_name) {
            dst.push_str(&label.value);
            dst.push_str(separator);
            need_truncate = true;
        }
    }
    if need_truncate {
        dst.truncate(dst.len() - separator.len());
    }
    dst
}

fn set_label_value(labels: &mut Vec<Label>, labels_offset: usize, name: &str, value: String) {
    let mut sub = &labels[labels_offset..];
    for label in sub.iter_mut() {
        if label.name == name {
            label.value = value;
            return;
        }
    }
    labels.push(Label {
        name: name.to_string(),
        value,
    })
}

static EMPTY_STRING: &str = "";

fn get_label_value<'a>(labels: &'a [Label], name: &str) -> &'a str {
    for label in labels.iter() {
        if label.name == name {
            return &label.value;
        }
    }
    return &EMPTY_STRING;
}

/// returns label with the given name from labels.
fn get_label_by_name<'a>(labels: &'a mut [Label], name: &str) -> Option<&'a mut Label> {
    for label in labels.iter_mut() {
        if &label.name == name {
            return Some(label);
        }
    }
    return None;
}


/// labels_to_string returns Prometheus string representation for the given labels.
///
/// Labels in the returned string are sorted by name,
/// while the __name__ label is put in front of {} labels.
pub fn labels_to_string(labels: &[Label]) -> String {
    let mut labels_copy = Vec::with_capacity(labels.len());
    labels_copy.sort();
    let mut mname = "";
    let mut capacity = 0;
    for label in labels.iter() {
        if label.name == METRIC_NAME_LABEL {
            mname = &label.value;
            capacity += label.value.len();
        } else {
            capacity += label.name.len() + label.value.len() + 2;
            labels_copy.push(label);
        }
    }
    if !mname.is_empty() && labels_copy.is_empty() {
        return mname.to_string();
    }
    let mut b = String::with_capacity(capacity);
    b.push('{');
    for (i, label) in labels_copy.iter().enumerate() {
        b.push_str(&label.name);
        b.push('=');
        b.push_str(&*enquote('"', &label.value));
        if i + 1 < labels_copy.len() {
            b.push(',');
        }
    }
    b.push('}');
    b
}

pub(super) fn fill_label_references(dst: &mut String, replacement: &str, labels: &[Label]) {
    let mut s = replacement;
    while !s.is_empty() {
        if let Some(n) = s.find("{{") {
            dst.push_str(&s[0..n]);
            s = &s[n + 2..];
        } else {
            dst.push_str(s);
            return;
        }
        if let Some(n) = s.find("}}") {
            let label_name = &s[0..n];
            let label_value = get_label_value(labels, label_name);
            s = &s[n + 2..];
            dst.push_str(label_value);
        } else {
            dst.push_str("{{");
            dst.push_str(s);
            return;
        }
    }
}

static UNSUPPORTED_METRIC_NAME_CHARS_REGEX: OnceLock<Regex> = OnceLock::new();
static UNSUPPORTED_LABEL_NAME_REGEX: OnceLock<Regex> = OnceLock::new();

/// unsupported_label_name_chars_regex returns regex for unsupported chars in label names.
fn unsupported_label_name_chars_regex() -> Regex {
    UNSUPPORTED_LABEL_NAME_REGEX.call_once(|| {
        Regex::new(r"[^a-zA-Z0-9_]").unwrap()
    })
}

fn unsupported_metric_name_chars_regex() -> Regex {
    UNSUPPORTED_METRIC_NAME_CHARS_REGEX.call_once(|| {
        Regex::new(r"[^a-zA-Z0-9_:]").unwrap()
    })
}

pub fn is_valid_metric_name(name: &str) -> bool {
    let re = unsupported_metric_name_chars_regex();
    !re.is_match(name)
}

pub fn is_valid_label_name(name: &str) -> bool {
    let re = unsupported_label_name_chars_regex();
    !re.is_match(name)
}

static LABEL_NAME_SANITIZER: OnceLock<FastStringTransformer> = OnceLock::new();

fn get_metric_name_sanitizer() -> &'static FastStringTransformer {
    static METRIC_NAME_SANITIZER: OnceLock<FastStringTransformer> = OnceLock::new();
    METRIC_NAME_SANITIZER.get_or_init(|| {
        FastStringTransformer::new(|s: &str| -> String {
            return unsupported_metric_name_chars_regex().replace_all(s, "_").to_string();
        })
    })
}


fn label_name_sanitizer() -> &'static FastStringTransformer {
    LABEL_NAME_SANITIZER.get_or_init(|| {
        FastStringTransformer::new(|s: &str| -> String {
            return unsupported_label_name_chars_regex().replace_all(s, "_").to_string();
        })
    })
}

/// sanitize_label_name replaces unsupported by Prometheus chars in label names with _.
///
/// See https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
pub fn sanitize_label_name(name: &str) -> String {
    label_name_sanitizer().transform(name)
}

/// sanitize_metric_name replaces unsupported by Prometheus chars in metric names with _.
///
// See https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
pub fn sanitize_metric_name(value: &str) -> String {
    return get_metric_name_sanitizer().transform(value);
}