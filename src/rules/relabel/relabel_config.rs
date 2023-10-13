use crate::common::regex_util::{remove_start_end_anchors, simplify, PromRegex};
use crate::rules::relabel::relabel::ParsedRelabelConfig;
use crate::rules::relabel::{new_graphite_label_rules, GraphiteLabelRule, GraphiteMatchTemplate, IfExpression, DebugStep, labels_to_string};
use metricsql_engine::METRIC_NAME_LABEL;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;
use lazy_static::lazy_static;
use crate::common::FastStringTransformer;
use crate::common::types::Label;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub enum RelabelAction {
    Graphite,
    #[default]
    Replace,
    ReplaceAll,
    KeepIfEqual,
    DropIfEqual,
    KeepEqual,
    DropEqual,
    Keep,
    Drop,
    HashMod,
    KeepMetrics,
    DropMetrics,
    Uppercase,
    Lowercase,
    LabelMap,
    LabelMapAll,
    LabelDrop,
    LabelKeep,
}

impl Display for RelabelAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RelabelAction::*;
        match self {
            Graphite => write!(f, "graphite"),
            Replace => write!(f, "replace"),
            ReplaceAll => write!(f, "replace_all"),
            KeepIfEqual => write!(f, "keep_if_equal"),
            DropIfEqual => write!(f, "drop_if_equal"),
            KeepEqual => write!(f, "keepequal"),
            DropEqual => write!(f, "dropequal"),
            Keep => write!(f, "keep"),
            Drop => write!(f, "drop"),
            HashMod => write!(f, "hashmod"),
            KeepMetrics => write!(f, "keep_metrics"),
            DropMetrics => write!(f, "drop_metrics"),
            Uppercase => write!(f, "uppercase"),
            Lowercase => write!(f, "lowercase"),
            LabelMap => write!(f, "labelmap"),
            LabelMapAll => write!(f, "labelmap_all"),
            LabelDrop => write!(f, "labeldrop"),
            LabelKeep => write!(f, "labelkeep"),
        }
    }
}

impl FromStr for RelabelAction {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use RelabelAction::*;
        match s.to_lowercase().as_str() {
            "graphite" => Ok(Graphite),
            "replace" => Ok(Replace),
            "replace_all" => Ok(ReplaceAll),
            "keep_if_equal" => Ok(KeepIfEqual),
            "drop_if_equal" => Ok(DropIfEqual),
            "keepequal" => Ok(KeepEqual),
            "dropequal" => Ok(DropEqual),
            "keep" => Ok(Keep),
            "drop" => Ok(Drop),
            "hashmod" => Ok(HashMod),
            "keep_metrics" => Ok(KeepMetrics),
            "drop_metrics" => Ok(DropMetrics),
            "uppercase" => Ok(Uppercase),
            "lowercase" => Ok(Lowercase),
            "labelmap" => Ok(LabelMap),
            "labelmap_all" => Ok(LabelMapAll),
            "labeldrop" => Ok(LabelDrop),
            "labelkeep" => Ok(LabelKeep),
            _ => Err(format!("unknown action: {}", s)),
        }
    }
}

/// RelabelConfig represents relabel config.
///
/// See https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct RelabelConfig {
    #[serde(rename = "if")]
    pub if_expr: Option<IfExpression>,
    pub action: RelabelAction,
    pub source_labels: Vec<String>,
    pub separator: String,
    pub target_label: String,
    pub regex: Option<Regex>,
    pub modulus: u64,
    pub replacement: String,

    // match is used together with Labels for `action: graphite`. For example:
    // - action: graphite
    //   match: 'foo.*.*.bar'
    //   labels:
    //     job: '$1'
    //     instance: '${2}:8080'
    #[serde(rename = "match")]
    pub r#match: String,

    // Labels is used together with match for `action: graphite`. For example:
    // - action: graphite
    //   match: 'foo.*.*.bar'
    //   labels:
    //     job: '$1'
    //     instance: '${2}:8080'
    pub labels: HashMap<String, String>,
}

pub struct ParsedConfigs(pub Vec<ParsedRelabelConfig>);

impl ParsedConfigs {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// apply_debug applies pcs to labels in debug mode.
    ///
    /// It returns DebugStep list - one entry per each applied relabeling step.
    pub fn apply_debug(&self, labels: &mut Vec<Label>) -> Vec<DebugStep> {
        self.apply_internal(labels, 0, true)
    }

    /// applies self to labels starting from the labelsOffset.
    pub fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        let _ = self.apply_internal(labels, labels_offset, false);
    }
    fn apply_internal(&self, labels: &mut Vec<Label>, labels_offset: usize, debug: bool) -> Vec<DebugStep> {
        let mut dss: Vec<DebugStep> = Vec::with_capacity(labels.len());
        let mut in_str: String = "".to_string();
        if debug {
            in_str = labels_to_string(labels[&labels_offset.. ])
        }
        for prc in self.0.iter() {
            let labels = prc.apply(labels, labels_offset);
            if debug {
                let out_str = labels_to_string(labels[labels_offset .. ]);
                dss.push(DebugStep{
                    rule: prc.to_string(),
                    r#in: in_str,
                    out: out_str.clone(),
                });
                in_str = out_str
            }
            if labels.len() == labels_offset {
                // All the labels have been removed.
                return dss;
            }
        }

        remove_empty_labels(labels, labels_offset);
        if debug {
            let out_str = labels_to_string(&labels[labels_offset .. ]);
            if out_str != in_str {
                dss.push(DebugStep{
                    rule: "remove empty labels".to_string(),
                    r#in: in_str,
                    out: out_str,
                });
            }
        }

        dss
    }
}

// todo:
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/lib/promrelabel/config.go#L123
impl Display for ParsedConfigs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        for prc in &self.0 {
            s.push_str(&prc.to_string());
        }
        write!(f, "{}", s)
    }
}

/// parses relabel configs from the given data.
pub fn parse_relabel_configs_data(data: &str) -> Result<ParsedConfigs, String> {
    let rcs: Vec<RelabelConfig> = serde_yaml::from_str(data)
        .map_err(|e| format!("cannot parse relabel configs from data: {:?}", e))?;
    return parse_relabel_configs(&rcs);
}

/// parse_relabel_configs parses rcs to dst.
pub fn parse_relabel_configs(rcs: &[RelabelConfig]) -> Result<ParsedConfigs, String> {
    if rcs.is_empty() {
        return Ok(ParsedConfigs::default());
    }
    let mut prcs: Vec<ParsedRelabelConfig> = Vec::with_capacity(rcs.len());
    for (i, item) in rcs.iter().enumerate() {
        let prc = parse_relabel_config(item);
        if let Err(err) = prc {
            return Err(format!(
                "error when parsing `relabel_config` #{}: {:?}",
                i + 1,
                err
            ));
        }
        prcs.push(prc.unwrap());
    }
    Ok(ParsedConfigs(prcs))
}

lazy_static! {
    pub static ref defaultOriginalRegexForRelabelConfig: Regex = Regex::new(".*");
    pub static ref defaultRegexForRelabelConfig:Regex = Regex::new("^(.*)$");
}

pub fn parse_relabel_config(rc: &RelabelConfig) -> Result<ParsedRelabelConfig, String> {
    use RelabelAction::*;

    let mut source_labels = &rc.source_labels;
    let mut separator = ";";

    if !rc.separator.is_empty() {
        separator = &rc.separator;
    }

    let mut target_label = &rc.target_label;
    let reg_str = rc.regex.to_string();
    let (regex_anchored, regex_original_compiled, prom_regex ) = if !is_empty_regex(&rc.regex) && !is_default_regex(&reg_str) {
        let mut regex = reg_str;
        let mut regex_orig = regex;
        if rc.action != ReplaceAll && rc.action != LabelMapAll {
            let stripped = remove_start_end_anchors(&regex);
            regex_orig = stripped.to_string();
            regex = format!("^(?:{stripped})$");
        }
        let regex_anchored =
            Regex::new(&regex).map_err(|e| format!("cannot parse `regex` {regex}: {:?}", e))?;

        let regex_original_compiled = Regex::new(&regex_orig)
            .map_err(|e| format!("cannot parse `regex` {regex_orig}: {:?}", e))?;

        let prom_regex = PromRegex::new(&regex_orig)
            .map_err(|err|
                format!("BUG: cannot parse already parsed regex {}: {:?}", regex_orig, err)
            )?;

        (regex_anchored, regex_original_compiled, prom_regex)
    } else {
        (
            defaultRegexForRelabelConfig.clone(),
            defaultOriginalRegexForRelabelConfig.clone(),
            PromRegex::new(".*").unwrap()
        )
    };
    let modulus = rc.modulus;
    let mut replacement = if !rc.replacement.is_empty() {
        rc.replacement.clone()
    } else {
        "$1"
    };
    let mut graphite_match_template: Option<GraphiteMatchTemplate> = None;
    if !rc.r#match.is_empty() {
        graphite_match_template = Some(GraphiteMatchTemplate::new(&rc.r#match));
    }
    let mut graphite_label_rules: Vec<GraphiteLabelRule> = vec![];
    if !rc.labels.is_empty() {
        graphite_label_rules = new_graphite_label_rules(&rc.labels)
    }

    let mut action = rc.action.clone();
    match rc.action {
        Graphite => {
            if graphite_match_template.is_none() {
                return Err(format!("missing `match` for `action=graphite`; see https://docs.victoriametrics.com/vmagent.html#graphite-relabeling"));
            }
            if graphite_label_rules.is_empty() {
                return Err(format!("missing `labels` for `action=graphite`; see https://docs.victoriametrics.com/vmagent.html#graphite-relabeling"));
            }
            if !rc.source_labels.is_empty() {
                return Err(format!("`source_labels` cannot be used with `action=graphite`; see https://docs.victoriametrics.com/vmagent.html#graphite-relabeling"));
            }
            if !rc.target_label.is_empty() {
                return Err(format!("`target_label` cannot be used with `action=graphite`; see https://docs.victoriametrics.com/vmagent.html#graphite-relabeling"));
            }
            if !rc.replacement.is_empty() {
                return Err(format!("`replacement` cannot be used with `action=graphite`; see https://docs.victoriametrics.com/vmagent.html#graphite-relabeling"));
            }
            if rc.regex.is_some() {
                return Err(format!("`regex` cannot be used for `action=graphite`; see https://docs.victoriametrics.com/vmagent.html#graphite-relabeling"));
            }
        }
        Replace => {
            if target_label.is_empty() {
                return Err(format!("missing `target_label` for `action=replace`"));
            }
        }
        ReplaceAll => {
            if source_labels.is_empty() {
                return Err(format!("missing `source_labels` for `action=replace_all`"));
            }
            if target_label.is_empty() {
                return Err(format!("missing `target_label` for `action=replace_all`"));
            }
        }
        KeepIfEqual => {
            if source_labels.len() < 2 {
                return Err(format!("`source_labels` must contain at least two entries for `action=keep_if_equal`; got {:?}", source_labels));
            }
            if !target_label.is_empty() {
                return Err(format!(
                    "`target_label` cannot be used for `action=keep_if_equal`"
                ));
            }
            if rc.regex.is_some() {
                return Err(format!("`regex` cannot be used for `action=keep_if_equal`"));
            }
        }
        DropIfEqual => {
            if source_labels.len() < 2 {
                return Err(format!("`source_labels` must contain at least two entries for `action=drop_if_equal`; got {:?}", source_labels));
            }
            if !target_label.is_empty() {
                return Err(format!(
                    "`target_label` cannot be used for `action=drop_if_equal`"
                ));
            }
            if rc.regex.is_some() {
                return Err(format!("`regex` cannot be used for `action=drop_if_equal`"));
            }
        }
        KeepEqual => {
            if target_label.is_empty() {
                return Err(format!("missing `target_label` for `action=keepequal`"));
            }
            if rc.regex.is_some() {
                return Err(format!("`regex` cannot be used for `action=keepequal`"));
            }
        }
        DropEqual => {
            if target_label.is_empty() {
                return Err(format!("missing `target_label` for `action=dropequal`"));
            }
            if rc.regex.is_some() {
                return Err(format!("`regex` cannot be used for `action=dropequal`"));
            }
        }
        Keep => {
            if source_labels.is_empty() && rc.if_expr.is_none() {
                return Err(format!("missing `source_labels` for `action=keep`"));
            }
        }
        Drop => {
            if source_labels.is_empty() && rc.if_expr.is_none() {
                return Err(format!("missing `source_labels` for `action=drop`"));
            }
        }
        HashMod => {
            if source_labels.is_empty() {
                return Err(format!("missing `source_labels` for `action=hashmod`"));
            }
            if target_label.is_empty() {
                return Err(format!("missing `target_label` for `action=hashmod`"));
            }
            if modulus < 1 {
                return Err(format!(
                    "unexpected `modulus` for `action=hashmod`: {modulus}; must be greater than 0"
                ));
            }
        }
        KeepMetrics => {
            if is_empty_regex(&rc.regex) && rc.if_expr.is_none() {
                return Err(format!(
                    "`regex` must be non-empty for `action=keep_metrics`"
                ));
            }
            if source_labels.len() > 0 {
                return Err(format!(
                    "`source_labels` must be empty for `action=keep_metrics`; got {:?}",
                    source_labels
                ));
            }
            source_labels = &vec![METRIC_NAME_LABEL.to_string()];
            action = Keep;
        }
        DropMetrics => {
            if is_empty_regex(&rc.regex) && rc.if_expr.is_none() {
                return Err(format!(
                    "`regex` must be non-empty for `action=drop_metrics`"
                ));
            }
            if source_labels.len() > 0 {
                return Err(format!(
                    "`source_labels` must be empty for `action=drop_metrics`; got {:?}",
                    source_labels
                ));
            }
            source_labels = &vec![METRIC_NAME_LABEL.to_string()];
            action = Drop;
        }
        Uppercase | Lowercase => {
            if source_labels.is_empty() {
                return Err(format!("missing `source_labels` for `action=uppercase`"));
            }
            if target_label.is_empty() {
                return Err(format!("missing `target_label` for `action=uppercase`"));
            }
        }
        LabelMap | LabelMapAll | LabelDrop | LabelKeep => {
            if source_labels.is_empty() {
                return Err(format!("missing `source_labels` for `action=labelmap`"));
            }
            if target_label.is_empty() {
                return Err(format!("missing `target_label` for `action=labelmap`"));
            }
            if rc.regex.is_some() {
                return Err(format!("`regex` cannot be used for `action=labelmap`"));
            }
        }
        _ => {
            return Err(format!("unknown `action` {action}"));
        }
    }
    if action != Graphite {
        if graphite_match_template.is_some() {
            return Err(format!("`match` config cannot be applied to `action={}`; it is applied only to `action=graphite`", action));
        }
        if !graphite_label_rules.is_empty() {
            return Err(format!("`labels` config cannot be applied to `action={}`; it is applied only to `action=graphite`", action));
        }
    }

    let rule_original = match serde_yaml::to_string(&rc) {
        Ok(data) => data,
        Err(err) => {
            panic!("BUG: cannot marshal RelabelConfig to yaml: {:?}", err);
        }
    };

    let mut prc = ParsedRelabelConfig {
        rule_original: rule_original.to_string(),
        source_labels,
        separator: separator.to_string(),
        target_label: target_label.to_string(),
        regex_anchored,
        modulus,
        action,
        r#if: rc.if_expr.clone(),
        graphite_match_template,
        graphite_label_rules,
        regex: prom_regex,
        regex_original: regex_original_compiled,
        string_replacer: Default::default(),
        has_capture_group_in_target_label: target_label.contains("$"),
        has_capture_group_in_replacement: replacement.contains("$"),
        has_label_reference_in_replacement: replacement.contains("{{"),
        replacement,
        submatch_replacer: Default::default(),
    };
    prc.string_replacer = FastStringTransformer::new(prc.replace_full_string_slow);
    prc.submatch_replacer = FastStringTransformer::new(prc.replace_string_submatches_slow);
    Ok(prc)
}

fn is_default_regex(expr: &str) -> bool {
    let (prefix, suffix) = simplify(expr);
    if prefix != "" {
        return false;
    }
    return suffix == ".*";
}

fn is_empty_regex(regex: &Option<Regex>) -> bool {
    regex.is_none() || regex.as_ref().unwrap().as_str() == ""
}

fn remove_empty_labels(labels: &mut Vec<Label>, labels_offset: usize) {
    let mut i: usize = labels.len() - 1;
    while i >= labels_offset {
        let label = &labels[i];
        if label.name.is_empty() || label.value.is_empty() {
            labels.remove(i);
        }
        i -= 1;
    }
}