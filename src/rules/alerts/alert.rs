use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use ahash::AHashMap;

use gtmpl::{Context, Template};
use gtmpl_derive::Gtmpl;
use serde::{Deserialize, Serialize};
use crate::common::types::Label;

use crate::rules::alerts::{AlertsError, AlertsResult, ErrorGroup};
use crate::rules::alerts::template::{clone_template, funcs_with_query, get_template, get_with_funcs, QueryFn};
use crate::rules::relabel::ParsedRelabelConfig;
use crate::ts::Timestamp;

/// AlertState is the state of an alert.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlertState {
    /// Inactive is the state of an alert that is neither firing nor pending.
    Inactive,
    /// Pending is the state of an alert that has been active for less than the configured threshold duration.
    Pending,
    /// Firing is the state of an alert that has been active for longer than the configured threshold duration.
    Firing,
}

impl Display for AlertState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            AlertState::Inactive => "inactive",
            AlertState::Pending => "pending",
            AlertState::Firing => "firing",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for AlertState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "inactive" => Ok(AlertState::Inactive),
            "pending" => Ok(AlertState::Pending),
            "firing" => Ok(AlertState::Firing),
            _ => Err(format!("unknown alert state: {}", s)),
        }
    }
}

/// the triggered alert
// TODO: Looks like alert name isn't unique
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Alert {
    /// group_id contains the id of the parent rules group
    pub group_id: u64,
    /// name represents Alert name
    pub name: String,
    /// labels is the list of label-value pairs attached to the Alert
    pub labels: AHashMap<String, String>,
    /// Annotations is the list of annotations generated on Alert evaluation
    pub annotations: AHashMap<String, String>,
    /// state represents the current state of the Alert
    pub state: AlertState,
    /// expr contains the expression that was executed to generate the Alert
    pub expr: String,
    /// active_at defines the moment of time when the Alert has become active
    pub active_at: Timestamp,
    /// start defines the moment of time when the alert starts firing
    pub start: Timestamp,
    /// end defines the moment of time when the alert is set to expire
    pub end: Timestamp,
    /// resolved_at defines the moment when Alert was switched from Firing to Inactive
    pub resolved_at: Timestamp,
    /// last_sent defines the moment when Alert was sent last time
    pub last_sent: Timestamp,
    /// defines the moment when Alert::Firing was kept because of `keep_firing_for` instead of real alert
    pub keep_firing_since: Timestamp,
    /// value stores the value returned from evaluating expression from expr field
    pub value: f64,
    /// id is the unique identifier for the Alert
    pub id: u64,
    /// restored is true if Alert was restored after restart
    pub restored: bool,
    /// for defines for how long Alert needs to be active to become StateFiring
    pub r#for: Duration,

    pub external_url: String,
}

/// alert_tpl_data is used to execute templating
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[derive(Gtmpl)]
pub struct AlertTplData {
    pub labels: AHashMap<String, String>,
    pub value: f64,
    pub expr: String,
    pub alert_id: u64,
    pub group_id: u64,
    pub active_at: Timestamp,
    pub r#for: Duration,
}

const TPL_HEADERS: String = [
    "{{ $value := .value }}",
    "{{ $labels := .labels }}",
    "{{ $expr := .expr }}",
    "{{ $externalLabels := .external_labels }}",
    "{{ $externalURL := .external_url }}",
    "{{ $alertID := .alertID }}",
    "{{ $groupID := .group_id }}",
    "{{ $activeAt := .active_id }}",
    "{{ $for := .for }}",
].join("");


impl Alert {
    /// exec_template executes the Alert template for given map of annotations.
    /// Every alert could have a different provider, so function requires a queryFunction
    /// as an argument.
    pub fn exec_template(&mut self, q: QueryFn, labels: &AHashMap<String, String>, annotations: &AHashMap<String, String>) -> AlertsResult<AHashMap<String, String>> {
        let tpl_data = AlertTplData {
            value: self.value,
            labels: labels.clone(), // ??? why not use ref ?
            expr: self.expr.clone(),  // todo(perf) why not use ref ?
            alert_id: self.id,
            active_at: self.active_at,
            r#for: self.r#for.clone(),
            group_id: self.group_id,
        };
        exec_template(q, annotations, &tpl_data)
    }


    pub fn to_prom_labels(&self, relabel_cfg: Option<ParsedRelabelConfig>) -> Vec<Label> {
        let mut labels = Vec::with_capacity(self.labels.len());
        for (k, v) in self.labels.iter() {
            labels.push(Label {
                name: k.clone(),
                value: v.clone(),
            });
        }
        if let Some(mut relabelCfg) = relabel_cfg {
            relabelCfg.apply(&mut labels, 0);
        }
        labels.sort();
        return labels;
    }
}

/// exec_template executes the given template for given annotations map.
pub fn exec_template(q: QueryFn, annotations: &AHashMap<String, String>, tpl_data: &AlertTplData) -> AlertsResult<AHashMap<String, String>> {
    let tmpl = get_with_funcs(funcs_with_query(q))?;
    return template_annotations(annotations, tpl_data, &tmpl);
}


/// validate annotations for possible template error, uses empty data for template population
pub(crate) fn validate_templates(annotations: &AHashMap<String, String>) -> AlertsResult<()> {
    let tmpl = get_template();
    let labels = AHashMap::new();
    let _ = template_annotations(annotations, &AlertTplData {
        labels,
        value: 1f64,
        expr: "up = 0".to_string(),
        alert_id: 0,
        group_id: 1,
        active_at: 0,
        r#for: Default::default(),
    }, &tmpl);
}

fn template_annotations(annotations: &AHashMap<String, String>, template_data: &AlertTplData, tmpl: &Template) -> AlertsResult<AHashMap<String, String>> {
    let mut builder = String::with_capacity(256);
    let mut r = AHashMap::with_capacity(annotations.len());
    let mut err_group = Vec::with_capacity(annotations.len());

    let header_len = TPL_HEADERS.len();
    for (key, text) in annotations {
        builder.clear();
        builder.resize(header_len + text.len());
        builder.push_str(&TPL_HEADERS);
        builder.push_str(&text);

        let output = template_annotation(&builder, template_data, tmpl);
        match output {
            Ok(text) => {
                r.insert(key.to_string(), text.to_string());
            }
            Err(err) => {
                let msg = format!("key {key}, template {text}: {:?}", err);
                err_group.push(msg);
            }
        }
    }

    if err_group.is_empty() {
        return Ok(r);
    }
    Err(AlertsError::TemplateExecutionError(ErrorGroup(err_group)))
}

fn template_annotation(text: &str, data: &AlertTplData, tmpl: &Template) -> AlertsResult<String> {
    let mut tpl = clone_template(tmpl); // ??????
    tpl.parse(text)
        .map_err(|err| {
            return AlertsError::TemplateParseError(format!("{:?}", err));
        })?;

    let context = Context::from(data);
    tpl.render(&context)
        .map_err(|err| {
            AlertsError::Generic(format!("error evaluating annotation template: {}", err))
        })
}
