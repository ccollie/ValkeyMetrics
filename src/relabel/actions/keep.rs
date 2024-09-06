use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::relabel::actions::Action;
use crate::relabel::is_default_regex_for_config;
use crate::relabel::regex_parse::parse_regex;
use crate::relabel::utils::concat_label_values;
use crate::storage::Label;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeepAction {
    pub source_labels: Vec<String>,
    pub separator: String,
    pub regex: PromRegex,
    pub regex_anchored: Regex,
}

impl KeepAction {
    pub fn new(source_labels: Vec<String>,
               separator: String,
               regex: Option<Regex>) -> Result<Self, String> {

        // if source_labels.is_empty() && if_expr.is_none() {
        //     return Err("missing `source_labels` for `action=keep`".to_string());
        // }

        let (regex_anchored, _, prom_regex)  = parse_regex(regex, true)?;

        Ok(Self {
            source_labels,
            separator,
            regex: prom_regex,
            regex_anchored
        })
    }
}

impl Action for KeepAction {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        // Keep the entry if `source_labels` joined with `separator` matches `regex`
        if is_default_regex_for_config(&self.regex_anchored) {
            // Fast path for the case with `if` and without explicitly set `regex`:
            //
            // - action: keep
            //   if: 'some{label=~"filters"}'
            //
            return;
        }
        let buf = concat_label_values(&labels, &self.source_labels, &self.separator);
        let keep = self.regex.is_match(&buf);
        if !keep {
            labels.truncate(labels_offset);
        }
    }
}
