use serde::{Deserialize, Serialize};

use crate::relabel::actions::Action;
use crate::relabel::utils::contains_all_label_values;
use crate::storage::Label;

/// Keep the entry if target_label contains all the label values listed in source_labels.
/// For example, the following relabeling rule would leave the entry if __meta_consul_tags
/// contains values of __meta_required_tag1 and __meta_required_tag2:
///
///   - action: keep_if_contains
///     target_label: __meta_consul_tags
///     source_labels: [__meta_required_tag1, __meta_required_tag2]
///
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeepIfContainsAction {
    pub source_labels: Vec<String>,
    pub target_label: String,
}

impl KeepIfContainsAction {
    pub fn new(source_labels: Vec<String>, target_label: String) -> Result<Self, String> {
        if source_labels.is_empty() {
            return Err("missing `source_labels` for `action=keep_if_contains`".to_string());
        }
        Ok(Self {
            source_labels,
            target_label,
        })
    }
}

impl Action for KeepIfContainsAction {
    fn apply(&self, labels: &mut Vec<Label>, labels_offset: usize) {
        if contains_all_label_values(labels, &self.target_label, &self.source_labels) {
            return
        }
        labels.truncate(labels_offset);
    }
}