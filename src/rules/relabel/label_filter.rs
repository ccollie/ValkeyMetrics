use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::common::regex_util::PromRegex;
use crate::common::types::Label;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LabelFilterOp {
    Equal,
    NotEqual,
    MatchRegexp,
    NotMatchRegexp,
}

impl LabelFilterOp {
    pub fn is_regex(&self) -> bool {
        match self {
            LabelFilterOp::MatchRegexp | LabelFilterOp::NotMatchRegexp => true,
            _ => false,
        }
    }

    pub fn is_negative(&self) -> bool {
        match self {
            LabelFilterOp::NotEqual | LabelFilterOp::NotMatchRegexp => true,
            _ => false,
        }
    }
}

impl fmt::Display for LabelFilterOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LabelFilterOp::Equal => write!(f, "="),
            LabelFilterOp::NotEqual => write!(f, "!="),
            LabelFilterOp::MatchRegexp => write!(f, "=~"),
            LabelFilterOp::NotMatchRegexp => write!(f, "!~"),
        }
    }
}

impl From<&str> for LabelFilterOp {
    fn from(s: &str) -> Self {
        match s {
            "=" => LabelFilterOp::Equal,
            "!=" => LabelFilterOp::NotEqual,
            "=~" => LabelFilterOp::MatchRegexp,
            "!~" => LabelFilterOp::NotMatchRegexp,
            _ => panic!("BUG: unexpected operation for label filter: {}", s),
        }
    }
}


/// labelFilter contains PromQL filter for `{label op "value"}`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LabelFilter {
    pub label: String,
    pub op: LabelFilterOp,
    // todo: enum
    pub value: String,

    // re contains compiled regexp for `=~` and `!~` op.
    pub re: Option<PromRegex>,
}

impl LabelFilter {
    pub fn matches(&self, labels: &[Label]) -> bool {
        match self.op {
            LabelFilterOp::Equal => self.equal_value(labels),
            LabelFilterOp::NotEqual => !self.equal_value(labels),
            LabelFilterOp::MatchRegexp => self.match_regexp(labels),
            LabelFilterOp::NotMatchRegexp => !self.match_regexp(labels),
        }
    }

    fn equal_value(&self, labels: &[Label]) -> bool {
        let mut label_name_matches = 0;
        for label in labels {
            if to_canonical_label_name(&label.name) != self.label {
                continue;
            }
            label_name_matches += 1;
            if &label.value == self.value {
                return true;
            }
        }
        if label_name_matches == 0 {
            // Special case for {non_existing_label=""}, which matches anything except of non-empty non_existing_label
            return self.value == "";
        }
        return false;
    }

    fn match_regexp(&self, labels: &[Label]) -> bool {
        let mut label_name_matches = 0;
        let re = &self.re.unwrap();

        for label in labels {
            if to_canonical_label_name(&label.name) != self.label {
                continue;
            }
            label_name_matches += 1;
            if re.match_string(&label.value) {
                return true;
            }
        }
        if label_name_matches == 0 {
            // Special case for {non_existing_label=~"something|"}, which matches empty non_existing_label
            return re.match_string("");
        }
        return false;
    }
}

pub(super) fn to_canonical_label_name(label_name: &str) -> &str {
    if label_name == "__name__" {
        return "";
    }
    label_name
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LabelMatchers(Vec<LabelFilter>);

impl LabelMatchers {
    pub fn new(filters: Vec<LabelFilter>) -> Self {
        LabelMatchers(filters)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// find the matcher's value whose name equals the specified name. This function
    /// is designed to prepare error message of invalid promql expression.
    pub fn find_matcher_value(&self, name: &str) -> Option<String> {
        for m in &self.0 {
            if m.label.eq(name) {
                return Some(m.value.clone());
            }
        }
        None
    }

    /// find matchers whose name equals the specified name
    pub fn find_matchers(&self, name: &str) -> Vec<LabelFilter> {
        self.0
            .iter()
            .filter(|m| m.label.eq(name))
            .cloned()
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn push(&mut self, m: LabelFilter) {
        self.0.push(m);
    }

    pub fn sort(&mut self) {
        self.0.sort();
    }

    pub fn iter(&self) -> impl Iterator<Item=&LabelFilter> {
        self.0.iter()
    }
}

impl fmt::Display for LabelMatchers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self.0)
    }
}

impl Deref for LabelMatchers {
    type Target = Vec<LabelFilter>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}