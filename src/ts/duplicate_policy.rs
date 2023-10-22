use serde::{Deserialize, Serialize};
use std::fmt::Display;
use crate::common::types::Sample;

#[derive(Debug, Default, PartialEq, Deserialize, Serialize, Clone, Copy)]
pub enum DuplicatePolicy {
    /// ignore any newly reported value and reply with an error
    #[default]
    Block,
    /// ignore any newly reported value
    First,
    /// overwrite the existing value with the new value
    Last,
    /// only override if the value is lower than the existing value
    Min,
    /// only override if the value is higher than the existing value
    Max,
    /// append the new value to the existing value
    Sum,
}

impl Display for DuplicatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DuplicatePolicy::Block => write!(f, "block"),
            DuplicatePolicy::First => write!(f, "first"),
            DuplicatePolicy::Last => write!(f, "last"),
            DuplicatePolicy::Min => write!(f, "min"),
            DuplicatePolicy::Max => write!(f, "max"),
            DuplicatePolicy::Sum => write!(f, "sum"),
        }
    }
}

impl From<&str> for DuplicatePolicy {
    fn from(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "block" => DuplicatePolicy::Block,
            "first" => DuplicatePolicy::First,
            "last" => DuplicatePolicy::Last,
            "min" => DuplicatePolicy::Min,
            "max" => DuplicatePolicy::Max,
            "sum" => DuplicatePolicy::Sum,
            _ => panic!("invalid duplicate policy: {}", s),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DuplicateStatus {
    Ok,
    Err,
    Deduped,
}

// This function will decide according to the policy how to handle duplicate sample, the `new_sample`
// will contain the data that will be kept in the database.
pub fn handle_duplicate_sample(
    policy: DuplicatePolicy,
    old_sample: Sample,
    new_sample: &mut Sample,
) -> DuplicateStatus {
    let has_nan = old_sample.value.is_nan() || new_sample.value.is_nan();
    if has_nan && policy != DuplicatePolicy::Block {
        // take the valid sample regardless of policy
        if new_sample.value.is_nan() {
            new_sample.value = old_sample.value;
        }
        return DuplicateStatus::Ok;
    }
    match policy {
        DuplicatePolicy::Block => DuplicateStatus::Err,
        DuplicatePolicy::First => {
            // keep the first sample
            new_sample.value = old_sample.value;
            return DuplicateStatus::Ok;
        }
        DuplicatePolicy::Last => {
            // keep the last sample
            return DuplicateStatus::Ok;
        }
        DuplicatePolicy::Min => {
            // keep the min sample
            if old_sample.value < new_sample.value {
                new_sample.value = old_sample.value;
            }
            return DuplicateStatus::Ok;
        }
        DuplicatePolicy::Max => {
            // keep the max sample
            if old_sample.value > new_sample.value {
                new_sample.value = old_sample.value;
            }
            return DuplicateStatus::Ok;
        }
        DuplicatePolicy::Sum => {
            // sum the samples
            new_sample.value += old_sample.value;
            return DuplicateStatus::Ok;
        }
    };
    DuplicateStatus::Err
}
