use crate::common::types::Sample;
use crate::ts::{DuplicatePolicy, DuplicateStatus};

// This function will decide according to the policy how to handle duplicate sample, the `new_sample`
// will contain the data that will be kept in the database.
pub fn handle_duplicate_sample(
    policy: DuplicatePolicy,
    old_sample: Sample,
    new_sample: &mut Sample,
) -> DuplicateStatus {
    use DuplicatePolicy::*;
    let has_nan = old_sample.value.is_nan() || new_sample.value.is_nan();
    if has_nan && policy != Block {
        // take the valid sample regardless of policy
        if new_sample.value.is_nan() {
            new_sample.value = old_sample.value;
        }
        return DuplicateStatus::Ok;
    }
    match policy {
        Block => return DuplicateStatus::Err,
        First => {
            // keep the first sample
            new_sample.value = old_sample.value;
        }
        Last => {},
        Min => {
            // keep the min sample
            new_sample.value = old_sample.value.min(new_sample.value);
        }
        Max => {
            // keep the max sample
            new_sample.value = old_sample.value.max(new_sample.value);
        }
        Sum => {
            // sum the samples
            new_sample.value += old_sample.value;
        }
    }
    DuplicateStatus::Ok
}
