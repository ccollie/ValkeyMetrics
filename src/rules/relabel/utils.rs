use metricsql_engine::parse_metric_selector;
use crate::common::types::Label;

/// new_labels_from_string creates labels from s, which can have the form `metric{labels}`.
///
/// This function must be used only in non performance-critical code, since it allocates too much
pub fn new_labels_from_string(metric_with_labels: &str) -> Vec<Label> {
    // add a value to metric_with_labels, so it could be parsed by prometheus protocol parser.
    let filters = parse_metric_selector(metric_with_labels)
        .map_err(|err| format!("cannot parse metric selector {:?}: {}", metric_with_labels, err)).unwrap();
    let mut x: Vec<Label> = vec![];
    for tag in filters.iter() {
        x.push(Label{
            name: tag.label.clone(),
            value: tag.value.clone(),
        });
    }
    return x
}