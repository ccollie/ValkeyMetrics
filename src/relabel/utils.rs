use metricsql_engine::parse_metric_selector;
use crate::storage::Label;

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

pub fn concat_label_values(labels: &[Label], label_names: &[String], separator: &str) -> String {
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

pub fn set_label_value(labels: &mut Vec<Label>, labels_offset: usize, name: &str, value: String) {
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

pub fn get_label_value<'a>(labels: &'a [Label], name: &str) -> &'a str {
    for label in labels.iter() {
        if label.name == name {
            return &label.value;
        }
    }
    return &EMPTY_STRING;
}

/// returns label with the given name from labels.
pub fn get_label_by_name<'a>(labels: &'a mut [Label], name: &str) -> Option<&'a mut Label> {
    for label in labels.iter_mut() {
        if &label.name == name {
            return Some(label);
        }
    }
    return None;
}

pub fn are_equal_label_values(labels: &[Label], label_names: &[String]) -> bool {
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

pub fn contains_all_label_values(labels: &[Label], target_label: &str, source_labels: &[String]) -> bool {
    let target_label_value = get_label_value(labels, target_label);
    for sourceLabel in source_labels.iter() {
        let v = get_label_value(labels, sourceLabel);
        if !target_label_value.contains(v) {
            return false
        }
    }
    true
}