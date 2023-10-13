use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};
use crate::common::types::{Label, Sample};


#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawTimeSeries {
    pub key: String,
    pub samples: Vec<Sample>,
    pub labels: Vec<Label>,
}

pub fn new_time_series(key: String, values: &[f64], timestamps: &[i64], labels: AHashMap<String, String>) -> RawTimeSeries {
    let samples = values
        .iter()
        .zip(timestamps.iter())
        .map(|(value, timestamp)| Sample {
            value: *value,
            timestamp: *timestamp,
        })
        .collect();

    let tags = labels
        .into_iter()
        .map(|(k, v)| Label { name: k, value: v })
        .collect();

    RawTimeSeries {
        key,
        samples,
        labels: tags,
    }
}

pub fn get_changed_label_names(prev: &[Label], current: &[Label]) -> AHashSet<String> {
    let mut in_map = get_label_map(prev);
    let mut out_map = get_label_map(current);
    let mut changed = AHashSet::with_capacity(prev.len());
    for (k, v) in out_map.iter() {
        if let Some(inV) = in_map.get(k) {
            if inV != v {
                changed.insert(k.clone());
            }
        } else {
            changed.insert(k.clone());
        }
    }

    for (k, v) in in_map.iter() {
        if let Some(outV) = out_map.get(k) {
            if outV != v {
                changed.insert(k.clone());
            }
        } else {
            changed.insert(k.clone());
        }
    }

    changed
}

fn get_label_map(labels: &[Label]) -> AHashMap<String, String> {
    let mut map = AHashMap::with_capacity(labels.len());
    for label in labels {
        map.insert(label.name.clone(), label.value.clone());
    }
    map
}