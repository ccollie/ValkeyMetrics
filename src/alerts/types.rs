use crate::common::types::{Label, Sample};
use ahash::{AHashMap, AHashSet};

#[derive(Debug, Default, Clone, PartialEq)]
pub struct RawTimeSeries {
    pub key: String,
    pub samples: Vec<Sample>,
    pub labels: Vec<Label>,
}

pub fn get_changed_label_names(prev: &[Label], current: &[Label]) -> AHashSet<String> {
    let in_map = get_label_map(prev);
    let out_map = get_label_map(current);
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

pub fn hashmap_to_labels<T: Into<String>>(hash: impl Iterator<Item=(T, T)>) -> Vec<Label> {
    hash.map(|(k, v)| Label { name: k.into(), value: v.into() }).collect()
}