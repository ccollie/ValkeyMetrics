use crate::common::types::{Label, Sample};

#[derive(Debug, Default, Clone, PartialEq)]
pub struct RawTimeSeries {
    pub key: String,
    pub samples: Vec<Sample>,
    pub labels: Vec<Label>,
}

pub fn hashmap_to_labels<T: Into<String>>(hash: impl Iterator<Item=(T, T)>) -> Vec<Label> {
    hash.map(|(k, v)| Label { name: k.into(), value: v.into() }).collect()
}