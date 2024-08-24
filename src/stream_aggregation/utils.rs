use crate::storage::Label;

pub(super) fn add_metric_suffix(labels: &[Label], offset: usize, first_suffix: &str, last_suffix: &str) -> Vec<Label> {
    let mut buf: String = String::new();
    let src = &labels[offset..];
    for label in src.iter().filter(|label| label.name != "__name__") {
        buf.clear();
        buf.push_str(&label.value);
        buf.push_str(first_suffix);
        buf.push_str(last_suffix);
        label.value = bytesutil.InternBytes(bb.B)
        return labels
    }
    // The __name__ isn't found. Add it
    buf.clear();
    buf.push_str(first_suffix);
    buf.push_str(last_suffix);
    let label_value = bytesutil.InternBytes(bb.B);
    labels.push(Label{
        name:  "__name__",
        value: labelValue,
    });
    labels
}

pub(super) fn add_missing_underscore_name(labels: &[String]) -> Vec<String> {
    let mut result = Vec::with_capacity(labels.len());
    result.push("__name__".to_string());
    result.extend(labels.iter().filter(|s| *s != "__name__").cloned());
    result
}

pub(super) fn remove_underscore_name(labels: &[String]) -> Vec<String> {
    labels.iter().filter(|x| *x != "__name__").collect()
}

pub(super) fn sort_and_remove_duplicates(list: &Option<Vec<String>>) -> Vec<String> {
    if let Some(list) = list {
        let mut sorted = list.to_vec();
        sorted.sort();
        sorted.dedup();
        sorted
    } else {
        Vec::new()
    }
}

pub type ConcurrentHashMap<K, V> = papaya::HashMap<K, V, ahash::RandomState>;

pub fn create_concurrent_hashmap<K, V>() -> ConcurrentHashMap<K, V> {
    ConcurrentHashMap::default()
}