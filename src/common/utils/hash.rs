use std::hash::Hasher;
use ahash::AHashMap;
use metricsql_engine::METRIC_NAME_LABEL;
use xxhash_rust::xxh3::Xxh3;

pub fn hash_labels_without_metric_name(labels: &AHashMap<String, String>) -> u64 {
    let mut hasher: Xxh3 = Xxh3::with_seed(0);
    let keys: Vec<&String> = labels
        .keys()
        // drop __name__ to be consistent with Prometheus alerting
        .filter(|k| k.as_str() != METRIC_NAME_LABEL)
        .sorted()
        .collect();

    for key in keys {
        hasher.write(key.as_bytes());
        if let Some(value) = labels.get(key) {
            hasher.write(value.as_bytes());
        }
        hasher.write("\0xff".as_bytes());
    }
    hasher.digest()
}

pub fn hash_labels(labels: &AHashMap<String, String>) -> u64 {
    let mut hasher: Xxh3 = Xxh3::with_seed(0);
    let keys: Vec<&String> = labels
        .keys()
        .sorted()
        .collect();

    for key in keys {
        hasher.write(key.as_bytes());
        if let Some(value) = labels.get(key) {
            hasher.write(value.as_bytes());
        }
        hasher.write("\0xff".as_bytes());
    }
    hasher.digest()
}