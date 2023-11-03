use lru_time_cache::LruCache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const DEFAULT_MAX_SIZE_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_CACHE_SIZE: usize = 100;

#[derive(Clone, Debug)]
pub struct PrefixSuffix {
    pub prefix: String,
    pub suffix: String,
}

pub struct PrefixCache {
    requests: AtomicU64,
    misses: AtomicU64,
    inner: Mutex<LruCache<String, Arc<PrefixSuffix>>>,
    max_size_bytes: usize,
}

impl PrefixCache {
    pub fn new(max_size_bytes: usize) -> Self {
        Self {
            requests: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inner: Mutex::new(LruCache::with_capacity(DEFAULT_CACHE_SIZE)),
            max_size_bytes
        }
    }

    pub fn get(&self, key: &str) -> Option<Arc<PrefixSuffix>> {
        let mut inner = self.inner.lock().unwrap();
        let item = inner.get(key);
        if item.is_some() {
            self.requests.fetch_add(1, Ordering::Relaxed);
            Some(item?.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn put(&self, key: &str, value: Arc<PrefixSuffix>) {
        self.inner.lock().unwrap().insert(key.to_string(), value);
    }

    /// returns the number of cached regexps for tag filters.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }

    pub fn remove(&self, key: &str) -> Option<Arc<PrefixSuffix>> {
        self.inner.lock().unwrap().remove(key)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    pub fn max_size_bytes(&self) -> usize {
        self.max_size_bytes
    }
}