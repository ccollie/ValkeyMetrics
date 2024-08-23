use lru_time_cache::LruCache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use metricsql_common::prelude::match_handlers::StringMatchHandler;

const DEFAULT_MAX_SIZE_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_CACHE_SIZE: usize = 100;

#[derive(Clone, Debug)]
pub(crate) struct RegexpCacheValue {
    pub or_values: Vec<String>,
    pub re_match: StringMatchHandler,
    pub re_cost: usize,
    pub literal_suffix: Option<String>,
    pub size_bytes: usize,
}

pub struct RegexpCache {
    requests: AtomicU64,
    misses: AtomicU64,
    inner: Mutex<LruCache<String, Arc<RegexpCacheValue>>>,
    max_size_bytes: usize,
}

impl RegexpCache {
    pub fn new(max_size_bytes: usize) -> Self {
        Self {
            requests: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inner: Mutex::new(LruCache::with_capacity(DEFAULT_CACHE_SIZE)),
            max_size_bytes
        }
    }

    pub fn get(&self, key: &str) -> Option<Arc<RegexpCacheValue>> {
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

    pub fn put(&self, key: &str, value: Arc<RegexpCacheValue>) {
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

    pub fn remove(&self, key: &str) -> Option<Arc<RegexpCacheValue>> {
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