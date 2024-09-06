use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use lazy_static::lazy::Lazy;

const INTERN_STRING_MAX_LEN: usize = 500;
static DISABLE_CACHE: Lazy<bool> = Lazy::new(|| false);
static CACHE_EXPIRE_DURATION: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(6 * 60));

struct InternStringMap {
    mutable: Mutex<HashMap<String, String>>,
    mutable_reads: AtomicU64,
    readonly: AtomicU64,
}

struct InternStringMapEntry {
    deadline: Instant,
    s: String,
}

impl InternStringMap {
    fn new() -> Self {
        let m = InternStringMap {
            mutable: Mutex::new(HashMap::new()),
            mutable_reads: AtomicU64::new(0),
            readonly: AtomicU64::new(0),
        };

        thread::spawn(move || {
            let cleanup_interval = CACHE_EXPIRE_DURATION.div_f64(2.0);
            loop {
                thread::sleep(cleanup_interval);
                m.cleanup();
            }
        });

        m
    }

    fn intern(&self, s: String) -> String {
        if is_skip_cache(&s) {
            return s.clone();
        }

        let readonly = self.get_readonly();
        if let Some(e) = readonly.get(&s) {
            return e.s.clone();
        }

        let mut mutable = self.mutable.lock().unwrap();
        if let Some(s_interned) = mutable.get(&s) {
            self.mutable_reads.fetch_add(1, Ordering::SeqCst);
            if self.mutable_reads.load(Ordering::SeqCst) > readonly.len() as u64 {
                self.migrate_mutable_to_readonly_locked();
                self.mutable_reads.store(0, Ordering::SeqCst);
            }
            return s_interned.clone();
        }

        let s_interned = s.clone();
        mutable.insert(s_interned.clone(), s_interned.clone());
        self.mutable_reads.fetch_add(1, Ordering::SeqCst);
        self.mutable_reads.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
            if x + 1 > readonly.len() as u64 {
                self.migrate_mutable_to_readonly_locked();
                Some(0)
            } else {
                Some(x + 1)
            }
        }).unwrap();
        s_interned
    }

    fn migrate_mutable_to_readonly_locked(&self) {
        let mut mutable = self.mutable.lock().unwrap();
        let readonly = self.get_readonly();
        let mut readonly_copy = HashMap::with_capacity(readonly.len() + mutable.len());
        for (k, e) in readonly {
            readonly_copy.insert(k.clone(), e.clone());
        }
        let deadline = Instant::now() + *CACHE_EXPIRE_DURATION;
        for (k, s) in mutable.drain() {
            readonly_copy.insert(k.clone(), InternStringMapEntry {
                s: s.clone(),
                deadline,
            });
        }
        self.readonly.store(Arc::new(readonly_copy).as_ptr() as u64, Ordering::SeqCst);
    }

    fn cleanup(&self) {
        let readonly = self.get_readonly();
        let current_time = Instant::now();
        let need_cleanup = readonly.values().any(|e| e.deadline <= current_time);
        if !need_cleanup {
            return;
        }

        let mut readonly_copy = HashMap::with_capacity(readonly.len());
        for (k, e) in readonly {
            if e.deadline > current_time {
                readonly_copy.insert(k.clone(), e.clone());
            }
        }
        self.readonly.store(Arc::new(readonly_copy).as_ptr() as u64, Ordering::SeqCst);
    }

    fn get_readonly(&self) -> HashMap<String, InternStringMapEntry> {
        let ptr = self.readonly.load(Ordering::SeqCst);
        unsafe { Arc::from_raw(ptr as *const HashMap<String, InternStringMapEntry>).clone() }
    }
}

fn is_skip_cache(s: &str) -> bool {
    *DISABLE_CACHE || s.len() > *INTERN_STRING_MAX_LEN
}

static ISM: LazyLock<InternStringMap> = LazyLock::new(|| InternStringMap::new());

pub fn intern_bytes(b: &[u8]) -> String {
    let s = unsafe { String::from_utf8_unchecked(b.to_vec()) };
    intern_string(s)
}

fn intern_string(s: String) -> String {
    ISM.intern(s)
}