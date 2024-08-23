use std::mem::size_of;
use crate::stream_aggregation::{PushSample, AGGR_STATE_SIZE};
use dashmap::DashMap;
use std::sync::{atomic::{AtomicI32, AtomicU64, Ordering}, Arc, Mutex};
use xxhash_rust::xxh3::xxh3_64;

const DEDUP_AGGR_SHARDS_COUNT: usize = 128;

pub struct DedupAggr {
    shards: Vec<DedupAggrShard>,
    current_idx: AtomicI32,
}

struct DedupAggrShard {
    state: [DedupAggrState; AGGR_STATE_SIZE],
}

pub struct DedupAggrState {
    m: DashMap<String, DedupAggrSample>,
    samples_buf: Vec<DedupAggrSample>,
    size_bytes: AtomicU64,
    items_count: AtomicU64,
}

struct DedupAggrSample {
    value: f64,
    timestamp: i64,
}

impl DedupAggr {
    fn new() -> Self {
        let shards = vec![DedupAggrShard {
            state: [None; AGGR_STATE_SIZE],
        }; DEDUP_AGGR_SHARDS_COUNT];
        Self {
            shards,
            current_idx: AtomicI32::new(0),
        }
    }

    fn size_bytes(&self) -> u64 {
        let mut n = size_of::<DedupAggr>() as u64;
        let current_idx = self.current_idx.load(Ordering::Relaxed);
        for shard in &self.shards {
            if let Some(state) = &shard.state[current_idx as usize] {
                n += state.lock().unwrap().size_bytes.load(Ordering::Relaxed);
            }
        }
        n
    }

    fn items_count(&self) -> u64 {
        let mut n = 0;
        let current_idx = self.current_idx.load(Ordering::Relaxed);
        for shard in &self.shards {
            if let Some(state) = &shard.state[current_idx as usize] {
                n += state.lock().unwrap().items_count.load(Ordering::Relaxed);
            }
        }
        n
    }

    fn push_samples(&self, samples: Vec<PushSample>, _delete_deadline: i64, dedup_idx: usize) {
        let pss = get_per_shard_samples();
        let shards = &mut pss.shards;
        for sample in samples {
            let h = xxh3_64(sample.key.as_bytes());
            let idx = h % shards.len() as u64;
            shards[idx as usize].push(sample);
        }
        for (i, shard_samples) in shards.iter().enumerate() {
            if !shard_samples.is_empty() {
                self.shards[i].push_samples(shard_samples.clone(), dedup_idx);
            }
        }
        put_per_shard_samples(pss);
    }

    fn flush<F>(&self, f: F, delete_deadline: i64, dedup_idx: usize, flush_idx: usize)
    where
        F: Fn(Vec<PushSample>, i64, usize) + Send + Sync + 'static,
    {
        let mut wg = tokio::sync::WaitGroup::new();
        for shard in &self.shards {
            flush_concurrency_ch.lock().unwrap().push(());
            wg.add(1);
            let shard = shard.clone();
            let f = Arc::new(f);
            tokio::spawn(async move {
                let ctx = get_dedup_flush_ctx();
                shard.flush(ctx, f, delete_deadline, dedup_idx, flush_idx);
                put_dedup_flush_ctx(ctx);
                flush_concurrency_ch.lock().unwrap().pop();
                wg.done();
            });
        }
        wg.wait().await;
        let current_idx = self.current_idx.load(Ordering::Relaxed);
        self.current_idx.store((current_idx + 1) % AGGR_STATE_SIZE as i32, Ordering::Relaxed);
    }
}

struct DedupFlushCtx {
    samples: Vec<PushSample>,
}

impl DedupFlushCtx {
    fn reset(&mut self) {
        self.samples.clear();
    }
}

static DEDUP_FLUSH_CTX_POOL: Mutex<Vec<DedupFlushCtx>> = Mutex::new(Vec::new());
static FLUSH_CONCURRENCY_CH: Mutex<Vec<()>> = Mutex::new(Vec::new());

fn get_dedup_flush_ctx() -> DedupFlushCtx {
    DEDUP_FLUSH_CTX_POOL.lock().unwrap().pop().unwrap_or_else(|| DedupFlushCtx {
        samples: Vec::new(),
    })
}

fn put_dedup_flush_ctx(mut ctx: DedupFlushCtx) {
    ctx.reset();
    DEDUP_FLUSH_CTX_POOL.lock().unwrap().push(ctx);
}

struct PerShardSamples {
    shards: Vec<Vec<PushSample>>,
}

impl PerShardSamples {
    fn reset(&mut self) {
        for shard_samples in &mut self.shards {
            shard_samples.clear();
        }
    }
}

static PER_SHARD_SAMPLES_POOL: Mutex<Vec<PerShardSamples>> = Mutex::new(Vec::new());

fn get_per_shard_samples() -> PerShardSamples {
    PER_SHARD_SAMPLES_POOL.lock().unwrap().pop().unwrap_or_else(|| PerShardSamples {
        shards: vec![Vec::new(); DEDUP_AGGR_SHARDS_COUNT],
    })
}

fn put_per_shard_samples(pss: PerShardSamples) {
    pss.reset();
    PER_SHARD_SAMPLES_POOL.lock().unwrap().push(pss);
}

impl DedupAggrShard {
    fn push_samples(&mut self, samples: Vec<PushSample>, dedup_idx: usize) {
        let mut state = self.state[dedup_idx].get_or_insert_with(|| {
            Arc::new(Mutex::new(DedupAggrState {
                m: DashMap::new(),
                samples_buf: Vec::new(),
                size_bytes: AtomicU64::new(0),
                items_count: AtomicU64::new(0),
            }))
        });

        let mut state = state.lock().unwrap();
        let mut samples_buf = state.samples_buf.clone();
        for sample in samples {
            let key = sample.key.clone();
            if let Some(s) = state.m.get(&key) {
                let mut s = s.value().clone();
                if sample.timestamp > s.timestamp || (sample.timestamp == s.timestamp && sample.value > s.value) {
                    s.value = sample.value;
                    s.timestamp = sample.timestamp;
                    state.m.insert(key, s);
                }
            } else {
                samples_buf.push(DedupAggrSample {
                    value: sample.value,
                    timestamp: sample.timestamp,
                });
                state.m.insert(key.clone(), samples_buf.last().unwrap().clone());
                state.items_count.fetch_add(1, Ordering::Relaxed);
                state.size_bytes.fetch_add(key.len() as u64 + std::mem::size_of::<String>() as u64 + std::mem::size_of::<DedupAggrSample>() as u64, Ordering::Relaxed);
            }
        }
        state.samples_buf = samples_buf;
    }

    fn flush<F>(&self, ctx: &mut DedupFlushCtx, f: Arc<F>, delete_deadline: i64, dedup_idx: usize, flush_idx: usize)
    where
        F: Fn(Vec<PushSample>, i64, usize) + Send + Sync + 'static,
    {
        let state = self.state[dedup_idx].as_ref().and_then(|s| s.lock().ok().map(|s| s.clone()));
        if let Some(state) = state {
            let mut m = state.m.clone();
            state.m.clear();
            state.samples_buf.clear();
            state.size_bytes.store(0, Ordering::Relaxed);
            state.items_count.store(0, Ordering::Relaxed);

            let mut dst_samples = ctx.samples.clone();
            for entry in m.iter() {
                let key = entry.key().clone();
                let s = entry.value().clone();
                dst_samples.push(PushSample {
                    key,
                    value: s.value,
                    timestamp: s.timestamp,
                });

                if dst_samples.len() >= 10_000 {
                    f(dst_samples.clone(), delete_deadline, flush_idx);
                    dst_samples.clear();
                }
            }
            f(dst_samples.clone(), delete_deadline, flush_idx);
            ctx.samples = dst_samples;
        }
    }
}
