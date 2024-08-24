use dashmap::DashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::time::interval;
use prometheus::{Histogram, Counter, register_histogram, register_counter};
use log::warn;
use metricsql_parser::label::Labels;
use metricsql_runtime::Label;
use xxhash_rust::xxh3::xxh3_64;
use crate::stream_aggregation::{PushSample, AGGR_STATE_SIZE};

pub struct Deduplicator {
    da: DedupAggr,
    drop_labels: Vec<String>,
    dedup_interval: i64,
    stop_tx: Option<oneshot::Sender<()>>,
    dedup_flush_duration: Histogram,
    dedup_flush_timeouts: AtomicU64,
}

impl Deduplicator {
    fn new(push_func: PushFunc, dedup_interval: Duration, drop_labels: Vec<String>, alias: String) -> Self {
        let da = DedupAggr::new();
        let dedup_interval_ms = dedup_interval.as_millis() as i64;
        let (stop_tx, stop_rx) = oneshot::channel();

        tokio::spawn(async move {
            let mut interval = interval(dedup_interval);
            loop {
                tokio::select! {
                    _ = stop_rx => {
                        break;
                    }
                    _ = interval.tick() => {
                        let flush_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
                        let flush_intervals = flush_time / dedup_interval_ms;
                        let flush_idx = (flush_intervals % AGGR_STATE_SIZE as i64) as usize;
                        da.flush(push_func.clone(), dedup_interval_ms, flush_time, flush_idx);
                    }
                }
            }
        });

        Self {
            da,
            drop_labels,
            dedup_interval: dedup_interval_ms,
            stop_tx: Some(stop_tx),
            dedup_flush_duration,
            dedup_flush_timeouts,
        }
    }

    fn must_stop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
    }

    fn push(&self, tss: Vec<TimeSeries>) {
        let mut ctx = get_deduplicator_push_ctx();
        let pss = &mut ctx.pss;
        let labels = &mut ctx.labels;
        let buf = &mut ctx.buf;

        let drop_labels = &self.drop_labels;
        let aggr_intervals = AGGR_STATE_SIZE as i64;
        for ts in tss {
            if !drop_labels.is_empty() {
                labels.labels = drop_series_labels(labels.labels.drain(..).collect(), ts.labels, drop_labels);
            } else {
                labels.labels = ts.labels;
            }
            if labels.labels.is_empty() {
                continue;
            }
            labels.sort();

            let buf_len = buf.len();
            buf.extend_from_slice(labels.compress().as_bytes());
            let key = String::from_utf8_lossy(&buf[buf_len..]).to_string();
            for s in ts.samples {
                let flush_intervals = s.timestamp / self.dedup_interval + 1;
                let idx = (flush_intervals % aggr_intervals) as usize;
                pss[idx].push(PushSample {
                    key: key.clone(),
                    value: s.value,
                    timestamp: s.timestamp,
                });
            }
        }

        for (idx, ps) in pss.iter().enumerate() {
            self.da.push_samples(ps.clone(), 0, idx);
        }

        put_deduplicator_push_ctx(ctx);
    }
}

pub fn drop_series_labels(dst: &mut Vec<Label>, src: &[Label], label_names: &[String]) {
    for label in src.iter() {
        if !label_names.contains(&label.name) {
            dst.push(label.clone());
        }
    }
}

struct DeduplicatorPushCtx {
    pss: [Vec<PushSample>; AGGR_STATE_SIZE],
    labels: Labels,
    buf: Vec<u8>,
}

impl DeduplicatorPushCtx {
    fn reset(&mut self) {
        for sc in &mut self.pss {
            sc.clear();
        }
        self.labels.reset();
        self.buf.clear();
    }
}

static DEDUPLICATOR_PUSH_CTX_POOL: LazyLock<Mutex<Vec<DeduplicatorPushCtx>>> = LazyLock::new(|| Mutex::new(Vec::new()));

fn get_deduplicator_push_ctx() -> DeduplicatorPushCtx {
    DEDUPLICATOR_PUSH_CTX_POOL.lock().unwrap().pop().unwrap_or_else(|| DeduplicatorPushCtx {
        pss: [Vec::new(); AGGR_STATE_SIZE],
        labels: Labels::new(),
        buf: Vec::new(),
    })
}

fn put_deduplicator_push_ctx(mut ctx: DeduplicatorPushCtx) {
    ctx.reset();
    DEDUPLICATOR_PUSH_CTX_POOL.lock().unwrap().push(ctx);
}

struct DeduplicatorFlushCtx {
    tss: Vec<TimeSeries>,
    labels: Vec<Label>,
    samples: Vec<Sample>,
}

impl DeduplicatorFlushCtx {
    fn reset(&mut self) {
        self.tss.clear();
        self.labels.clear();
        self.samples.clear();
    }
}

static DEDUPLICATOR_FLUSH_CTX_POOL: LazyLock<Mutex<Vec<DeduplicatorFlushCtx>>> = LazyLock::new(|| Mutex::new(Vec::new()));

fn get_deduplicator_flush_ctx() -> DeduplicatorFlushCtx {
    DEDUPLICATOR_FLUSH_CTX_POOL.lock().unwrap().pop().unwrap_or_else(|| DeduplicatorFlushCtx {
        tss: Vec::new(),
        labels: Vec::new(),
        samples: Vec::new(),
    })
}

fn put_deduplicator_flush_ctx(mut ctx: DeduplicatorFlushCtx) {
    ctx.reset();
    DEDUPLICATOR_FLUSH_CTX_POOL.lock().unwrap().push(ctx);
}

struct TimeSeries {
    labels: Vec<Label>,
    samples: Vec<Sample>,
}

type PushFunc = Arc<dyn Fn(Vec<TimeSeries>) + Send + Sync>;

struct DedupAggr {
    shards: Vec<DedupAggrShard>,
    current_idx: AtomicI32,
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
        self.current_idx.store((self.current_idx.load(Ordering::Relaxed) + 1) % AGGR_STATE_SIZE as i32, Ordering::Relaxed);
    }
}

struct DedupAggrShard {
    state: [Option<Arc<Mutex<DedupAggrState>>>; AGGR_STATE_SIZE],
}

struct DedupAggrState {
    m: DashMap<String, DedupAggrSample>,
    samples_buf: Vec<DedupAggrSample>,
    size_bytes: AtomicU64,
    items_count: AtomicU64,
}

struct DedupAggrSample {
    value: f64,
    timestamp: i64,
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
            if let Some(s) = state.m.get_mut(&key) {
                let mut s = s.clone();
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

fn put_per_shard_samples(mut pss: PerShardSamples) {
    pss.reset();
    PER_SHARD_SAMPLES_POOL.lock().unwrap().push(pss);
}