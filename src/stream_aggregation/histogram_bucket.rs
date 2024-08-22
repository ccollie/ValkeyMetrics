use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use crate::stream_aggregation::stream_aggr::FlushCtx;

struct HistogramBucketAggrState {
    m: DashMap<OutputKey, Arc<Mutex<HistogramBucketStateValue>>>,
}

struct HistogramBucketStateValue {
    state: [Histogram; AGGR_STATE_SIZE],
    total: Histogram,
    deleted: bool,
    delete_deadline: i64,
}

impl HistogramBucketAggrState {
    fn new() -> Self {
        Self { m: DashMap::new() }
    }

    fn push_samples(&self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(&s.key);

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(HistogramBucketStateValue {
                        state: [Histogram::new(); AGGR_STATE_SIZE],
                        total: Histogram::new(),
                        deleted: false,
                        delete_deadline,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                sv.state[idx].increment(s.value as u64).unwrap();
                sv.delete_deadline = delete_deadline;
                break;
            }
        }
    }

    fn flush_state(&self, ctx: &FlushCtx) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64;

        for entry in self.m.iter() {
            let mut sv = entry.value().lock().unwrap();

            if now > sv.delete_deadline {
                sv.deleted = true;
                self.m.remove(&entry.key());
                continue;
            }

            sv.total.merge(&sv.state[ctx.idx]);
            let total = &sv.total;
            sv.state[ctx.idx].reset();

            let key = entry.key();
            for bucket in total.buckets() {
                if bucket.count > 0 {
                    ctx.append_series_with_extra_label(key, "histogram_bucket", bucket.count as f64, "vmrange", &bucket.range);
                }
            }
        }
    }
}
