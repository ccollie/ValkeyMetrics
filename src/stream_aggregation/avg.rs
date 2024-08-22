use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use bytes::Bytes;
use crate::stream_aggregation::PushSample;
use crate::stream_aggregation::stream_aggr::FlushCtx;

pub struct AvgAggrState {
    m: DashMap<Bytes, Arc<Mutex<AvgStateValue>>>,
}

pub struct AvgState {
    sum: f64,
    count: f64,
}

pub struct AvgStateValue {
    state: [AvgState; aggr_state_size],
    deleted: bool,
    delete_deadline: i64,
}

const aggr_state_size: usize = 10; // Example size, replace with actual

impl AvgAggrState {
    fn new() -> Self {
        Self { m: DashMap::new() }
    }

    fn push_samples(&self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(s.key);

            loop {
                let v = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(AvgStateValue {
                        state: [AvgState { sum: 0.0, count: 0.0 }; aggr_state_size],
                        deleted: false,
                        delete_deadline: 0,
                    }))
                });

                let mut sv = v.lock().unwrap();
                if !sv.deleted {
                    sv.state[idx].sum += s.value;
                    sv.state[idx].count += 1.0;
                    sv.delete_deadline = delete_deadline;
                    break;
                } else {
                    // The entry has been deleted by the concurrent call to flush_state
                    // Try obtaining and updating the entry again.
                    self.m.remove(&output_key);
                }
            }
        }
    }

    fn flush_state(&self, ctx: &FlushCtx) {
        for entry in self.m.iter() {
            let (k, v) = entry.pair();
            let mut sv = v.lock().unwrap();

            // Check for stale entries
            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                // Mark the current entry as deleted
                sv.deleted = deleted;
                drop(sv);
                self.m.remove(k);
                continue;
            }

            let state = &sv.state[ctx.idx];
            sv.state[ctx.idx] = AvgState { sum: 0.0, count: 0.0 };
            drop(sv);

            if state.count > 0.0 {
                let key = k.clone();
                let avg = state.sum / state.count;
                ctx.append_series(key, "avg", avg);
            }
        }
    }
}


fn get_output_key(key: Bytes) -> Bytes {
    // Implement the logic to get the output key
    key
}