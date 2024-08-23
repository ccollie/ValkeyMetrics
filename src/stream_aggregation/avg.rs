use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use bytes::Bytes;
use crate::stream_aggregation::{PushSample, AGGR_STATE_SIZE};
use crate::stream_aggregation::stream_aggr::{AggrState, FlushCtx};

#[derive(Debug, Clone)]
pub struct AvgAggrState {
    m: DashMap<Bytes, Arc<Mutex<AvgStateValue>>>,
}

#[derive(Debug, Clone, Copy)]
pub struct AvgState {
    sum: f64,
    count: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct AvgStateValue {
    state: [AvgState; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

impl AvgAggrState {
    pub fn new() -> Self {
        Self { m: DashMap::new() }
    }
}

impl AggrState for AvgAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(s.key);

            loop {
                let v = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(AvgStateValue {
                        state: [AvgState { sum: 0.0, count: 0.0 }; AGGR_STATE_SIZE],
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

    fn flush_state(&mut self, ctx: &mut FlushCtx) {
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
            sv.state[ctx.idx] = AvgState { sum: 0.0, count: 0 };
            drop(sv);

            if state.count > 0 {
                let key = k.clone();
                let avg = state.sum / state.count as f64;
                ctx.append_series(key, "avg", avg);
            }
        }
    }
}