use super::stream_aggr::{get_output_key, AggrState, FlushCtx};
use crate::stream_aggregation::{PushSample, AGGR_STATE_SIZE};
use ahash::RandomState;
use bytes::Bytes;
use papaya::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct AvgAggrState {
    m: HashMap<Bytes, Arc<Mutex<AvgStateValue>>, RandomState>,
}

#[derive(Debug, Clone, Copy)]
pub struct AvgState {
    sum: f64,
    count: usize,
}

#[derive(Clone, Copy)]
pub struct AvgStateValue {
    state: [AvgState; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

impl AvgAggrState {
    pub fn new() -> Self {
        Self { m: HashMap::with_hasher(RandomState::new()) }
    }

    fn update_state(sv: &mut AvgStateValue, sample: &PushSample, delete_deadline: i64, idx: usize) {
        let state = &mut sv.state[idx];
        state.sum += sample.value;
        state.count += 1;
        sv.delete_deadline = delete_deadline;
    }
}

impl AggrState for AvgAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(s.key);

            loop {
                let mut entry = self.m.pin().get(&output_key);
                if let Some(entry) = entry {
                    let mut sv = entry.lock().unwrap();
                    if sv.deleted {
                        // The entry has been deleted by the concurrent call to flush_state
                        // Try obtaining and updating the entry again.
                        self.m.pin().remove(&output_key);
                        continue;
                    }
                    Self::update_state(&mut sv, &s, delete_deadline, idx);
                } else {
                    let mut state = AvgStateValue {
                        state: [AvgState { sum: 0.0, count: 0 }; AGGR_STATE_SIZE],
                        deleted: false,
                        delete_deadline,
                    };
                    Self::update_state(&mut state, &s, delete_deadline, idx);
                    let sv = Arc::new(Mutex::new(state));
                    let map = self.m.pin();
                    map.insert(output_key.to_string(), sv);
                }
                break;
            }
        }
    }

    fn flush_state(&mut self, ctx: &mut FlushCtx) {
        let map = self.m.pin();
        for (k, v) in map.iter() {
            let mut sv = v.lock().unwrap();

            // Check for stale entries
            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                // Mark the current entry as deleted
                sv.deleted = deleted;
                drop(sv);
                map.remove(k);
                continue;
            }

            let state = &sv.state[ctx.idx];
            sv.state[ctx.idx] = AvgState { sum: 0.0, count: 0 };
            drop(sv);

            if state.count > 0 {
                let key = k.clone();
                let avg = state.sum / state.count as f64;
                ctx.append_series(k, "avg", avg);
            }
        }
    }
}