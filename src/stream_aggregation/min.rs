use crate::stream_aggregation::stream_aggr::{get_output_key, AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use papaya::HashMap;
use std::sync::{Arc, Mutex};


pub struct MinAggrState {
    m: HashMap<OutputKey, Arc<Mutex<MinStateValue>>, ahash::RandomState>,
}

struct MinStateValue {
    state: [MinState; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

struct MinState {
    min: f64,
    exists: bool,
}

impl MinAggrState {
    pub fn new() -> Self {
        Self {
            m: HashMap::with_hasher(ahash::RandomState::new())
        }
    }

    fn update_state(sv: &mut MinStateValue, sample: &PushSample, delete_deadline: i64, idx: usize) {
        let state = &mut sv.state[idx];
        if !state.exists {
            state.min = sample.value;
            state.exists = true;
        } else if sample.value < state.min {
            state.min = sample.value;
        }
        sv.delete_deadline = delete_deadline;
    }
}

impl AggrState for MinAggrState {
    fn push_samples(&mut self, samples: &Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(&s.key);

            let guard = self.m.guard();
            let mut entry = self.m.get(&output_key, &guard);
            if let Some(entry) = entry {
                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }
                Self::update_state(&mut sv, &s, delete_deadline, idx);
            } else {
                let mut state = MinStateValue {
                    state: [MinState { min: 0.0, exists: false }; AGGR_STATE_SIZE],
                    deleted: false,
                    delete_deadline,
                };
                Self::update_state(&mut state, &s, delete_deadline, idx);
                let sv = Arc::new(Mutex::new(state));
                self.m.insert(output_key.to_string(), sv, &guard);
            }
        }
    }

    fn flush_state(&mut self, ctx: &mut FlushCtx) {
        let mut map = self.m.pin();
        for (key, value) in map.iter() {
            let mut sv = value.lock().unwrap();

            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                sv.deleted = true;
                map.remove(&key);
                continue;
            }

            let state = &sv.state[ctx.idx];
            sv.state[ctx.idx] = MinState { min: 0.0, exists: false };

            if state.exists {
                ctx.append_series(&key, "min", state.min);
            }
        }
    }
}