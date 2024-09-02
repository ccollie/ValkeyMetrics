use crate::stream_aggregation::stream_aggr::{get_output_key, AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use papaya::HashMap;
use ahash::RandomState;
use std::sync::{Arc, Mutex};

pub struct LastAggrState {
    m: HashMap<OutputKey, Arc<Mutex<LastStateValue>>, RandomState>,
}

struct LastStateValue {
    state: [LastState; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

struct LastState {
    last: f64,
    timestamp: i64,
}

impl LastAggrState {
    pub fn new() -> Self {
        Self { m: HashMap::with_hasher(RandomState::new()) }
    }

    fn update_state(sv: &mut LastStateValue, sample: &PushSample, delete_deadline: i64, idx: usize) {
        let state = &mut sv.state[idx];
        if sample.timestamp >= state.timestamp {
            state.last = sample.value;
            state.timestamp = sample.timestamp;
        }
        sv.delete_deadline = delete_deadline;
    }
}

impl AggrState for LastAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(&s.key);

            let map = self.m.pin();
            let mut entry = map.get(&output_key);
            if let Some(entry) = entry {
                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }
                Self::update_state(&mut sv, &s, delete_deadline, idx);
            } else {
                let mut state = LastStateValue {
                    state: [LastState { last: 0.0, timestamp: 0 }; AGGR_STATE_SIZE],
                    deleted: false,
                    delete_deadline,
                };
                Self::update_state(&mut state, &s, delete_deadline, idx);
                let sv = Arc::new(Mutex::new(state));
                map.insert(output_key.to_string(), sv);
            }
        }
    }

    fn flush_state(&mut self, ctx: &mut FlushCtx) {
        let map = self.m.pin();
        for (key, value) in map.iter() {
            let mut sv = value.lock().unwrap();

            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                sv.deleted = true;
                map.remove(key);
                continue;
            }

            let state = &sv.state[ctx.idx];
            sv.state[ctx.idx] = LastState { last: 0.0, timestamp: 0 };

            if state.timestamp > 0 {
                ctx.append_series(key, "last", state.last);
            }
        }
    }
}