use crate::stream_aggregation::stream_aggr::{get_output_key, AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use ahash::RandomState;
use papaya::HashMap;
use std::sync::{Arc, Mutex};

pub struct StddevAggrState {
    m: HashMap<OutputKey, Arc<Mutex<StddevStateValue>>, RandomState>,
}

struct StddevStateValue {
    state: [StddevState; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

struct StddevState {
    count: f64,
    avg: f64,
    q: f64,
}

impl StddevAggrState {
    pub fn new() -> Self {
        Self { m: HashMap::with_hasher(RandomState::new()) }
    }

    fn update_state(sv: &mut StddevStateValue, sample: &PushSample, delete_deadline: i64, idx: usize) {
        let state = &mut sv.state[idx];
        state.count += 1.0;
        let avg = state.avg + (sample.value - state.avg) / state.count;
        state.q += (sample.value - state.avg) * (sample.value - avg);
        state.avg = avg;
        sv.delete_deadline = delete_deadline;
    }
}

impl AggrState for StddevAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples.iter() {
            let output_key = get_output_key(&s.key);

            let mut entry = self.m.pin().get(&output_key);
            if let Some(entry) = entry {
                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }
                Self::update_state(&mut sv, &s, delete_deadline, idx);
            } else {
                let mut state = StddevStateValue {
                    state: [StddevState { count: 0.0, avg: 0.0, q: 0.0 }; AGGR_STATE_SIZE],
                    deleted: false,
                    delete_deadline,
                };
                Self::update_state(&mut state, &s, delete_deadline, idx);
                let sv = Arc::new(Mutex::new(state));
                let map = self.m.pin();
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
            sv.state[ctx.idx] = StddevState { count: 0.0, avg: 0.0, q: 0.0 };

            if state.count > 0.0 {
                ctx.append_series(key, "stddev", (state.q / state.count).sqrt());
            }
        }
    }
}