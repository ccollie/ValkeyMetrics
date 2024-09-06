use crate::stream_aggregation::stream_aggr::{get_output_key, AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use ahash::RandomState;
use papaya::HashMap;
use std::sync::{Arc, Mutex};

pub struct SumSamplesAggrState {
    m: HashMap<OutputKey, Arc<Mutex<SumSamplesStateValue>>, RandomState>,
}

struct SumSamplesStateValue {
    state: [SumState; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

struct SumState {
    sum: f64,
    exists: bool,
}

impl SumSamplesAggrState {
    pub fn new() -> Self {
        Self { m: HashMap::with_hasher(RandomState::new()) }
    }
    fn update_state(sv: &mut SumSamplesStateValue, sample: &PushSample, delete_deadline: i64, idx: usize) {
        let state = &mut sv.state[idx];
        state.sum += sample.value;
        state.exists = true;
        sv.delete_deadline = delete_deadline;
    }
}

impl AggrState for SumSamplesAggrState {
    fn push_samples(&mut self, samples: &Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples.iter() {
            let output_key = get_output_key(&s.key);

            let guard = self.m.guard();
            let mut entry = self.m.get(output_key, &guard);
            if let Some(entry) = entry {
                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }
                Self::update_state(&mut sv, &s, delete_deadline, idx);
            } else {
                let mut state = SumSamplesStateValue {
                    state: [SumState { sum: 0.0, exists: false }; AGGR_STATE_SIZE],
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
        for entry in map.iter() {
            let mut sv = entry.value().lock().unwrap();

            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                sv.deleted = true;
                map.remove(&entry.key());
                continue;
            }

            let state = &sv.state[ctx.idx];
            sv.state[ctx.idx] = SumState { sum: 0.0, exists: false };

            if state.exists {
                ctx.append_series(&entry.key(), "sum_samples", state.sum);
            }
        }
    }
}