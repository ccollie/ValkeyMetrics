use crate::stream_aggregation::stream_aggr::{get_output_key, AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use papaya::HashMap;
use ahash::RandomState;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub struct UniqueSamplesAggrState {
    m: HashMap<OutputKey, Arc<Mutex<UniqueSamplesStateValue>>, RandomState>,
}

struct UniqueSamplesStateValue {
    state: [HashSet<f64>; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

impl UniqueSamplesAggrState {
    pub fn new() -> Self {
        Self { m: HashMap::with_hasher(RandomState::new()) }
    }

    fn update_state(sv: &mut UniqueSamplesStateValue, sample: &PushSample, delete_deadline: i64, idx: usize) {
        let state = &mut sv.state[idx];
        state.insert(sample.value);
        sv.delete_deadline = delete_deadline;
    }
}

impl AggrState for UniqueSamplesAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples.iter() {
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
                let mut state = UniqueSamplesStateValue {
                    state: [HashSet::new(); AGGR_STATE_SIZE],
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
        for entry in map.iter() {
            let mut sv = entry.value().lock().unwrap();

            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                sv.deleted = true;
                map.remove(&entry.key());
                continue;
            }

            let state = sv.state[ctx.idx].len();
            sv.state[ctx.idx].clear();

            ctx.append_series(&entry.key(), "unique_samples", state as f64);
        }
    }
}
