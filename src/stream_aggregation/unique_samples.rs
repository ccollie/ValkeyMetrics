use crate::stream_aggregation::stream_aggr::{AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub struct UniqueSamplesAggrState {
    m: DashMap<OutputKey, Arc<Mutex<UniqueSamplesStateValue>>>,
}

struct UniqueSamplesStateValue {
    state: [HashSet<f64>; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

impl UniqueSamplesAggrState {
    pub fn new() -> Self {
        Self { m: DashMap::new() }
    }
}

impl AggrState for UniqueSamplesAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(&s.key);

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(UniqueSamplesStateValue {
                        state: [HashSet::new(); AGGR_STATE_SIZE],
                        deleted: false,
                        delete_deadline,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                sv.state[idx].insert(s.value);
                sv.delete_deadline = delete_deadline;
                break;
            }
        }
    }

    fn flush_state(&mut self, ctx: &mut FlushCtx) {
        for entry in self.m.iter() {
            let mut sv = entry.value().lock().unwrap();

            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                sv.deleted = true;
                self.m.remove(&entry.key());
                continue;
            }

            let state = sv.state[ctx.idx].len();
            sv.state[ctx.idx].clear();

            ctx.append_series(&entry.key(), "unique_samples", state as f64);
        }
    }
}
