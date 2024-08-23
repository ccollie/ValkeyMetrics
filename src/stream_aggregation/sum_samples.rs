use crate::stream_aggregation::stream_aggr::{AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use dashmap::DashMap;
use std::sync::{Arc, Mutex};

pub struct SumSamplesAggrState {
    m: DashMap<OutputKey, Arc<Mutex<SumSamplesStateValue>>>,
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
        Self { m: DashMap::new() }
    }
}

impl AggrState for SumSamplesAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(&s.key);

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(SumSamplesStateValue {
                        state: [SumState { sum: 0.0, exists: false }; AGGR_STATE_SIZE],
                        deleted: false,
                        delete_deadline,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                sv.state[idx].sum += s.value;
                sv.state[idx].exists = true;
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

            let state = &sv.state[ctx.idx];
            sv.state[ctx.idx] = SumState { sum: 0.0, exists: false };

            if state.exists {
                ctx.append_series(&entry.key(), "sum_samples", state.sum);
            }
        }
    }
}