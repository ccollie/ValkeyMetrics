use crate::stream_aggregation::stream_aggr::{AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use dashmap::DashMap;
use std::sync::{Arc, Mutex};

pub struct StdvarAggrState {
    m: DashMap<OutputKey, Arc<Mutex<StdvarStateValue>>>,
}

pub struct StdvarStateValue {
    state: [StdvarState; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

struct StdvarState {
    count: f64,
    avg: f64,
    q: f64,
}

impl StdvarAggrState {
    pub fn new() -> Self {
        Self { m: DashMap::new() }
    }
}

impl AggrState for StdvarAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(&s.key);

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(StdvarStateValue {
                        state: [StdvarState { count: 0.0, avg: 0.0, q: 0.0 }; AGGR_STATE_SIZE],
                        deleted: false,
                        delete_deadline,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                let state = &mut sv.state[idx];
                state.count += 1.0;
                let avg = state.avg + (s.value - state.avg) / state.count;
                state.q += (s.value - state.avg) * (s.value - avg);
                state.avg = avg;
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
            sv.state[ctx.idx] = StdvarState { count: 0.0, avg: 0.0, q: 0.0 };

            if state.count > 0.0 {
                ctx.append_series(&entry.key(), "stdvar", state.q / state.count);
            }
        }
    }
}