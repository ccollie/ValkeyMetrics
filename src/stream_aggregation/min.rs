use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use crate::stream_aggregation::stream_aggr::FlushCtx;


pub struct MinAggrState {
    m: DashMap<OutputKey, Arc<Mutex<MinStateValue>>>,
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
    fn new() -> Self {
        Self { m: DashMap::new() }
    }

    fn push_samples(&self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(&s.key);

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(MinStateValue {
                        state: [MinState { min: 0.0, exists: false }; AGGR_STATE_SIZE],
                        deleted: false,
                        delete_deadline,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                let state = &mut sv.state[idx];
                if !state.exists {
                    state.min = s.value;
                    state.exists = true;
                } else if s.value < state.min {
                    state.min = s.value;
                }
                sv.delete_deadline = delete_deadline;
                break;
            }
        }
    }

    fn flush_state(&self, ctx: &FlushCtx) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64;

        for entry in self.m.iter() {
            let mut sv = entry.value().lock().unwrap();

            if now > sv.delete_deadline {
                sv.deleted = true;
                self.m.remove(&entry.key());
                continue;
            }

            let state = &sv.state[ctx.idx];
            sv.state[ctx.idx] = MinState { min: 0.0, exists: false };

            if state.exists {
                ctx.append_series(&entry.key(), "min", state.min);
            }
        }
    }
}