use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use crate::stream_aggregation::stream_aggr::FlushCtx;

struct StdvarAggrState {
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
    fn new() -> Self {
        Self { m: DashMap::new() }
    }

    fn push_samples(&self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
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
            sv.state[ctx.idx] = StdvarState { count: 0.0, avg: 0.0, q: 0.0 };

            if state.count > 0.0 {
                ctx.append_series(&entry.key(), "stdvar", state.q / state.count);
            }
        }
    }
}
