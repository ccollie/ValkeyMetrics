use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use crate::stream_aggregation::stream_aggr::FlushCtx;

pub struct LastAggrState {
    m: DashMap<OutputKey, Arc<Mutex<LastStateValue>>>,
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
    fn new() -> Self {
        Self { m: DashMap::new() }
    }

    fn push_samples(&self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(&s.key);

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(LastStateValue {
                        state: [LastState { last: 0.0, timestamp: 0 }; AGGR_STATE_SIZE],
                        deleted: false,
                        delete_deadline,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                if s.timestamp >= sv.state[idx].timestamp {
                    sv.state[idx].last = s.value;
                    sv.state[idx].timestamp = s.timestamp;
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
            sv.state[ctx.idx] = LastState { last: 0.0, timestamp: 0 };

            if state.timestamp > 0 {
                ctx.append_series(&entry.key(), "last", state.last);
            }
        }
    }
}