use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use papaya::HashMap;

const AGGR_STATE_SIZE: usize = 8; // Assuming aggrStateSize is 8 based on the Go code

type OutputKey = String;

struct CountSamplesAggrState {
    map: HashMap<OutputKey, Arc<Mutex<CountSamplesStateValue>>>,
    m: DashMap<OutputKey, Arc<Mutex<CountSamplesStateValue>>>,
}

struct CountSamplesStateValue {
    state: [u64; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

impl CountSamplesAggrState {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            m: DashMap::new()
        }
    }

    fn push_samples(&self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(&s.key);

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(CountSamplesStateValue {
                        state: [0; AGGR_STATE_SIZE],
                        deleted: false,
                        delete_deadline,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                sv.state[idx] += 1;
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

            let state = sv.state[ctx.idx];
            sv.state[ctx.idx] = 0;

            if state > 0 {
                ctx.append_series(&entry.key(), "count_samples", state as f64);
            }
        }
    }
}

struct FlushCtx {
    flush_timestamp: i64,
    idx: usize,
    append_series: fn(&str, &str, f64),
}

fn get_output_key(key: &str) -> OutputKey {
    key.to_string()
}