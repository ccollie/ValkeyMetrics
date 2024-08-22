use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashSet;
use xxhash_rust::xxh3::xxh3_64;
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use crate::stream_aggregation::stream_aggr::FlushCtx;

pub struct CountSeriesAggrState {
    m: DashMap<OutputKey, Arc<Mutex<CountSeriesStateValue>>>,
}

struct CountSeriesStateValue {
    state: [HashSet<u64>; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

impl CountSeriesAggrState {
    fn new() -> Self {
        Self { m: DashMap::new() }
    }

    fn push_samples(&self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let (input_key, output_key) = get_input_output_key(&s.key);

            let h = xxh3_64(input_key.as_bytes());

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(CountSeriesStateValue {
                        state: [HashSet::new(); AGGR_STATE_SIZE],
                        deleted: false,
                        delete_deadline,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                sv.state[idx].insert(h);
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

            let state = sv.state[ctx.idx].len();
            sv.state[ctx.idx].clear();

            if state > 0 {
                ctx.append_series(&entry.key(), "count_series", state as f64);
            }
        }
    }
}
