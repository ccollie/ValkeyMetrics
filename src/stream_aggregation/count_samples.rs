use crate::stream_aggregation::stream_aggr::{get_output_key, AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use papaya::HashMap;
use std::sync::{Arc, Mutex};


pub(crate) struct CountSamplesAggrState {
    m: HashMap<OutputKey, Arc<Mutex<CountSamplesStateValue>>, ahash::RandomState>,
}

struct CountSamplesStateValue {
    state: [u64; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

impl CountSamplesAggrState {
    pub fn new() -> Self {
        Self {
            m: HashMap::with_hasher(ahash::RandomState::new()),
        }
    }

}

impl AggrState for CountSamplesAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        let map = self.m.pin();
        for s in samples.iter() {
            let output_key = get_output_key(&s.key);

            loop {
                let entry = map.get_or_insert_with(output_key.to_string(), || {
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

    fn flush_state(&mut self, ctx: &mut FlushCtx) {
        let map = self.m.pin();
        for entry in map.iter() {
            let mut sv = entry.value().lock().unwrap();

            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                sv.deleted = true;
                self.m.remove(entry.output_key);
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