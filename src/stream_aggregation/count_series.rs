use crate::stream_aggregation::stream_aggr::{get_input_output_key, AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use xxhash_rust::xxh3::xxh3_64;

pub struct CountSeriesAggrState {
    m: papaya::HashMap<OutputKey, Arc<Mutex<CountSeriesStateValue>>>,
}

struct CountSeriesStateValue {
    state: [HashSet<u64>; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

impl CountSeriesAggrState {
    pub fn new() -> Self {
        Self { m: papaya::HashMap::new() }
    }
}

impl AggrState for CountSeriesAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        let map = self.m.pin();
        for s in samples {
            let (input_key, output_key) = get_input_output_key(&s.key);

            let h = xxh3_64(input_key.as_bytes());

            loop {
                // todo: avoid allocation (to_string)
                let entry = map.get_or_insert_with(output_key.to_string(), || {
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

    fn flush_state(&mut self, ctx: &mut FlushCtx) {
        let map = self.m.pin();
        for (k, value) in map.iter() {
            let mut sv = value.lock().unwrap();

            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                sv.deleted = true;
                map.remove(k);
                continue;
            }

            let state = sv.state[ctx.idx].len();
            sv.state[ctx.idx].clear();

            if state > 0 {
                ctx.append_series(&value.key(), "count_series", state as f64);
            }
        }
    }
}