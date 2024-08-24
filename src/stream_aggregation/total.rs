use super::stream_aggr::{get_input_output_key, AggrState, FlushCtx};
use crate::stream_aggregation::{PushSample, AGGR_STATE_SIZE};
use ahash::RandomState;
use bytes::Bytes;
use papaya::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) struct TotalAggrState {
    m: HashMap<Bytes, Arc<Mutex<TotalStateValue>>, RandomState>,
    reset_total_on_flush: bool,
    keep_first_sample: bool,
}

struct TotalStateValue {
    shared: TotalState,
    state: [f64; AGGR_STATE_SIZE],
    delete_deadline: i64,
    deleted: bool,
}

struct TotalState {
    total: f64,
    last_values: HashMap<Bytes, TotalLastValueState, RandomState>,
}

struct TotalLastValueState {
    value: f64,
    timestamp: i64,
    delete_deadline: i64,
}

impl TotalAggrState {
    pub(crate) fn new(reset_total_on_flush: bool, keep_first_sample: bool) -> Self {
        Self {
            m: HashMap::with_hasher(RandomState::new()),
            reset_total_on_flush,
            keep_first_sample,
        }
    }

    fn get_suffix(&self) -> &'static str {
        if self.reset_total_on_flush {
            if self.keep_first_sample {
                return "increase";
            }
            return "increase_prometheus";
        }
        if self.keep_first_sample {
            return "total";
        }
        "total_prometheus"
    }
}

impl AggrState for TotalAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples.iter() {
            let (input_key, output_key) = get_input_output_key(s.key);

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(TotalStateValue {
                        shared: TotalState {
                            total: 0.0,
                            last_values: HashMap::with_hasher(RandomState::new()),
                        },
                        state: [0.0; AGGR_STATE_SIZE],
                        delete_deadline,
                        deleted: false,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                if let Some(lv) = sv.shared.last_values.get_mut(&input_key) {
                    if s.timestamp < lv.timestamp {
                        continue;
                    }

                    if s.value >= lv.value {
                        sv.state[idx] += s.value - lv.value;
                    } else {
                        sv.state[idx] += s.value;
                    }
                } else if self.keep_first_sample {
                    sv.state[idx] += s.value;
                }

                let new_lv = TotalLastValueState {
                    value: s.value,
                    timestamp: s.timestamp,
                    delete_deadline,
                };

                sv.shared.last_values.insert(input_key.clone(), new_lv);
                sv.delete_deadline = delete_deadline;
                break;
            }
        }
    }

    fn flush_state(&mut self, ctx: &mut FlushCtx) {
        let suffix = self.get_suffix();
        for entry in self.m.iter() {
            let mut sv = entry.value().lock().unwrap();
            if ctx.flush_timestamp > sv.delete_deadline {
                sv.deleted = true;
                self.m.remove(&entry.key());
                continue;
            }

            let total = sv.shared.total + sv.state[ctx.idx];
            sv.shared.last_values.retain(|_, v| ctx.flush_timestamp <= v.delete_deadline);
            sv.state[ctx.idx] = 0.0;

            if !self.reset_total_on_flush {
                if total.abs() >= (1 << 53) as f64 {
                    sv.shared.total = 0.0;
                } else {
                    sv.shared.total = total;
                }
            }

            ctx.append_series(entry.key().clone(), suffix, total);
        }
    }
}