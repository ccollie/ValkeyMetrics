use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use crate::stream_aggregation::PushSample;
use crate::stream_aggregation::stream_aggr::FlushCtx;

const AGGR_STATE_SIZE: usize = 8; // Assuming aggrStateSize is 8 based on the Go code

type OutputKey = String;

pub struct TotalAggrState {
    m: DashMap<OutputKey, Arc<Mutex<TotalStateValue>>>,
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
    last_values: HashMap<String, TotalLastValueState>,
}

struct TotalLastValueState {
    value: f64,
    timestamp: i64,
    delete_deadline: i64,
}

impl TotalAggrState {
    fn new(reset_total_on_flush: bool, keep_first_sample: bool) -> Self {
        Self {
            m: DashMap::new(),
            reset_total_on_flush,
            keep_first_sample,
        }
    }

    fn push_samples(&self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let (input_key, output_key) = get_input_output_key(&s.key);

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(TotalStateValue {
                        shared: TotalState {
                            total: 0.0,
                            last_values: HashMap::new(),
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

                let mut lv = sv.shared.last_values.get(&input_key).cloned().unwrap_or_default();
                if lv.timestamp != 0 || self.keep_first_sample {
                    if s.timestamp < lv.timestamp {
                        continue;
                    }

                    if s.value >= lv.value {
                        sv.state[idx] += s.value - lv.value;
                    } else {
                        sv.state[idx] += s.value;
                    }
                }
                lv.value = s.value;
                lv.timestamp = s.timestamp;
                lv.delete_deadline = delete_deadline;

                sv.shared.last_values.insert(input_key.clone(), lv);
                sv.delete_deadline = delete_deadline;
                break;
            }
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

    fn flush_state(&self, ctx: &FlushCtx) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64;

        let suffix = self.get_suffix();

        for entry in self.m.iter() {
            let mut sv = entry.value().lock().unwrap();

            if now > sv.delete_deadline {
                sv.deleted = true;
                self.m.remove(&entry.key());
                continue;
            }

            let total = sv.shared.total + sv.state[ctx.idx];
            sv.shared.last_values.retain(|_, v| now <= v.delete_deadline);
            sv.state[ctx.idx] = 0.0;

            if !self.reset_total_on_flush {
                if total.abs() >= (1 << 53) as f64 {
                    sv.shared.total = 0.0;
                } else {
                    sv.shared.total = total;
                }
            }

            ctx.append_series(&entry.key(), suffix, total);
        }
    }
}
