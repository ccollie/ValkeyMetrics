use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use crate::stream_aggregation::stream_aggr::FlushCtx;

struct RateAggrState {
    m: DashMap<OutputKey, Arc<Mutex<RateStateValue>>>,
    is_avg: bool,
}

struct RateStateValue {
    state: DashMap<String, RateState>,
    deleted: bool,
    delete_deadline: i64,
}

struct RateState {
    last_values: [RateLastValueState; AGGR_STATE_SIZE],
    prev_timestamp: i64,
    prev_value: f64,
    delete_deadline: i64,
}

struct RateLastValueState {
    first_value: f64,
    value: f64,
    timestamp: i64,
    total: f64,
}

impl RateAggrState {
    fn new(is_avg: bool) -> Self {
        Self {
            m: DashMap::new(),
            is_avg,
        }
    }

    fn push_samples(&self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let (input_key, output_key) = get_input_output_key(&s.key);

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(RateStateValue {
                        state: DashMap::new(),
                        deleted: false,
                        delete_deadline,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                let mut state = sv.state.entry(input_key.clone()).or_insert_with(|| RateState {
                    last_values: [RateLastValueState {
                        first_value: 0.0,
                        value: 0.0,
                        timestamp: 0,
                        total: 0.0,
                    }; AGGR_STATE_SIZE],
                    prev_timestamp: 0,
                    prev_value: 0.0,
                    delete_deadline,
                });

                let mut lv = state.last_values[idx];
                if lv.timestamp > 0 {
                    if s.timestamp < lv.timestamp {
                        continue;
                    }
                    if state.prev_timestamp == 0 {
                        state.prev_timestamp = lv.timestamp;
                        state.prev_value = lv.value;
                    }
                    if s.value >= lv.value {
                        lv.total += s.value - lv.value;
                    } else {
                        lv.total += s.value;
                    }
                } else if state.prev_timestamp > 0 {
                    lv.first_value = s.value;
                }
                lv.value = s.value;
                lv.timestamp = s.timestamp;
                state.last_values[idx] = lv;
                state.delete_deadline = delete_deadline;
                sv.state.insert(input_key.clone(), state.clone());
                sv.delete_deadline = delete_deadline;
                break;
            }
        }
    }

    fn get_suffix(&self) -> String {
        if self.is_avg {
            "rate_avg".to_string()
        } else {
            "rate_sum".to_string()
        }
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

            let mut rate = 0.0;
            let mut count_series = 0;

            for state_entry in sv.state.iter() {
                let mut state = state_entry.value().clone();
                if now > state.delete_deadline {
                    sv.state.remove(&state_entry.key());
                    continue;
                }

                let v1 = state.last_values[ctx.idx];
                let rate_interval = v1.timestamp - state.prev_timestamp;
                if rate_interval > 0 && state.prev_timestamp > 0 {
                    if v1.first_value >= state.prev_value {
                        v1.total += v1.first_value - state.prev_value;
                    } else {
                        v1.total += v1.first_value;
                    }

                    rate += (v1.total) * 1000.0 / rate_interval as f64;
                    state.prev_timestamp = v1.timestamp;
                    state.prev_value = v1.value;
                    count_series += 1;
                }

                state.last_values[ctx.idx] = RateLastValueState {
                    first_value: 0.0,
                    value: 0.0,
                    timestamp: 0,
                    total: 0.0,
                };
                sv.state.insert(state_entry.key().clone(), state);
            }

            if count_series > 0 {
                if self.is_avg {
                    rate /= count_series as f64;
                }
                ctx.append_series(&entry.key(), &suffix, rate);
            }
        }
    }
}
