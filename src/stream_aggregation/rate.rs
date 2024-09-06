use crate::stream_aggregation::stream_aggr::{get_input_output_key, AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use std::sync::{Arc, Mutex};
use ahash::{HashMap, HashMapExt, RandomState};

pub struct RateAggrState {
    map: papaya::HashMap<OutputKey, Arc<Mutex<RateStateValue>>, RandomState>,
    is_avg: bool,
}

struct RateStateValue {
    state: HashMap<String, RateState>,
    deleted: bool,
    delete_deadline: i64,
}

struct RateState {
    last_values: [RateLastValueState; AGGR_STATE_SIZE],
    prev_timestamp: i64,
    prev_value: f64,
    delete_deadline: i64,
}

#[derive(Clone)]
struct RateLastValueState {
    first_value: f64,
    value: f64,
    timestamp: i64,
    total: f64,
}

impl RateAggrState {
    pub fn new(is_avg: bool) -> Self {
        Self {
            map: papaya::HashMap::with_hasher(RandomState::new()),
            is_avg,
        }
    }

    fn get_suffix(&self) -> &'static str {
        if self.is_avg {
            "rate_avg"
        } else {
            "rate_sum"
        }
    }
}

impl AggrState for RateAggrState {
    fn push_samples(&mut self, samples: &Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples.iter() {
            let (input_key, output_key) = get_input_output_key(&s.key);

            loop {
                let map = self.map.pin();
                let mut entry = map.get_or_insert_with(output_key.to_string(), ||{
                    Arc::new(Mutex::new(RateStateValue {
                        state: HashMap::new(),
                        deleted: false,
                        delete_deadline,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                // todo: how to eliminate clone
                let mut state = sv.state.entry(input_key.to_string()).or_insert_with(|| RateState {
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

                if let Some(lv) = state.last_values.get_mut(idx) {
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
                    state.last_values[idx] = lv.clone();
                    state.delete_deadline = delete_deadline;
                    // todo: how to eliminate alloc (to_string)
                    sv.state.insert(input_key.to_string(), state.clone());
                    sv.delete_deadline = delete_deadline;
                    break;
                }
            }
        }
    }

    fn flush_state(&mut self, ctx: &mut FlushCtx) {
        let suffix = self.get_suffix();

        let map = self.map.pin();
        for entry in map.iter() {
            let mut sv = entry.value().lock().unwrap();

            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                sv.deleted = true;
                let guard = self.map.guard();
                self.map.remove(&entry.key(), &guard);
                continue;
            }

            let mut rate = 0.0;
            let mut count_series = 0;

            for mut state_entry in sv.state.iter_mut() {
                let mut state = state_entry.value_mut();
                if ctx.flush_timestamp > state.delete_deadline {
                    sv.state.remove(&state_entry.key());
                    continue;
                }

                let v1 = state.last_values.get_mut(ctx.idx);
                if v1.is_none() {
                    continue;
                }
                let v1 = v1.unwrap();
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
                sv.state.insert(state_entry.key().clone(), state.clone());
            }

            if count_series > 0 {
                if self.is_avg {
                    rate /= count_series as f64;
                }
                ctx.append_series(&entry.key(), suffix, rate);
            }
        }
    }
}