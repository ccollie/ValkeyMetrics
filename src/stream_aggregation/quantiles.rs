use crate::stream_aggregation::stream_aggr::{get_output_key, AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use std::sync::{Arc, Mutex};
use papaya::HashMap;
use super::fast_histogram::FastHistogram;

pub struct QuantilesAggrState {
    m: HashMap<OutputKey, Arc<Mutex<QuantilesStateValue>>>,
    phis: Vec<f64>,
}

struct QuantilesStateValue {
    state: [Option<FastHistogram>; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

impl QuantilesAggrState {
    pub fn new(phis: Vec<f64>) -> Self {
        Self { m: HashMap::new(), phis }
    }

    fn update_state(sv: &mut QuantilesStateValue, sample: &PushSample, delete_deadline: i64, idx: usize) {
        let state = &mut sv.state[idx];
        if state.is_none() {
            *state = Some(FastHistogram::new());
        }
        if let Some(hist) = state {
            hist.increment(sample.value as u64).unwrap();
        }
        sv.delete_deadline = delete_deadline;
    }
}

impl AggrState for QuantilesAggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(&s.key);

            let map = self.m.pin();
            let entry = map.get(&output_key);
            if let Some(entry) = entry {
                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }
                Self::update_state(&mut sv, &s, delete_deadline, idx);
            } else {
                let mut state = QuantilesStateValue {
                    state: [None; AGGR_STATE_SIZE],
                    deleted: false,
                    delete_deadline,
                };
                Self::update_state(&mut state, &s, delete_deadline, idx);
                let sv = Arc::new(Mutex::new(state));
                map.insert(output_key.to_string(), sv);
            }
        }
    }

    fn flush_state(&mut self, ctx: &mut FlushCtx) {
        let mut map = self.m.pin();
        for (key, value) in map.iter() {
            let mut sv = value.lock().unwrap();

            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                sv.deleted = true;
                map.remove(key);
                continue;
            }

            let state = sv.state[ctx.idx].take();

            if let Some(mut hist) = state {
                let quantiles = self.phis.iter().map(|&phi| hist.quantile(phi * 100.0)).collect::<Vec<_>>();
                for (i, &quantile) in quantiles.iter().enumerate() {
                    let phi_str = format!("{}", self.phis[i]);
                    ctx.append_series_with_extra_label(&key, "quantiles", quantile, "quantile", &phi_str);
                }
            }
        }
    }
}