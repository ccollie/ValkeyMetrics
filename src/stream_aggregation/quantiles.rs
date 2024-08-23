use crate::stream_aggregation::stream_aggr::{AggrState, FlushCtx};
use crate::stream_aggregation::{OutputKey, PushSample, AGGR_STATE_SIZE};
use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use crate::stream_aggregation::fast_histogram::FastHistogram;
// Assuming a suitable histogram crate

pub struct QuantilesAggrState {
    m: DashMap<OutputKey, Arc<Mutex<QuantilesStateValue>>>,
    phis: Vec<f64>,
}

struct QuantilesStateValue {
    state: [Histogram; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

impl QuantilesAggrState {
    pub fn new(phis: Vec<f64>) -> Self {
        Self { m: DashMap::new(), phis }
    }

    pub fn flush_state(&self, ctx: &mut FlushCtx, flush_timestamp: i64, idx: usize) {
        let mut quantiles = Vec::new();
        let mut b = String::new();

        for entry in self.m.iter() {
            let (key, sv) = entry.pair();
            let _guard = sv.mu.lock();

            if flush_timestamp > sv.delete_deadline {
                sv.deleted = true;
                self.m.remove(key);
                continue;
            }

            if let Some(state) = &sv.state[idx] {
                quantiles.clear();
                state.quantiles(&mut quantiles, &self.phis);
                state.reset();

                for (i, quantile) in quantiles.iter().enumerate() {
                    b.clear();
                    write!(&mut b, "{}", self.phis[i]).unwrap();
                    let phi_str = b.as_str().into();
                    ctx.append_series_with_extra_label(key, "quantiles", flush_timestamp, *quantile, "quantile", phi_str);
                }
            }
        }
    }

}

impl AggrState for QuantilesAggrState {
    fn push_samples(&self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for s in samples {
            let output_key = get_output_key(&s.key);

            loop {
                let entry = self.m.entry(output_key.clone()).or_insert_with(|| {
                    Arc::new(Mutex::new(QuantilesStateValue {
                        state: [None; AGGR_STATE_SIZE],
                        deleted: false,
                        delete_deadline,
                    }))
                });

                let mut sv = entry.lock().unwrap();
                if sv.deleted {
                    continue;
                }

                if sv.state[idx].is_none() {
                    sv.state[idx] = Some(Histogram::new());
                }
                if let Some(hist) = &mut sv.state[idx] {
                    hist.increment(s.value as u64).unwrap();
                }
                sv.delete_deadline = delete_deadline;
                break;
            }
        }
    }

    fn flush_state(&self, ctx: &mut FlushCtx) {
        for entry in self.m.iter() {
            let mut sv = entry.value().lock().unwrap();

            let deleted = ctx.flush_timestamp > sv.delete_deadline;
            if deleted {
                sv.deleted = true;
                self.m.remove(&entry.key());
                continue;
            }

            let state = sv.state[ctx.idx].take();

            if let Some(hist) = state {
                let quantiles = self.phis.iter().map(|&phi| hist.percentile(phi * 100.0).unwrap()).collect::<Vec<_>>();
                for (i, &quantile) in quantiles.iter().enumerate() {
                    let phi_str = format!("{}", self.phis[i]);
                    ctx.append_series_with_extra_label(&entry.key(), "quantiles", quantile, "quantile", &phi_str);
                }
            }
        }
    }
}