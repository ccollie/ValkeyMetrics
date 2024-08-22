use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use histogram::Histogram;
use crate::stream_aggregation::{PushSample, AGGR_STATE_SIZE};
use crate::stream_aggregation::stream_aggr::FlushCtx;
// Assuming a suitable histogram crate

type OutputKey = String;

struct QuantilesAggrState {
    m: DashMap<OutputKey, Arc<Mutex<QuantilesStateValue>>>,
    phis: Vec<f64>,
}

struct QuantilesStateValue {
    state: [Option<Histogram>; AGGR_STATE_SIZE],
    deleted: bool,
    delete_deadline: i64,
}

impl QuantilesAggrState {
    fn new(phis: Vec<f64>) -> Self {
        Self { m: DashMap::new(), phis }
    }

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
