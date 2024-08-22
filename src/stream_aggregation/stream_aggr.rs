use crate::storage::time_series::TimeSeries;
use crate::storage::Sample;
use metricsql_parser::label::{Label, Labels};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use crate::relabel::{IfExpression, ParsedConfigs, RelabelConfig};
use crate::stream_aggregation::PushSample;
// Placeholder for the Go libraries used in the original code

// Constants
const AGGR_STATE_SIZE: usize = 6;

// Supported outputs
const SUPPORTED_OUTPUTS: [&'static str; 18] = [
    "avg",
    "count_samples",
    "count_series",
    "histogram_bucket",
    "increase",
    "increase_prometheus",
    "last",
    "max",
    "min",
    "quantiles(phi1, ..., phiN)",
    "rate_avg",
    "rate_sum",
    "stddev",
    "stdvar",
    "sum_samples",
    "total",
    "total_prometheus",
    "unique_samples",
];

// Options struct
pub struct Options {
    dedup_interval: Option<Duration>,
    drop_input_labels: Vec<String>,
    no_align_flush_to_interval: bool,
    flush_on_shutdown: bool,
    keep_metric_names: bool,
    ignore_old_samples: bool,
    ignore_first_intervals: usize,
    keep_input: bool,
}

// Config struct
pub struct Config {
    name: Option<String>,
    r#match: Option<IfExpression>,
    interval: Duration,
    no_align_flush_to_interval: Option<bool>,
    flush_on_shutdown: Option<bool>,
    dedup_interval: Option<Duration>,
    staleness_interval: Option<Duration>,
    outputs: Vec<String>,
    keep_metric_names: Option<bool>,
    ignore_old_samples: Option<bool>,
    ignore_first_intervals: Option<usize>,
    by: Vec<String>,
    without: Vec<String>,
    drop_input_labels: Option<Vec<String>>,
    input_relabel_configs: Vec<RelabelConfig>,
    output_relabel_configs: Vec<RelabelConfig>,
}

// Aggregators struct
pub struct Aggregators {
    aggregators: Vec<Arc<Mutex<Aggregator>>>,
    config_data: Vec<u8>,
    file_path: String,
    metrics: Arc<metrics::Set>,
}

impl Aggregators {
    // IsEnabled returns true if Aggregators has at least one configured aggregator
    fn is_enabled(&self) -> bool {
        !self.aggregators.is_empty()
    }

    // MustStop stops a.
    fn must_stop(&mut self) {
        if let Some(ms) = self.ms.take() {
            metrics::unregister_set(ms, true);
        }

        for aggr in &self.aggregators {
            aggr.must_stop();
        }
        self.aggregators.clear();
    }

    // Equal returns true if a and b are initialized from identical configs.
    fn equal(&self, b: &Option<Aggregators>) -> bool {
        match b {
            None => false,
            Some(b) => self.config_data == b.config_data,
        }
    }

    // Push pushes tss to a.
    //
    // Push sets matchIdxs[idx] to 1 if the corresponding tss[idx] was used in aggregations.
    // Otherwise matchIdxs[idx] is set to 0.
    //
    // Push returns matchIdxs with len equal to len(tss).
    // It re-uses the matchIdxs if it has enough capacity to hold len(tss) items.
    // Otherwise it allocates new matchIdxs.
    fn push(&self, tss: Vec<TimeSeries>, mut match_idxs: Vec<u8>) -> Vec<u8> {
        match_idxs.resize(tss.len(), 0);

        if self.aggregators.is_empty() {
            return match_idxs;
        }

        for aggr in &self.aggregators {
            let mut inner = aggr.lock().unwrap();
            inner.push(tss.clone(), &mut match_idxs);
        }

        match_idxs
    }
}

// Aggregator struct
pub struct Aggregator {
    r#match: Option<IfExpression>,
    drop_input_labels: Vec<String>,
    input_relabeling: Option<ParsedConfigs>,
    output_relabeling: Option<ParsedConfigs>,
    staleness_interval: Option<Duration>,
    keep_metric_names: bool,
    ignore_old_samples: bool,
    by: Vec<String>,
    without: Vec<String>,
    aggregate_only_by_time: bool,
    tick_interval: i64,
    interval: Duration,
    dedup_interval: Option<Duration>,
    aggr_outputs: Vec<AggrOutput>,
    suffix: String,
    stop_ch: Arc<Mutex<()>>,
    flush_after: histogram::FastHistogram,
    flush_duration: metrics::Histogram,
    dedup_flush_duration: metrics::Histogram,
    samples_lag: metrics::Histogram,
    flush_timeouts: metrics::Counter,
    dedup_flush_timeouts: metrics::Counter,
    ignored_old_samples: metrics::Counter,
    ignored_nan_samples: metrics::Counter,
    matched_samples: metrics::Counter,
}

// AggrOutput struct
struct AggrOutput {
    aggr_state: Box<dyn AggrState>,
    output_samples: metrics::Counter,
}

// AggrState trait
pub(crate) trait AggrState {
    fn push_samples(&mut self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize);
    fn flush_state(&mut self, ctx: &mut FlushCtx);
}

// PushFunc trait
pub(super) trait PushFunc {
    fn push(&self, tss: Vec<TimeSeries>);
}

// PushCtx struct
pub struct PushCtx {
    samples: [Vec<PushSample>; AGGR_STATE_SIZE],
    labels: Labels,
    input_labels: Labels,
    output_labels: Labels,
    buf: Vec<u8>,
}

// FlushCtx struct
pub struct FlushCtx {
    aggregator: Arc<Mutex<Aggregator>>,
    aggr_output: AggrOutput,
    push_func: Option<Box<dyn PushFunc>>,
    flush_timestamp: i64,
    pub(crate) idx: usize,
    tss: Vec<TimeSeries>,
    labels: Vec<Label>,
    samples: Vec<Sample>,
}

impl FlushCtx {
    pub fn reset(&mut self) {
        self.a = None;
        self.aggr_output = None;
        self.push_func = None;
        self.flush_timestamp = 0;
        self.reset_series();
    }

    pub fn reset_series(&mut self) {
        self.tss.clear();
        self.labels.clear();
        self.samples.clear();
    }

    pub fn flush_series(&mut self) {
        let tss = std::mem::take(&mut self.tss);
        if tss.is_empty() {
            return;
        }

        let output_relabeling = self.a.as_ref().unwrap().output_relabeling.clone();
        if output_relabeling.is_none() {
            if let Some(push_func) = &self.push_func {
                push_func(tss);
                self.ao.as_ref().unwrap().output_samples.fetch_add(tss.len() as u64, std::sync::atomic::Ordering::Relaxed);
            }
            return;
        }

        let mut aux_labels = promutils::get_labels();
        let mut dst_labels = Vec::new();
        let mut dst = Vec::new();
        for mut ts in tss {
            dst_labels.clear();
            dst_labels.extend_from_slice(&ts.labels);
            dst_labels = output_relabeling.as_ref().unwrap().apply(&dst_labels);
            if dst_labels.is_empty() {
                continue;
            }
            ts.labels = dst_labels.clone();
            dst.push(ts);
        }
        if let Some(push_func) = &self.push_func {
            push_func(dst);
            self.ao.as_ref().unwrap().output_samples.fetch_add(dst.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        promutils::put_labels(aux_labels);
    }

    pub fn append_series(&mut self, key: &str, suffix: &str, value: f64) {
        let labels_len = self.labels.len();
        let samples_len = self.samples.len();
        self.labels = decompress_labels(&self.labels, key);
        if !self.a.as_ref().unwrap().keep_metric_names {
            self.labels = add_metric_suffix(&self.labels, labels_len, &self.a.as_ref().unwrap().suffix, suffix);
        }
        self.samples.push(Sample {
            timestamp: self.flush_timestamp,
            value,
        });
        self.tss.push(TimeSeries {
            labels: self.labels[labels_len..].to_vec(),
            samples: self.samples[samples_len..].to_vec(),
        });

        if self.tss.len() >= 10_000 {
            self.flush_series();
        }
    }

    pub fn append_series_with_extra_label(&mut self, key: &str, suffix: &str, value: f64, extra_name: &str, extra_value: &str) {
        let labels_len = self.labels.len();
        let samples_len = self.samples.len();
        self.labels = decompress_labels(&self.labels, key);
        if !self.a.as_ref().unwrap().keep_metric_names {
            self.labels = add_metric_suffix(&self.labels, labels_len, &self.a.as_ref().unwrap().suffix, suffix);
        }
        self.labels.push(Label {
            name: extra_name.to_string(),
            value: extra_value.to_string(),
        });
        self.samples.push(Sample {
            timestamp: self.flush_timestamp,
            value,
        });
        self.tss.push(TimeSeries {
            labels: self.labels[labels_len..].to_vec(),
            samples: self.samples[samples_len..].to_vec(),
        });

        if self.tss.len() >= 10_000 {
            self.flush_series();
        }
    }
}

fn get_flush_ctx(a: Arc<Aggregator>, ao: Arc<AggrOutput>, push_func: Box<dyn Fn(Vec<TimeSeries>) + Send + Sync>, flush_timestamp: i64, idx: usize) -> Arc<Mutex<flushCtx>> {
    let mut pool = FLUSH_CTX_POOL.lock().unwrap();
    let ctx = pool.pop().unwrap_or_else(|| Arc::new(Mutex::new(FlushCtx {
        a: None,
        ao: None,
        push_func: None,
        flush_timestamp: 0,
        idx: 0,
        tss: Vec::new(),
        labels: Vec::new(),
        samples: Vec::new(),
    })));
    let mut ctx_guard = ctx.lock().unwrap();
    ctx_guard.a = Some(a);
    ctx_guard.ao = Some(ao);
    ctx_guard.push_func = Some(push_func);
    ctx_guard.flush_timestamp = flush_timestamp;
    ctx_guard.idx = idx;
    Arc::clone(&ctx)
}

fn put_flush_ctx(mut ctx: Arc<Mutex<FlushCtx>>) {
    let mut ctx_guard = ctx.lock().unwrap();
    ctx_guard.reset();
    let mut pool = FLUSH_CTX_POOL.lock().unwrap();
    pool.push(Arc::clone(&ctx));
}

fn main() {
    // Example usage of the Aggregators and Aggregator structs
    let options = Options {
        dedup_interval: Some(Duration::from_secs(10)),
        drop_input_labels: vec![],
        no_align_flush_to_interval: false,
        flush_on_shutdown: true,
        keep_metric_names: false,
        ignore_old_samples: false,
        ignore_first_intervals: 0,
        keep_input: false,
    };

    let config = Config {
        name: Some("example".to_string()),
        r#match: None,
        interval: Duration::from_secs(60),
        no_align_flush_to_interval: None,
        flush_on_shutdown: None,
        dedup_interval: None,
        staleness_interval: None,
        outputs: vec!["avg".to_string()],
        keep_metric_names: None,
        ignore_old_samples: None,
        ignore_first_intervals: None,
        by: vec![],
        without: vec![],
        drop_input_labels: None,
        input_relabel_configs: vec![],
        output_relabel_configs: vec![],
    };

    let metrics_set = Arc::new(metrics::Set::new());
    let aggregators = Aggregators::load_from_data(
        vec![],
        Box::new(|_| {}),
        &options,
        "alias".to_string(),
        metrics_set.clone(),
    ).unwrap();

    // Further usage of aggregators would follow, but are omitted for brevity.
}