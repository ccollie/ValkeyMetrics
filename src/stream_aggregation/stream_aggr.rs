use crate::common::current_time_millis;
use crate::relabel::{IfExpression, ParsedConfigs};
use super::timeseries::TimeSeries;
use crate::storage::Sample;
use crate::stream_aggregation::avg::AvgAggrState;
use crate::stream_aggregation::count_samples::CountSamplesAggrState;
use crate::stream_aggregation::count_series::CountSeriesAggrState;
use crate::stream_aggregation::fast_histogram::FastHistogram;
use crate::stream_aggregation::histogram_bucket::HistogramBucketAggrState;
use crate::stream_aggregation::last::LastAggrState;
use crate::stream_aggregation::max::MaxAggrState;
use crate::stream_aggregation::min::MinAggrState;
use crate::stream_aggregation::quantiles::QuantilesAggrState;
use crate::stream_aggregation::rate::RateAggrState;
use crate::stream_aggregation::stddev::StddevAggrState;
use crate::stream_aggregation::stdvar::StdvarAggrState;
use crate::stream_aggregation::sum_samples::SumSamplesAggrState;
use crate::stream_aggregation::total::TotalAggrState;
use crate::stream_aggregation::unique_samples::UniqueSamplesAggrState;
use crate::stream_aggregation::utils::{add_metric_suffix, sort_and_remove_duplicates};
use crate::stream_aggregation::{AggregationOutput, PushSample};
use ahash::{HashSet, HashSetExt};
use metricsql_common::prelude::{AtomicCounter, histogram::Histogram};
use metricsql_runtime::{Label, Labels};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use crate::stream_aggregation::deduplicator::drop_series_labels;

// Constants
const AGGR_STATE_SIZE: usize = 2;

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
#[derive(Serialize, Deserialize)]
pub struct StreamingAggregationConfig {
    /// `name` is an optional name of the given streaming aggregation config.
    ///
    /// If it is set, then it is used as `name` label in the exposed metrics
    /// for the given aggregation config at /metrics page.
    /// See https://docs.victoriametrics.com/vmagent/#monitoring and https://docs.victoriametrics.com/#monitoring
    pub name: Option<String>,

    /// an optional filter for incoming samples to aggregate.
    /// It can contain arbitrary Prometheus series selector
    /// according to https://docs.victoriametrics.com/keyconcepts/#filtering .
    /// If match isn't set, then all the incoming samples are aggregated.
    ///
    /// match also can contain a list of series selectors. Then the incoming samples are aggregated
    /// if they match at least a single series selector.
    pub r#match: Option<IfExpression>,

    /// `interval` is the interval for the aggregation.
    /// The aggregated stats is sent to remote storage once per interval.
    pub interval: Duration,

    /// `dedup_interval` is an optional interval for de-duplication of input samples before the aggregation.
    /// Samples are de-duplicated on a per-series basis. See https://docs.victoriametrics.com/keyconcepts/#time-series
    /// and https://docs.victoriametrics.com/#deduplication
    /// The deduplication is performed after input_relabel_configs relabeling is applied.
    /// By default, the deduplication is disabled unless -remoteWrite.streamAggr.dedupInterval or -streamAggr.dedupInterval
    /// command-line flags are set.
    pub dedup_interval: Option<Duration>,

    /// an optional interval for resetting the per-series state if no new samples
    /// are received during this interval for the following outputs:
    /// - histogram_bucket
    /// - increase
    /// - increase_prometheus
    /// - rate_avg
    /// - rate_sum
    /// - total
    /// - total_prometheus
    /// See https://docs.victoriametrics.com/stream-aggregation/#staleness for more details.
    pub staleness_interval: Option<Duration>,

    /// disables aligning of flush times for the aggregated data to multiples of interval.
    /// By default, flush times for the aggregated data is aligned to multiples of interval.
    /// For example:
    /// - if `interval: 1m` is set, then flushes happen at the end of every minute,
    /// - if `interval: 1h` is set, then flushes happen at the end of every hour
    pub no_align_flush_to_interval: Option<bool>,

    /// flush_on_shutdown instructs to flush aggregated data to the storage on the first and the last intervals
    /// during vmagent starts, restarts or configuration reloads.
    /// Incomplete aggregated data isn't flushed to the storage by default, since it is usually confusing.
    pub flush_on_shutdown: Option<bool>,

    /// an optional list of labels, which must be removed from the output aggregation.
    /// See https://docs.victoriametrics.com/stream-aggregation/#aggregating-by-labels
    pub without: Option<Vec<String>>,

    /// `by` is an optional list of labels, which must be preserved in the output aggregation.
    /// See https://docs.victoriametrics.com/stream-aggregation/#aggregating-by-labels
    pub by: Option<Vec<String>>,

    /// the list of unique aggregations to perform on the input data.
    /// See https://docs.victoriametrics.com/stream-aggregation/#aggregation-outputs
    pub outputs: Vec<AggregationOutput>,

    /// instructs keeping the original metric names for the aggregated samples.
    /// This option can't be enabled together with `-streamAggr.keepInput` or `-remoteWrite.streamAggr.keepInput`.
    /// This option can be set only if outputs list contains a single output.
    /// By default, a special suffix is added to original metric names in the aggregated samples.
    /// See https://docs.victoriametrics.com/stream-aggregation/#output-metric-names
    pub keep_metric_names: Option<bool>,

    /// ignore_old_samples instructs ignoring input samples with old timestamps outside the current aggregation interval.
    /// See https://docs.victoriametrics.com/stream-aggregation/#ignoring-old-samples
    /// See also -remoteWrite.streamAggr.ignoreOldSamples and -streamAggr.ignoreOldSamples command-line flag.
    pub ignore_old_samples: Option<bool>,

    /// `ignore_first_intervals` instructs ignoring the first N aggregation intervals after process start.
    /// See https://docs.victoriametrics.com/stream-aggregation/#ignore-aggregation-intervals-on-start
    /// See also -remoteWrite.streamAggr.ignoreFirstIntervals and -streamAggr.ignoreFirstIntervals command-line flags.
    pub ignore_first_intervals: Option<u32>,

    /// `drop_input_labels` instructs dropping the given labels from input samples.
    /// The labels' dropping is performed before input_relabel_configs are applied.
    /// This also means that the labels are dropped before de-duplication ( https://docs.victoriametrics.com/stream-aggregation/#deduplication )
    /// and stream aggregation.
    pub drop_input_labels: Option<Vec<String>>,

    /// `input_relabel_configs` is an optional relabeling rules,
    /// which are applied to the incoming samples after they pass the match filter
    /// and before being aggregated.
    /// See https://docs.victoriametrics.com/stream-aggregation/#relabeling
    pub input_relabel_configs: Option<ParsedConfigs>,

    /// output_relabel_configs is an optional relabeling rules,
    /// which are applied to the aggregated output metrics.
    pub output_relabel_configs: Option<ParsedConfigs>,
}


// Aggregators struct
pub struct Aggregators {
    aggregators: Vec<Arc<Mutex<Aggregator>>>,
    config_data: Vec<u8>,
    file_path: String,
}

impl Aggregators {
    /// is_enabled returns true if Aggregators has at least one configured aggregator
    fn is_enabled(&self) -> bool {
        !self.aggregators.is_empty()
    }

    // MustStop stops a.
    fn must_stop(&mut self) {
        for aggr in &self.aggregators {
            aggr.must_stop();
        }
        self.aggregators.clear();
    }

    /// returns true if a and b are initialized from identical configs.
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
    fn push(&self, tss: Vec<TimeSeries>, mut match_indices: Vec<u8>) -> Vec<u8> {
        match_indices.resize(tss.len(), 0);

        if self.aggregators.is_empty() {
            return match_indices;
        }

        for aggr in &self.aggregators {
            let mut inner = aggr.lock().unwrap();
            inner.push(tss.clone(), &mut match_indices);
        }

        match_indices
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
    dedup_interval: Duration,
    aggr_outputs: Vec<AggrOutput>,
    suffix: String,
    stop_ch: Arc<Mutex<()>>,
    min_timestamp: i64,
    flush_after: FastHistogram,
    flush_duration: Histogram,
    dedup_flush_duration: Histogram,
    samples_lag: Histogram,
    flush_timeouts: u64,
    dedup_flush_timeouts: u64,
    ignored_old_samples: u64,
    ignored_nan_samples: u64,
    matched_samples: u64,
}

// AggrOutput struct
struct AggrOutput {
    aggr_state: Box<dyn AggrState>,
    output_samples: AtomicUsize,
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
pub(crate) struct FlushCtx {
    aggregator: Arc<Mutex<Aggregator>>,
    aggr_output: AggrOutput,
    push_func: Option<Box<dyn PushFunc>>,
    pub flush_timestamp: i64,
    pub idx: usize,
    tss: Vec<TimeSeries>,
    labels: Vec<Label>,
    samples: Vec<Sample>,
}

impl FlushCtx {
    pub fn reset(&mut self) {
        self.aggregator.clear_poison(); // ????
        self.aggr_output.output_samples.store(0, Ordering::Relaxed);
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
                self.aggr_output.as_ref().unwrap().output_samples.fetch_add(tss.len() as u64, std::sync::atomic::Ordering::Relaxed);
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
            self.ao.as_ref().unwrap().output_samples.fetch_add(dst.len() as u64, Ordering::Relaxed);
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
        if !self.aggregator.lock().unwrap().keep_metric_names {
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

fn get_flush_ctx(a: Arc<Aggregator>, output: Arc<AggrOutput>, push_func: Box<dyn Fn(Vec<TimeSeries>) + Send + Sync>, flush_timestamp: i64, idx: usize) -> Arc<Mutex<flushCtx>> {
    let mut pool = FLUSH_CTX_POOL.lock().unwrap();
    let ctx = pool.pop().unwrap_or_else(|| Arc::new(Mutex::new(FlushCtx {
        aggregator: a.clone(),
        ao: None,
        push_func: None,
        flush_timestamp: 0,
        idx: 0,
        tss: Vec::new(),
        labels: Vec::new(),
        samples: Vec::new(),
        aggr_output: AggrOutput {},
    })));
    let mut ctx_guard = ctx.lock().unwrap();
    ctx_guard.a = Some(a);
    ctx_guard.ao = Some(output);
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

pub fn new_aggregator(cfg: &StreamingAggregationConfig,
                      path: &str,
                      push_func: Box<dyn PushFunc>,
                      opts: Option<&Options>,
                      alias: &str,
                      aggr_id: i32) -> Result<Arc<Aggregator>, String> {
    if cfg.interval < Duration::from_secs(1) {
        return Err(format!("aggregation interval cannot be smaller than 1s; got {:?}", &cfg.interval));
    }

    let opts = opts.unwrap_or_else(|| Options {
        dedup_interval: Some(Duration::from_secs(0)),
        drop_input_labels: vec![],
        keep_metric_names: false,
        ignore_old_samples: false,
        ignore_first_intervals: 0,
        no_align_flush_to_interval: false,
        flush_on_shutdown: false,
        keep_input: false,
    });

    let dedup_interval = if let Some(interval) = &cfg.dedup_interval {
        interval.clone()
    } else {
        opts.dedup_interval
    };
    if dedup_interval > cfg.interval {
        return Err(format!("dedup_interval={:?} cannot exceed interval={:?}", dedup_interval, cfg.interval));
    }
    if !dedup_interval.is_zero() && cfg.interval.as_millis() % dedup_interval.as_millis() != 0 {
        return Err(format!("interval={:?} must be a multiple of dedup_interval={:?}", cfg.interval, dedup_interval));
    }

    let staleness_interval = if let Some(interval) = &cfg.staleness_interval {
        interval.clone()
    } else {
        let ms = cfg.interval.as_millis() as i64;
        Duration::from_millis((ms * 2) as u64)
    };

    if staleness_interval < cfg.interval {
        return Err(format!("staleness_interval={:?} cannot be smaller than interval={:?}",
                           staleness_interval, &cfg.interval));
    }

    let drop_input_labels = cfg.drop_input_labels.unwrap_or(opts.drop_input_labels);
    let input_relabeling = cfg.input_relabel_configs.clone();
    let output_relabeling = cfg.output_relabel_configs.clone();

    let by = sort_and_remove_duplicates(&cfg.by);
    let without = sort_and_remove_duplicates(&cfg.without);
    if !by.is_empty() && !without.is_empty() {
        return Err(format!("`by: {:?}` and `without: {:?}` lists cannot be set simultaneously", by, without));
    }
    let aggregate_only_by_time = by.is_empty() && without.is_empty();

    let keep_metric_names = cfg.keep_metric_names.unwrap_or(opts.keep_metric_names);
    if keep_metric_names && cfg.outputs.len() != 1 {
        return Err("`outputs` list must contain only a single entry if `keep_metric_names` is set".to_string());
    }

    let ignore_old_samples = cfg.ignore_old_samples.unwrap_or(opts.ignore_old_samples);
    let ignore_first_intervals = cfg.ignore_first_intervals.unwrap_or(opts.ignore_first_intervals);

    let name = if let Some(name) = &cfg.name {
        name.clone()
    } else {
        "none".to_string()
    };

    let metric_labels = format!("name=\"{}\",path=\"{}\",url=\"{}\",position=\"{}\"", name, path, alias, aggr_id);

    if cfg.outputs.is_empty() {
        return Err("`outputs` list must contain at least a single entry".to_string());
    }

    let mut aggr_outputs = Vec::with_capacity(cfg.outputs.len());
    let mut outputs_seen = HashSet::with_capacity(cfg.outputs.len());
    for output in &cfg.outputs {
        let as_ = new_aggr_state(output, &mut outputs_seen, staleness_interval)?;
        aggr_outputs.push(AggrOutput {
            as_: as_,
            output_samples: 0,
        });
    }

    let suffix = format!(":{}_{}_", cfg.interval, if !by.is_empty() { format!("_by_{}", by.join("_")) } else { "".to_string() });

    let aggregator = Arc::new(Aggregator {
        r#match: cfg.r#match.clone(),
        drop_input_labels,
        input_relabeling,
        output_relabeling,
        keep_metric_names,
        ignore_old_samples,
        staleness_interval,
        by,
        without,
        aggregate_only_by_time,
        tick_interval: cfg.interval.as_millis() as i64,
        interval,
        dedup_interval,
        aggr_outputs,
        suffix,
        stop_ch: Arc::new(Mutex::new(false)),
        flush_after: FastHistogram::new(),
        flush_duration: Histogram::new(),
        dedup_flush_duration: Histogram::new(),
        samples_lag: Histogram::new(),
        matched_samples: 0,
        flush_timeouts: 0,
        dedup_flush_timeouts: 0,
        ignored_nan_samples: 0,
        ignored_old_samples: 0,
        min_timestamp: current_time_millis(),
        wg: Arc::new(Mutex::new(Vec::new())),
    });

    let mut aggregator_clone = Arc::clone(&aggregator);
    let handle = thread::spawn(move || {
        aggregator_clone.run_flusher(push_func, !opts.no_align_flush_to_interval, !opts.flush_on_shutdown, opts.ignore_first_intervals);
    });

    aggregator.wg.lock().unwrap().push(handle);

    Ok(aggregator)
}

fn new_aggr_state(output: &str, outputs_seen: &mut HashSet<String>, staleness_interval: Duration) -> Result<Box<dyn AggrState>, String> {
    if outputs_seen.contains_key(output) {
        return Err(format!("`outputs` list contains duplicate aggregation function: {}", output));
    }
    outputs_seen.insert(output.to_string());

    if output.starts_with("quantiles(") {
        if !output.ends_with(")") {
            return Err("missing closing brace for `quantiles()` output".to_string());
        }
        let args_str = &output[9..output.len() - 1];
        if args_str.is_empty() {
            return Err("`quantiles()` must contain at least one phi".to_string());
        }
        let args: Vec<&str> = args_str.split(',').collect();
        let mut phis = Vec::with_capacity(args.len());
        for arg in args {
            let phi = arg.trim().parse::<f64>().map_err(|e| format!("cannot parse phi={}: {}", arg, e))?;
            if phi < 0.0 || phi > 1.0 {
                return Err(format!("phi inside quantiles({}) must be in the range [0..1]; got {}", args_str, phi));
            }
            phis.push(phi);
        }
        if outputs_seen.contains_key("quantiles") {
            return Err("`outputs` list contains duplicated `quantiles()` function".to_string());
        }
        outputs_seen.insert("quantiles".to_string());
        return Ok(Box::new(QuantilesAggrState::new(phis)));
    }

    let output_type = AggregationOutput::from(output);
    match output_type {
        AggregationOutput::Avg => Ok(Box::new(AvgAggrState::new())),
        AggregationOutput::CountSamples => Ok(Box::new(CountSamplesAggrState::new())),
        AggregationOutput::CountSeries => Ok(Box::new(CountSeriesAggrState::new())),
        AggregationOutput::HistogramBucket => Ok(Box::new(HistogramBucketAggrState::new())),
        AggregationOutput::Increase => Ok(Box::new(TotalAggrState::new(true, true))),
        AggregationOutput::IncreasePrometheus => Ok(Box::new(TotalAggrState::new(true, false))),
        AggregationOutput::Last => Ok(Box::new(LastAggrState::new())),
        AggregationOutput::Max => Ok(Box::new(MaxAggrState::new())),
        AggregationOutput::Min => Ok(Box::new(MinAggrState::new())),
        AggregationOutput::RateAvg => Ok(Box::new(RateAggrState::new(true))),
        AggregationOutput::RateSum => Ok(Box::new(RateAggrState::new(false))),
        AggregationOutput::Stddev => Ok(Box::new(StddevAggrState::new())),
        AggregationOutput::Stdvar => Ok(Box::new(StdvarAggrState::new())),
        AggregationOutput::SumSamples => Ok(Box::new(SumSamplesAggrState::new())),
        AggregationOutput::Total => Ok(Box::new(TotalAggrState::new(false, true))),
        AggregationOutput::TotalPrometheus => Ok(Box::new(TotalAggrState::new(false, false))),
        AggregationOutput::UniqueSamples => Ok(Box::new(UniqueSamplesAggrState::new())),
        _ => Err(format!("unsupported output={}; supported values: {}", output, "TODO")),
    }
}

impl Aggregator {
    fn run_flusher1(
        &mut self,
        push_func: Box<dyn PushFunc>,
        align_flush_to_interval: bool,
        skip_incomplete_flush: bool,
        ignore_first_intervals: i32,
    ) {
        let aligned_sleep = |d: Duration| {
            if !align_flush_to_interval {
                return;
            }

            let now = current_time_millis();
            let millis = d.as_millis() as u64;

            let sleep_duration = millis - (now % millis);
            thread::sleep(Duration::from_millis(sleep_duration));
        };

        let ticker_wait = |t: &mut tokio::time::Interval| -> bool {
            tokio::select! {
                _ = self.stop_ch.recv() => {
                    false
                }
                _ = t.tick() => {
                    true
                }
            }
        };

        let mut ignore_first_intervals = ignore_first_intervals;

        if self.dedup_interval.is_zero() {
            aligned_sleep(self.interval);
            let mut t = tokio::time::interval(self.interval);

            if align_flush_to_interval && skip_incomplete_flush {
                self.flush(None, 0);
                ignore_first_intervals -= 1;
            }

            loop {
                if !ticker_wait(&mut t) {
                    break;
                }

                if ignore_first_intervals > 0 {
                    self.flush(None, 0);
                    ignore_first_intervals -= 1;
                } else {
                    self.flush(Some(&push_func), t.tick().unwrap().as_millis() as i64);
                }

                if align_flush_to_interval {
                    t.tick().await;
                }
            }
        } else {
            aligned_sleep(self.dedup_interval);
            let mut t = tokio::time::interval(self.dedup_interval);

            let flush_deadline = current_time_millis() + self.interval;
            let mut is_skipped_first_flush = false;
            loop {
                if !ticker_wait(&mut t) {
                    break;
                }

                self.dedup_flush();

                let now = current_time_millis();
                if now > flush_deadline {
                    if align_flush_to_interval && skip_incomplete_flush && !is_skipped_first_flush {
                        self.flush(None, 0);
                        ignore_first_intervals -= 1;
                        is_skipped_first_flush = true;
                    } else if ignore_first_intervals > 0 {
                        self.flush(None, 0);
                        ignore_first_intervals -= 1;
                    } else {
                        self.flush(Some(&push_func), t.tick().unwrap().as_millis() as i64);
                    }

                    while now > flush_deadline {
                        flush_deadline += self.interval;
                    }
                }

                if align_flush_to_interval {
                    t.tick().await;
                }
            }
        }

        if !skip_incomplete_flush && ignore_first_intervals <= 0 {
            self.dedup_flush();
            self.flush(Some(&push_func), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64);
        }
    }

    fn run_flusher(&mut self,
                   push_func: Box<dyn PushFunc>,
                   align_flush_to_interval: bool,
                   skip_incomplete_flush: bool,
                   ignore_first_intervals: i32
    ) {
        let mut ignore_first_intervals = ignore_first_intervals;
        let now = current_time_millis();
        let mut flush_time_msec = now;
        let tick_interval = Duration::from_millis(self.tick_interval as u64);
        let mut flush_deadline = now;
        let mut dedup_idx = 0;
        let mut flush_idx = 0;
        let mut is_skipped_first_flush = false;

        loop {
            if *self.stop_ch.lock().unwrap() {
                break;
            }

            thread::sleep(tick_interval);

            let now = current_time_millis();
            if self.ignore_old_samples {
                (dedup_idx, flush_idx) = self.get_aggr_idxs(now, flush_deadline);
            }

            if now > flush_deadline {
                if align_flush_to_interval && skip_incomplete_flush && !is_skipped_first_flush {
                    self.flush(None, 0, flush_idx);
                    is_skipped_first_flush = true;
                } else if ignore_first_intervals > 0 {
                    self.flush(None, 0, flush_idx);
                    ignore_first_intervals -= 1;
                } else {
                    self.flush(Some(push_func.clone()), flush_deadline, flush_idx);
                }
                flush_deadline += self.interval.as_millis() as i64;
            }
        }

        if !skip_incomplete_flush && ignore_first_intervals <= 0 {
            self.dedup_flush(flush_deadline, dedup_idx, flush_idx);
            self.flush(Some(push_func.clone()), flush_deadline, flush_idx);
        }
    }

    fn get_aggr_idxs(&self, dedup_time: i64, flush_time: i64) -> (usize, usize) {
        let flush_idx = get_state_idx (dedup_time, flush_time);
        let mut dedup_idx = flush_idx;
        if !self.dedup_interval.is_zero() {
            let millis = self.dedup_interval.as_millis() as i64;
            dedup_idx = get_state_idx(millis, dedupTime.Add(-self.dedup_interval).UnixMilli())
        }
        (dedup_idx, flush_idx)
    }

    fn dedup_flush(&mut self, delete_deadline: i64, dedup_idx: usize, flush_idx: usize) {
        if self.dedup_interval.is_zero() {
            return;
        }

        let start_time = current_time_millis();

        for ao in &self.aggr_outputs {
            ao.as_.push_samples(Vec::new(), delete_deadline, dedup_idx);
        }

        let duration = SystemTime::now().duration_since(start_time).unwrap();
        if duration > self.dedup_interval {
            self.dedup_flush_timeouts += 1;
        }
    }

    fn flush(&mut self, push_func: PushFunc, flush_time_msec: i64, idx: usize) {
        let start_time = SystemTime::now();

        for ao in &self.aggr_outputs {
            let ctx = FlushCtx {
                aggregator: Arc::clone(&self),
                aggr_output: Arc::clone(&ao),
                push_func: push_func.clone(),
                flush_time_msec,
                idx,
                series: Vec::new(),
            };
            ao.as_.flush_state(&ctx);
        }

        let duration = SystemTime::now().duration_since(start_time).unwrap();
        if duration > self.interval {
            self.flush_timeouts += 1;
        }
    }

    fn must_stop(&self) {
        *self.stop_ch.lock().unwrap() = true;
        for handle in self.wg.lock().unwrap().drain(..) {
            handle.join().unwrap();
        }
    }

    fn push(&mut self, tss: &[TimeSeries], match_idxs: &mut [u8]) {
        let now = current_time_millis();
        let delete_deadline = now + if let Some(interval) = &self.staleness_interval {
            interval.as_millis() as i64
        } else {
            0
        };
        let mut samples = vec![Vec::new(); 1024];
        let mut buf = Vec::new();
        let mut labels = Vec::new();
        let mut input_labels = Vec::new();
        let mut output_labels = Vec::new();
        let mut max_lag_msec = 0;
        let mut flush_idx = 0;

        for (idx, ts) in tss.iter().enumerate() {
            if let Some(matcher) = &self.r#match {
                if !matcher.is_match(&ts.labels) {
                    continue;
                }
            }
            match_idxs[idx] = 1;

            if self.drop_input_labels {
                drop_series_labels(&mut labels, &ts.labels, &self.drop_input_labels);
            } else {
                labels = ts.labels.clone();
            }
            if let Some(relabeling) = self.input_relabeling {
                labels = relabeling.apply(labels);
            }
            if labels.is_empty() {
                continue;
            }
            labels.sort_by(|a, b| a.name.cmp(&b.name));

            if !self.aggregate_only_by_time {
                (input_labels, output_labels) = get_input_output_labels(input_labels, output_labels, labels, &self.by, &self.without);
            } else {
                output_labels = labels;
            }

            let buf_len = buf.len();
            buf = compress_labels(buf, input_labels, output_labels);
            let key = String::from_utf8_lossy(&buf[buf_len..]).to_string();

            for s in ts.samples.iter() {
                if s.value.is_nan() {
                    self.ignored_nan_samples += 1;
                    continue;
                }
                if self.ignore_old_samples && s.timestamp < self.min_timestamp {
                    self.ignored_old_samples += 1;
                    continue;
                }
                let lag_msec = now - s.timestamp;
                if lag_msec > max_lag_msec {
                    max_lag_msec = lag_msec;
                }
                if self.ignore_old_samples {
                    flush_idx = (s.timestamp / self.tick_interval) as usize % 1024;
                }
                samples[flush_idx].push(PushSample {
                    key: key.clone(),
                    value: s.value,
                    timestamp: s.timestamp,
                });
            }
        }

        for (idx, s) in samples.iter().enumerate() {
            if !s.is_empty() {
                self.samples_lag.update(max_lag_msec as f64 / 1_000.0);
                self.matched_samples += s.len() as u64;
                self.push_samples(s.clone(), delete_deadline, idx);
            }
        }
    }

    fn push_samples(&self, samples: Vec<PushSample>, delete_deadline: i64, idx: usize) {
        for ao in &self.aggr_outputs {
            ao.as_.push_samples(samples.clone(), delete_deadline, idx);
        }
    }
}

fn compress_labels(dst: &mut Vec<u8>, input_labels: Vec<Label>, output_labels: Vec<Label>) -> Vec<u8> {
    let mut bb = Vec::new();
    bb = compress(bb, input_labels);
    dst.extend_from_slice(&(bb.len() as u64).to_le_bytes());
    dst.extend_from_slice(&bb);
    compress(dst, output_labels)
}

fn decompress_labels(dst: Vec<Label>, key: &str) -> Vec<Label> {
    decompress(dst, key.as_bytes())
}

pub(super) fn get_output_key(key: &str) -> &str {
    let bytes = key.as_bytes();
    let (input_key_len, n_size) = uvarint(bytes);
    &key[n_size + input_key_len..]
}

pub(super) fn get_input_output_key(key: &str) -> (&str, &str) {
    let bytes = key.as_bytes();
    let (input_key_len, n_size) = uvarint(bytes);
    let input_key = &key[n_size..n_size + input_key_len];
    let output_key = &key[n_size + input_key_len..];
    (input_key, output_key)
}

#[inline]
fn get_state_idx(interval: i64, ts: i64) -> usize {
    (ts / interval).abs() as usize % AGGR_STATE_SIZE
}

fn get_input_output_labels(input_labels: Vec<Label>, output_labels: Vec<Label>, labels: Vec<Label>, by: &[String], without: &[String]) -> (Vec<Label>, Vec<Label>) {
    (input_labels, output_labels)
}

fn compress(dst: Vec<u8>, labels: Vec<Label>) -> Vec<u8> {
    dst
}

fn decompress(dst: Vec<Label>, bytes: &[u8]) -> Vec<Label> {
    dst
}
