mod avg;
mod stream_aggr;
mod count_samples;
mod max;
mod utils;
mod quantiles;
mod stdvar;
mod rate;
mod dedup;
mod deduplicator;
mod unique_samples;
mod total;
mod count_series;
mod last;
mod min;
mod sum_samples;
mod stddev;
mod histogram_bucket;
mod dedup_tests;
mod fast_histogram;
mod labels_compressor;
mod timeseries;
mod interner;

use std::fmt::Display;
use std::str::FromStr;

const AGGR_STATE_SIZE: usize = 8; // Assuming aggrStateSize is 8 based on the Go code

pub type OutputKey = String;

// PushSample struct
#[derive(Debug, Clone)]
pub(crate) struct PushSample {
    pub key: String,
    pub value: f64,
    pub timestamp: i64,
}

pub struct InternedLabel {
    pub(crate) key: String,
    pub(crate) value: String,
}

#[derive(Debug)]
pub enum AggregationOutput {
    Avg,
    CountSamples,
    CountSeries,
    HistogramBucket,
    Increase,
    IncreasePrometheus,
    Last,
    Max,
    Min,
    Quantiles,
    RateAvg,
    RateSum,
    Stddev,
    Stdvar,
    SumSamples,
    Total,
    TotalPrometheus,
    UniqueSamples,
}

impl AggregationOutput {
    pub fn name(&self) -> &'static str {
        match self {
            AggregationOutput::Avg => "avg",
            AggregationOutput::CountSamples => "count_samples",
            AggregationOutput::CountSeries => "count_series",
            AggregationOutput::HistogramBucket => "histogram_bucket",
            AggregationOutput::Increase => "increase",
            AggregationOutput::IncreasePrometheus => "increase_prometheus",
            AggregationOutput::Last => "last",
            AggregationOutput::Max => "max",
            AggregationOutput::Min => "min",
            AggregationOutput::Quantiles => "quantiles",
            AggregationOutput::RateAvg => "rate_avg",
            AggregationOutput::RateSum => "rate_sum",
            AggregationOutput::Stddev => "stddev",
            AggregationOutput::Stdvar => "stdvar",
            AggregationOutput::SumSamples => "sum_samples",
            AggregationOutput::Total => "total",
            AggregationOutput::TotalPrometheus => "total_prometheus",
            AggregationOutput::UniqueSamples => "unique_samples",
        }
    }
}

impl Display for AggregationOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for AggregationOutput {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "avg" => Ok(AggregationOutput::Avg),
            "count_samples" => Ok(AggregationOutput::CountSamples),
            "count_series" => Ok(AggregationOutput::CountSeries),
            "histogram_bucket" => Ok(AggregationOutput::HistogramBucket),
            "increase" => Ok(AggregationOutput::Increase),
            "increase_prometheus" => Ok(AggregationOutput::IncreasePrometheus),
            "last" => Ok(AggregationOutput::Last),
            "max" => Ok(AggregationOutput::Max),
            "min" => Ok(AggregationOutput::Min),
            "quantiles" => Ok(AggregationOutput::Quantiles),
            "rate_avg" => Ok(AggregationOutput::RateAvg),
            "rate_sum" => Ok(AggregationOutput::RateSum),
            "stddev" => Ok(AggregationOutput::Stddev),
            "stdvar" => Ok(AggregationOutput::Stdvar),
            "sum_samples" => Ok(AggregationOutput::SumSamples),
            "total" => Ok(AggregationOutput::Total),
            "total_prometheus" => Ok(AggregationOutput::TotalPrometheus),
            "unique_samples" => Ok(AggregationOutput::UniqueSamples),
            _ => Err(format!("Unknown aggregation output: {}", s)),
        }
    }
}