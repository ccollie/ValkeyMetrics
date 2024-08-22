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

use std::fmt::Display;

const AGGR_STATE_SIZE: usize = 8; // Assuming aggrStateSize is 8 based on the Go code

pub type OutputKey = String;

// PushSample struct
#[derive(Debug, Clone)]
pub(crate) struct PushSample {
    pub key: String,
    pub value: f64,
    pub timestamp: i64,
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
    Quantiles(Vec<f64>),
    RateAvg,
    RateSum,
    Stddev,
    Stdvar,
    SumSamples,
    Total,
    TotalPrometheus,
    UniqueSamples,
}

impl Display for AggregationOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregationOutput::Avg => write!(f, "avg"),
            AggregationOutput::CountSamples => write!(f, "count_samples"),
            AggregationOutput::CountSeries => write!(f, "count_series"),
            AggregationOutput::HistogramBucket => write!(f, "histogram_bucket"),
            AggregationOutput::Increase => write!(f, "increase"),
            AggregationOutput::IncreasePrometheus => write!(f, "increase_prometheus"),
            AggregationOutput::Last => write!(f, "last"),
            AggregationOutput::Max => write!(f, "max"),
            AggregationOutput::Min => write!(f, "min"),
            AggregationOutput::Quantiles(phis) => {
                write!(f, "quantiles(")?;
                for (i, phi) in phis.iter().enumerate() {
                    write!(f, "{}", phi)?;
                    if i < phis.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
            AggregationOutput::RateAvg => write!(f, "rate_avg"),
            AggregationOutput::RateSum => write!(f, "rate_sum"),
            AggregationOutput::Stddev => write!(f, "stddev"),
            AggregationOutput::Stdvar => write!(f, "stdvar"),
            AggregationOutput::SumSamples => write!(f, "sum_samples"),
            AggregationOutput::Total => write!(f, "total"),
            AggregationOutput::TotalPrometheus => write!(f, "total_prometheus"),
            AggregationOutput::UniqueSamples => write!(f, "unique_samples"),
        }
    }
}