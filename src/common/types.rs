
pub use metricsql_runtime::types::{Label, Sample, Timestamp, TimestampTrait, MetricName};
pub use metricsql_parser::label::{ Matcher, Matchers };
pub type IntMap<K,V> = metricsql_common::hash::IntMap<K, V>;
pub use metricsql_runtime::prelude::query::QueryParams;

pub trait SampleLike: Eq + PartialEq + PartialOrd + Ord {
    fn timestamp(&self) -> Timestamp;
    fn value(&self) -> f64;
}

impl SampleLike for Sample {
    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    fn value(&self) -> f64 {
        self.value
    }
}

