pub use metricsql_common::prelude::{
    StringMatchHandler
};
pub use metricsql_runtime::types::{Label, Sample, Timestamp, TimestampTrait, MetricName};
pub use metricsql_runtime::{TagFilter};
pub use metricsql_parser::label::{ LabelFilter, LabelFilterOp, Matchers };
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

#[derive(Debug)]
pub struct TaggedSample<T: Copy + Clone + SampleLike> {
    pub(crate) tag: T,
    pub(crate) sample: Sample,
}

impl<T: Copy + Clone + SampleLike> TaggedSample<T> {
    pub fn new(tag: T, sample: Sample) -> Self {
        TaggedSample { tag, sample }
    }
}

impl<T: Copy + Clone + SampleLike> PartialEq for TaggedSample<T> {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag && self.sample == other.sample
    }
}

impl<T: Copy + Clone + SampleLike> Eq for TaggedSample<T> {}

impl<T: Copy + Clone + SampleLike> PartialOrd for TaggedSample<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl<T: Copy + Clone + SampleLike> Ord for TaggedSample<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

