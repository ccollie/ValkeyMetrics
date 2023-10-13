use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use crate::common::types::Label;
use crate::rules::alerts::{AlertsResult, DataSourceType};
use crate::ts::Timestamp;

/// Querier trait wraps Query and query_range methods
pub trait Querier {
    /// query executes instant request with the given query at the given ts.
    /// It returns list of Metric in response
    fn query(&self, query: &str, ts: Timestamp) -> AlertsResult<QueryResult>;
    /// query_range executes range request with the given query on the given time range.
    /// It returns list of Metric in response and error if any.
    /// query_range should stop once ctx is cancelled.
    fn query_range(&self, query: &str, from: Timestamp, to: Timestamp) -> AlertsResult<QueryResult>;
}

/// Result represents expected response from the provider
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResult {
    /// Data contains list of received Metric
    pub(crate) data: Vec<Metric>,
    /// SeriesFetched contains amount of time series processed by provider during query evaluation.
    /// If nil, then this feature is not supported by the provider.
    /// SeriesFetched is supported by VictoriaMetrics since v1.90.
    pub(crate) series_fetched: usize
}

/// QuerierBuilder builds Querier with given params.
pub trait QuerierBuilder {
    /// build_with_params creates a new Querier object with the given params
    fn build_with_params(&self, params: QuerierParams) -> Box<dyn Querier>;
}

/// QuerierParams params for Querier.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QuerierParams {
    pub data_source_type: DataSourceType,
    pub evaluation_interval: Duration,
    pub eval_offset: Duration,
    pub query_params: AHashMap<String, String>,
    pub headers: AHashMap<String, String>,
    pub debug: bool
}

/// Metric is the basic entity which should be returned by provider
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    pub key: String,
    pub(crate) labels: Vec<Label>,
    pub(crate) timestamps: Vec<i64>,
    pub(crate) values: Vec<f64>
}

// teemporary. Please remove
pub type DatasourceMetric = Metric;

static EMPTY_STRING: &str = "";

impl Metric {
    pub fn new(labels: Vec<Label>, timestamps: Vec<i64>, values: Vec<f64>) -> Metric {
        Metric {
            key: "".to_string(),
            labels,
            timestamps,
            values
        }
    }

    /// SetLabel adds or updates existing one label by the given key and label
    pub fn set_label(&mut self, key: &str, value: &str) {
        for l in self.labels.iter_mut() {
            if l.name == key {
                l.value = value.to_string();
                return;
            }
        }
        self.add_label(key, value);
    }

    /// del_label deletes the given label from the label set
   pub fn del_label(&mut self, key: &str) {
        self.labels.retain(|l| l.name != key);
    }

    /// Label returns the given label value.
    /// If label is missing empty string will be returned
    pub fn label(&self, key: &str) -> &str {
        for l in self.labels.iter() {
            if l.name == key {
                return &l.value;
            }
        }
        EMPTY_STRING
    }

    /// set_labels sets the given map as Metric labels
   pub fn set_labels(&mut self, ls: HashMap<String, String>) {
        self.labels.clear();
        self.labels.reserve(ls.len());
        for (k, v) in ls {
            self.labels.push(Label{
                name: k.to_string(),
                value: v.to_string()
            })
        }
    }

    /// add_label adds the given label to the label set
   pub fn add_label(&mut self, key: &str, value: &str) {
        self.labels.push(Label{
            name: key.to_string(),
            value: value.to_string()
        })
    }
}

