use crate::alerts::{AlertDatasource, AlertsResult};
use crate::common::types::Timestamp;
use crate::query::{InstantQueryResult, RangeQueryResult};
use ahash::AHashMap;
use std::sync::Arc;
use std::time::Duration;

/// Querier trait wraps query and query_range methods
pub trait Querier {
    /// executes instant request with the given query at the given ts.
    /// It returns list of Metric in response
    fn query(&self, query: &str, ts: Timestamp) -> AlertsResult<InstantResult>;
    /// `query_range` executes range request with the given query on the given time range.
    /// It returns list of Metric in response and error if any.
    fn query_range(&self, query: &str, from: Timestamp, to: Timestamp) -> AlertsResult<RangeResult>;
}

pub type QuerierRef = Arc<dyn Querier>;

#[derive(Debug, Default)]
pub struct InstantResult(pub Vec<InstantQueryResult>);

impl InstantResult {
    pub fn push(&mut self, result: InstantQueryResult) {
        self.0.push(result);
    }
    pub fn into_iter(self) -> impl Iterator<Item = InstantQueryResult> {
        self.0.into_iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &InstantQueryResult> {
        self.0.iter()
    }

    pub fn remove(&mut self, index: usize) -> InstantQueryResult {
        self.0.remove(index)
    }

    pub fn into_vec(self) -> Vec<InstantQueryResult> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Result represents expected response from the provider
#[derive(Debug, Default, Clone)]
pub struct RangeResult {
    /// Data contains list of received Metric
    pub data: Vec<RangeQueryResult>,
}

impl RangeResult {
    /// new creates a new QueryResult with given data
    pub fn new(data: Vec<RangeQueryResult>) -> RangeResult {
        RangeResult {
            data,
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}


/// QuerierBuilder builds Querier with given params.
pub trait QuerierBuilder {
    /// build_with_params creates a new Querier object with the given params
    fn build_with_params(&self, params: QuerierParams) -> AlertDatasource;
}

pub type QuerierBuilderRef = Arc<dyn QuerierBuilder>;

/// QuerierParams params for Querier.
#[derive(Debug, Clone, PartialEq)]
pub struct QuerierParams {
    pub evaluation_interval: Duration,
    pub eval_offset: Duration,
    pub query_params: AHashMap<String, String>,
    pub debug: bool
}