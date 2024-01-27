use std::str::FromStr;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Enum for various alert errors.
#[derive(Debug, Clone, Error, Eq, PartialEq, Serialize, Deserialize)]
pub enum AlertsError {
    #[error("Invalid configuration. {0}")]
    InvalidConfiguration(String),

    #[error("Invalid rule. {0}")]
    InvalidRule(String),

    #[error("Serialization error. {0}")]
    CannotSerialize(String),

    #[error("Cannot deserialize. {0}")]
    CannotDeserialize(String),

    #[error("Duplicate sample. {0}")] // need better error
    DuplicateSample(String),

    #[error("Duplicate series. {0}")] // need better error
    DuplicateSeries(String),

    #[error("Invalid series selector: {0}")]
    InvalidSeriesSelector(String),

    #[error("Invalid timestamp: {0}")]
    MaxActivePendingExceeded(String),

    #[error("Failed to expand labels: {0}")]
    FailedToExpandLabels(String),

    #[error("Failed to execute query: {0}")]
    QueryExecutionError(String),

    #[error("Failed to create alert. {0}")]
    FailedToCreateAlert(String),

    #[error("Failed to template: {0}")]
    TemplateParseError(String),

    #[error("Failure executing template: {0}")]
    TemplateExecutionError(ErrorGroup),

    #[error("{0}")]
    Generic(String),
    
    #[error("Failure restoring rule: {0}")]
    RuleRestoreError(String)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorGroup(pub Vec<String>);

impl ErrorGroup {
    pub fn new() -> Self {
        ErrorGroup(Vec::new())
    }

    pub fn push(&mut self, err: String) {
        self.0.push(err);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromStr for ErrorGroup {
    type Err = AlertsError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ErrorGroup(vec![s.to_string()]))
    }
}

impl From<ErrorGroup> for AlertsError {
    fn from(err: ErrorGroup) -> Self {
        AlertsError::Generic(err.0.join(", "))
    }
}

pub struct MaxActivePendingExceeded {
    pub max_active: usize,
    pub limit: usize,
}

pub type AlertsResult<T> = Result<T, AlertsError>;