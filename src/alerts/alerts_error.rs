use std::fmt::Display;
use get_size::GetSize;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Enum for various alert errors.
#[derive(Debug, Clone, Error, Eq, PartialEq, Serialize, Deserialize)]
#[derive(GetSize)]
pub enum AlertsError {
    #[error("Invalid configuration. {0}")]
    InvalidConfiguration(String),

    #[error("Invalid rules. {0}")]
    InvalidRule(String),

    #[error("Serialization error. {0}")]
    CannotSerialize(String),

    #[error("Cannot deserialize. {0}")]
    CannotDeserialize(String),

    #[error("Duplicate sample. {0}")] // need better error
    DuplicateSample(String),

    #[error("Duplicate series. {0}")] // need better error
    DuplicateSeries(String),
    
    #[error("A rules named \"{0}\" already exists")]
    RuleAlreadyExists(String),

    #[error("Invalid series selector: {0}")]
    InvalidSeriesSelector(String),

    #[error("Failed to expand labels: {0}")]
    FailedToExpandLabels(String),

    #[error("Failed to execute query: {0}")]
    QueryExecutionError(String),

    #[error("Failed to create alert. {0}")]
    FailedToCreateAlert(String),

    #[error("Failed to parse template: {0}")]
    TemplateParseError(String),

    #[error("Error fetching group: {0}")]
    ErrorFetchingGroup(String),

    #[error("Failure executing template: {0}")]
    TemplateExecutionError(ErrorGroup),

    #[error("Failed to execute group rules: {0}")]
    GroupExecutionError(ErrorGroup),
    
    #[error("Failure expanding template: {0}")]
    TemplateExpansionError(String),

    #[error("{0}")]
    Generic(String),

    #[error("Failure restoring rules: {0}")]
    RuleRestoreError(String)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq)]
#[derive(GetSize)]
pub struct ErrorGroup(pub Vec<AlertsError>);

impl ErrorGroup {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &AlertsError> {
        self.0.iter()
    }
}

impl From<Vec<AlertsError>> for ErrorGroup {
    fn from(errors: Vec<AlertsError>) -> Self {
        ErrorGroup(errors)
    }
}

impl Display for ErrorGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.len();
        for (i, error) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}", error)?;
            if i < len - 1 {
                write!(f, "\n, ")?;
            }
        }
        Ok(())
    }
}

impl From<ErrorGroup> for AlertsError {
    fn from(err: ErrorGroup) -> Self {
        AlertsError::GroupExecutionError(err)
    }
}

pub type AlertsResult<T> = Result<T, AlertsError>;