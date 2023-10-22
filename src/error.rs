use thiserror::Error;

#[derive(Debug, Error, Eq, PartialEq)]
/// Enum for various errors in Tsldb.
pub enum TsdbError {
  #[error("Invalid size. Expected {0}, Received {1}.")]
  InvalidSize(usize, usize),

  #[error("Already at full capacity. Max capacity {0}.")]
  CapacityFull(usize),

  #[error("Time series block is empty - cannot be compressed.")]
  EmptyTimeSeriesBlock(),

  #[error("Cannot decode time series. {0}")]
  CannotDecodeTimeSeries(String),

  #[error("Invalid configuration. {0}")]
  InvalidConfiguration(String),

  #[error("Encoding error. {0}")]
  EncodingError(String),

  #[error("Serialization error. {0}")]
  CannotSerialize(String),

  #[error("Cannot deserialize. {0}")]
  CannotDeserialize(String),

  #[error("Cannot decompress. {0}")]
  DecompressionFailed(String),

  #[error("Duplicate sample. {0}")] // need better error
  DuplicateSample(String),

  #[error("Invalid compressed method. {0}")]
  InvalidCompression(String),

  #[error("Invalid timestamp. {0}")]
  InvalidTimestamp(String),

  #[error("Invalid duration. {0}")]
  InvalidTDuration(String),

  #[error("Invalid number. {0}")]
  InvalidNumber(String),

  #[error("Invalid series selector. {0}")]
  InvalidSeriesSelector(String),

  #[error("Sample timestamp is older than retention")]
  SampleTooOld,
}

pub type TsdbResult<T> = Result<T, TsdbError>;