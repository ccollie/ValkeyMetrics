use crate::aggregators::Aggregator;
use crate::common::rounding::RoundingStrategy;
use crate::common::types::{Matchers, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::series::timestamp_range::{TimestampRange, TimestampValue};
use crate::series::ChunkCompression;
use get_size::GetSize;
use metricsql_common::label::Label;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyString};

#[derive(Debug, Default, PartialEq, Deserialize, Serialize, Clone, Copy)]
#[derive(GetSize)]
pub enum DuplicatePolicy {
    /// ignore any newly reported value and reply with an error
    #[default]
    Block,
    /// ignore any newly reported value
    KeepFirst,
    /// overwrite the existing value with the new value
    KeepLast,
    /// only override if the value is lower than the existing value
    Min,
    /// only override if the value is higher than the existing value
    Max,
    /// append the new value to the existing value
    Sum,
}

impl Display for DuplicatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl DuplicatePolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            DuplicatePolicy::Block => "block",
            DuplicatePolicy::KeepFirst => "first",
            DuplicatePolicy::KeepLast => "last",
            DuplicatePolicy::Min => "min",
            DuplicatePolicy::Max => "max",
            DuplicatePolicy::Sum => "sum",
        }
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            DuplicatePolicy::Block => 0,
            DuplicatePolicy::KeepFirst => 1,
            DuplicatePolicy::KeepLast => 2,
            DuplicatePolicy::Min => 4,
            DuplicatePolicy::Max => 8,
            DuplicatePolicy::Sum => 16,
        }
    }

    pub fn duplicate_value(self, ts: Timestamp, old: f64, new: f64) -> TsdbResult<f64> {
        use DuplicatePolicy::*;
        let has_nan = old.is_nan() || new.is_nan();
        if has_nan && self != Block {
            // take the valid sample regardless of policy
            let value = if new.is_nan() { old } else { new };
            return Ok(value);
        }
        Ok(match self {
            Block => {
                // todo: format ts as iso-8601 or rfc3339
                let msg = format!("{new} @ {ts}");
                return Err(TsdbError::DuplicateSample(msg));
            }
            KeepFirst => old,
            KeepLast => new,
            Min => old.min(new),
            Max => old.max(new),
            Sum => old + new,
        })
    }
}

impl FromStr for DuplicatePolicy {
    type Err = ValkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use DuplicatePolicy::*;

        match s.to_ascii_lowercase().as_str() {
            "block" => Ok(Block),
            "first" => Ok(KeepFirst),
            "keepfirst" | "keep_first" => Ok(KeepFirst),
            "last" => Ok(KeepLast),
            "keeplast" | "keep_last" => Ok(KeepLast),
            "min" => Ok(Min),
            "max" => Ok(Max),
            "sum" => Ok(Sum),
            _ => Err(ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY)),
        }
    }
}

impl TryFrom<&str> for DuplicatePolicy {
    type Error = ValkeyError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        DuplicatePolicy::from_str(s)
    }
}

impl TryFrom<String> for DuplicatePolicy {
    type Error = ValkeyError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        DuplicatePolicy::from_str(&s)
    }
}

impl TryFrom<u8> for DuplicatePolicy {
    type Error = TsdbError;

    fn try_from(n: u8) -> Result<Self, Self::Error> {
        match n {
            0 => Ok(DuplicatePolicy::Block),
            1 => Ok(DuplicatePolicy::KeepFirst),
            2 => Ok(DuplicatePolicy::KeepLast),
            4 => Ok(DuplicatePolicy::Min),
            8 => Ok(DuplicatePolicy::Max),
            16 => Ok(DuplicatePolicy::Sum),
            _ => Err(TsdbError::General(format!("invalid duplicate policy: {n}"))),
        }
    }
}

pub struct MetadataFunctionArgs {
    pub start: Timestamp,
    pub end: Timestamp,
    pub matchers: Vec<Matchers>,
    pub limit: Option<usize>,
}

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct ValueFilter {
    pub min: f64,
    pub max: f64,
}

impl ValueFilter {
    pub(crate) fn new(min: f64, max: f64) -> ValkeyResult<Self> {
        if min > max {
            return Err(ValkeyError::Str("ERR invalid range"));
        }
        Ok(Self { min, max })
    }
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct RangeFilter {
    pub value: Option<ValueFilter>,
    pub timestamps: Option<Vec<Timestamp>>,
}

impl RangeFilter {
    pub fn filter(&self, timestamp: Timestamp, value: f64) -> bool {
        if let Some(value_filter) = &self.value {
            if value < value_filter.min || value > value_filter.max {
                return false;
            }
        }
        if let Some(timestamps) = &self.timestamps {
            if !timestamps.contains(&timestamp) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum RangeAlignment {
    #[default]
    Default,
    Start,
    End,
    Timestamp(Timestamp),
}

impl RangeAlignment {
    pub fn get_aligned_timestamp(&self, start: Timestamp, end: Timestamp) -> Timestamp {
        match self {
            RangeAlignment::Default => 0,
            RangeAlignment::Start => start,
            RangeAlignment::End => end,
            RangeAlignment::Timestamp(ts) => *ts,
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketTimestamp {
    #[default]
    Start,
    End,
    Mid
}

impl BucketTimestamp {
    pub fn calculate(&self, ts: Timestamp, time_delta: i64) -> Timestamp {
        match self {
            Self::Start => ts,
            Self::Mid => ts + time_delta / 2,
            Self::End => ts + time_delta,
        }
    }

}
impl TryFrom<&str> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() == 1 {
            let c = value.chars().next().unwrap();
            match c {
                '-' => return Ok(BucketTimestamp::Start),
                '+' => return Ok(BucketTimestamp::End),
                _ => {}
            }
        }
        match value {
            value if value.eq_ignore_ascii_case("start") => return Ok(BucketTimestamp::Start),
            value if value.eq_ignore_ascii_case("end") => return Ok(BucketTimestamp::End),
            value if value.eq_ignore_ascii_case("mid") => return Ok(BucketTimestamp::Mid),
            _ => {}
        }
        Err(ValkeyError::Str("TSDB: invalid BUCKETTIMESTAMP parameter"))
    }
}

impl TryFrom<&ValkeyString> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        value.to_string_lossy().as_str().try_into()
    }
}

#[derive(Debug, Clone)]
pub struct AggregationOptions {
    pub aggregator: Aggregator,
    pub bucket_duration: Duration,
    pub timestamp_output: BucketTimestamp,
    pub alignment: RangeAlignment,
    pub time_delta: i64,
    pub empty: bool
}

#[derive(Debug, Clone)]
pub struct RangeGroupingOptions {
    pub(crate) aggregator: Aggregator,
    pub(crate) group_label: String,
}

#[derive(Debug, Default, Clone)]
pub struct RangeOptions {
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub aggregation: Option<AggregationOptions>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
    pub series_selector: Matchers,
    pub with_labels: bool,
    pub selected_labels: Vec<String>,
    pub grouping: Option<RangeGroupingOptions>,
}

impl RangeOptions {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self {
            date_range: TimestampRange {
                start: TimestampValue::Value(start),
                end: TimestampValue::Value(end),
            },
            ..Default::default()
        }
    }

    pub fn is_aggregation(&self) -> bool {
        self.aggregation.is_some()
    }
}

#[derive(Debug, Default, Clone)]
pub struct TimeSeriesOptions {
    pub chunk_compression: Option<ChunkCompression>,
    pub chunk_size: Option<usize>,
    pub retention: Option<Duration>,
    pub duplicate_policy: Option<DuplicatePolicy>,
    pub dedupe_interval: Option<Duration>,
    pub labels: Vec<Label>,
    pub significant_digits: Option<u8>,
    pub rounding: Option<RoundingStrategy>
}

impl TimeSeriesOptions {
    pub fn chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = Some(chunk_size);
    }

    pub fn retention(&mut self, retention: Duration) {
        self.retention = Some(retention);
    }

    pub fn duplicate_policy(&mut self, duplicate_policy: DuplicatePolicy) {
        self.duplicate_policy = Some(duplicate_policy);
    }
}

#[cfg(test)]
mod tests {
    use super::DuplicatePolicy;
    use crate::error::TsdbError;
    use std::str::FromStr;

    #[test]
    fn test_duplicate_policy_parse() {
        assert!(matches!(DuplicatePolicy::from_str("block"), Ok(DuplicatePolicy::Block)));
        assert!(matches!(DuplicatePolicy::from_str("last"), Ok(DuplicatePolicy::KeepLast)));
        assert!(matches!(DuplicatePolicy::from_str("keepLast"), Ok(DuplicatePolicy::KeepLast)));
        assert!(matches!(DuplicatePolicy::from_str("first"), Ok(DuplicatePolicy::KeepFirst)));
        assert!(matches!(DuplicatePolicy::from_str("KeEpFIRst"), Ok(DuplicatePolicy::KeepFirst)));
        assert!(matches!(DuplicatePolicy::from_str("min"), Ok(DuplicatePolicy::Min)));
        assert!(matches!(DuplicatePolicy::from_str("max"), Ok(DuplicatePolicy::Max)));
        assert!(matches!(DuplicatePolicy::from_str("sum"), Ok(DuplicatePolicy::Sum)));
    }

    #[test]
    fn test_duplicate_policy_handle_duplicate() {
        let dp = DuplicatePolicy::Block;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert!(matches!(dp.duplicate_value(ts, old, new), Err(TsdbError::DuplicateSample(_))));

        let dp = DuplicatePolicy::KeepFirst;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), old);

        let dp = DuplicatePolicy::KeepLast;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), new);

        let dp = DuplicatePolicy::Min;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), old);

        let dp = DuplicatePolicy::Max;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), new);

        let dp = DuplicatePolicy::Sum;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), old + new);
    }

    #[test]
    fn test_duplicate_policy_handle_nan() {
        use DuplicatePolicy::*;

        let dp = Block;
        let ts = 0;
        let old = 1.0;
        let new = f64::NAN;
        assert!(matches!(dp.duplicate_value(ts, old, new), Err(TsdbError::DuplicateSample(_))));

        let policies = [KeepFirst, KeepLast, Min, Max, Sum];
        for policy in policies {
            assert_eq!(policy.duplicate_value(ts, 10.0, f64::NAN).unwrap(), 10.0);
            assert_eq!(policy.duplicate_value(ts, f64::NAN, 8.0).unwrap(), 8.0);
        }
    }
}