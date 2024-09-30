use crate::aggregators::Aggregator;
use crate::common::types::{Sample, Timestamp};
use crate::module::arg_parse::parse_timestamp;
use crate::storage::time_series::TimeSeries;
use crate::storage::MAX_TIMESTAMP;
use metricsql_parser::prelude::Matchers;
use metricsql_runtime::types::TimestampTrait;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use joinkit::EitherOrBoth;
use metricsql_common::humanize::humanize_duration;
use metricsql_parser::ast::Operator;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyString};

#[derive(Clone, Default, Debug, PartialEq, Copy)]
pub enum TimestampRangeValue {
    Earliest,
    Latest,
    #[default]
    Now,
    Value(Timestamp),
}

impl TimestampRangeValue {
    pub fn to_timestamp(&self) -> Timestamp {
        use TimestampRangeValue::*;
        match self {
            Earliest => 0,
            Latest => MAX_TIMESTAMP,
            Now => Timestamp::now(),
            Value(ts) => *ts,
        }
    }

    pub fn to_series_timestamp(&self, series: &TimeSeries) -> Timestamp {
        use TimestampRangeValue::*;
        match self {
            Earliest => series.first_timestamp,
            Latest => series.last_timestamp,
            Now => Timestamp::now(), // todo: use valkey server value
            Value(ts) => *ts,
        }
    }
}

impl TryFrom<&str> for TimestampRangeValue {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        use TimestampRangeValue::*;
        match value {
            "-" => Ok(Earliest),
            "+" => Ok(Latest),
            "*" => Ok(Now),
            _ => {
                let ts =
                    parse_timestamp(value).map_err(|_| ValkeyError::Str("invalid timestamp"))?;
                if ts < 0 {
                    return Err(ValkeyError::Str(
                        "TSDB: invalid timestamp, must be a non-negative integer",
                    ));
                }
                Ok(TimestampRangeValue::Value(ts))
            }
        }
    }
}

impl TryFrom<&ValkeyString> for TimestampRangeValue {
    type Error = ValkeyError;

    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        use TimestampRangeValue::*;
        if value.len() == 1 {
            let bytes = value.as_slice();
            match bytes[0] {
                b'-' => return Ok(Earliest),
                b'+' => return Ok(Latest),
                b'*' => return Ok(Now),
                _ => {}
            }
        }
        if let Ok(int_val) = value.parse_integer() {
            if int_val < 0 {
                return Err(ValkeyError::Str(
                    "TSDB: invalid timestamp, must be a non-negative integer",
                ));
            }
            Ok(Value(int_val))
        } else {
            let date_str = value.to_string_lossy();
            let ts =
                parse_timestamp(&date_str).map_err(|_| ValkeyError::Str("invalid timestamp"))?;
            Ok(TimestampRangeValue::Value(ts))
        }
    }
}

impl From<Timestamp> for TimestampRangeValue {
    fn from(ts: Timestamp) -> Self {
        TimestampRangeValue::Value(ts)
    }
}

impl Display for TimestampRangeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TimestampRangeValue::*;
        match self {
            Earliest => write!(f, "-"),
            Latest => write!(f, "+"),
            Value(ts) => write!(f, "{}", ts),
            Now => write!(f, "*"),
        }
    }
}

impl PartialOrd for TimestampRangeValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use TimestampRangeValue::*;

        match (self, other) {
            (Now, Now) => Some(Ordering::Equal),
            (Earliest, Earliest) => Some(Ordering::Equal),
            (Latest, Latest) => Some(Ordering::Equal),
            (Value(a), Value(b)) => a.partial_cmp(b),
            (Now, Value(v)) => {
                let now = Timestamp::now();
                now.partial_cmp(v)
            }
            (Value(v), Now) => {
                let now = Timestamp::now();
                v.partial_cmp(&now)
            }
            (Earliest, _) => Some(Ordering::Less),
            (_, Earliest) => Some(Ordering::Greater),
            (Latest, _) => Some(Ordering::Greater),
            (_, Latest) => Some(Ordering::Less),
        }
    }
}

// todo: better naming
#[derive(Clone, Debug, PartialEq, Copy)]
pub struct TimestampRange {
    pub start: TimestampRangeValue,
    pub end: TimestampRangeValue,
}

impl TimestampRange {
    pub fn new(start: TimestampRangeValue, end: TimestampRangeValue) -> ValkeyResult<Self> {
        if start > end {
            return Err(ValkeyError::Str("invalid timestamp range: start > end"));
        }
        Ok(TimestampRange { start, end })
    }

    pub fn get_series_range(&self, series: &TimeSeries, check_retention: bool) -> (Timestamp, Timestamp) {
        // In case a retention is set shouldn't return chunks older than the retention
        let mut start_timestamp = self.start.to_series_timestamp(series);
        let end_timestamp = self.end.to_series_timestamp(series);
        if check_retention && !series.retention.is_zero() {
            // todo: check for i64 overflow
            let retention_ms = series.retention.as_millis() as i64;
            let earliest = series.last_timestamp - retention_ms;
            start_timestamp = start_timestamp.max(earliest);
        }
        (start_timestamp, end_timestamp)
    }
}

impl Display for TimestampRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} to {}", self.start, self.end)
    }
}

impl Default for TimestampRange {
    fn default() -> Self {
        Self {
            start: TimestampRangeValue::Earliest,
            end: TimestampRangeValue::Latest,
        }
    }
}


pub struct MetadataFunctionArgs {
    pub label_name: Option<String>,
    pub start: Timestamp,
    pub end: Timestamp,
    pub matchers: Vec<Matchers>,
    pub limit: Option<usize>,
}

pub(crate) fn normalize_range_timestamps(
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> (TimestampRangeValue, TimestampRangeValue) {
    use TimestampRangeValue::*;
    match (start, end) {
        (Some(start), Some(end)) if start > end => (end.into(), start.into()),
        (Some(start), Some(end)) => (start.into(), end.into()),
        (Some(start), None) => (
            TimestampRangeValue::Value(start),
            Latest,
        ),
        (None, Some(end)) => (Earliest, end.into()),
        (None, None) => (Earliest, Latest),
    }
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
    pub fn new(value: Option<ValueFilter>, timestamps: Option<Vec<Timestamp>>) -> Self {
        Self { value, timestamps }
    }

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

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketTimestamp {
    #[default]
    Start,
    End,
    Mid
}

impl BucketTimestamp {
    pub fn calculate(&self, ts: crate::common::types::Timestamp, time_delta: i64) -> crate::common::types::Timestamp {
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
    pub alignment: Option<RangeAlignment>,
    pub with_labels: bool,
    pub selected_labels: BTreeSet<String>,
    pub grouping: Option<RangeGroupingOptions>,
    pub latest: bool
}

impl RangeOptions {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self {
            date_range: TimestampRange {
                start: TimestampRangeValue::Value(start),
                end: TimestampRangeValue::Value(end),
            },
            ..Default::default()
        }
    }

    pub fn is_aggregation(&self) -> bool {
        self.aggregation.is_some()
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub enum JoinType {
    Left(bool),
    Right(bool),
    #[default]
    Inner,
    Full,
    AsOf(JoinAsOfDirection, Duration),
}

impl JoinType {
    pub fn is_asof(&self) -> bool {
        matches!(self, JoinType::AsOf(..))
    }

    pub fn is_exclusive(&self) -> bool {
        matches!(self, JoinType::Left(..) | JoinType::Right(..))
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Left(exclusive) => {
                write!(f, "LEFT OUTER JOIN")?;
                if *exclusive {
                    write!(f, " EXCLUSIVE")?;
                }
            }
            JoinType::Right(exclusive) => {
                write!(f, "RIGHT OUTER JOIN")?;
                if *exclusive {
                    write!(f, " EXCLUSIVE")?;
                }
            }
            JoinType::Inner => {
                write!(f, "INNER JOIN")?;
            }
            JoinType::Full => {
                write!(f, "FULL JOIN")?;
            }
            JoinType::AsOf(dir, tolerance) => {
                write!(f, "ASOF JOIN")?;
                match dir {
                    JoinAsOfDirection::Forward => write!(f, " FORWARD")?,
                    JoinAsOfDirection::Backward => write!(f, " BACKWARD")?,
                }
                if !tolerance.is_zero() {
                    write!(f, " TOLERANCE {}", humanize_duration(tolerance))?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub enum JoinAsOfDirection {
    #[default]
    Forward,
    Backward,
}

impl Display for JoinAsOfDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinAsOfDirection::Forward => write!(f, "Forward"),
            JoinAsOfDirection::Backward => write!(f, "Backward"),
        }
    }
}
impl FromStr for JoinAsOfDirection {
    type Err = ValkeyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("forward") => Ok(JoinAsOfDirection::Forward),
            s if s.eq_ignore_ascii_case("backward") => Ok(JoinAsOfDirection::Backward),
            _ => Err(ValkeyError::Str("invalid join direction")),
        }
    }
}

impl TryFrom<&str> for JoinAsOfDirection {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let direction = value.to_lowercase();
        match direction.as_str() {
            "forward" => Ok(JoinAsOfDirection::Forward),
            "backward" => Ok(JoinAsOfDirection::Backward),
            _ => Err(ValkeyError::Str("invalid join direction")),
        }
    }
}

#[derive(Debug, Default)]
pub struct JoinOptions {
    pub join_type: JoinType,
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
    pub transform_op: Option<Operator>
}

#[derive(Clone, PartialEq, Debug)]
pub struct JoinValue {
    pub timestamp: Timestamp,
    pub value: EitherOrBoth<f64, f64>,
}

impl JoinValue {
    pub fn new(timestamp: Timestamp, left: Option<f64>, right: Option<f64>) -> Self {
        JoinValue {
            timestamp,
            value: match (&left, &right) {
                (Some(l), Some(r)) => EitherOrBoth::Both(*l, *r),
                (Some(l), None) => EitherOrBoth::Left(*l),
                (None, Some(r)) => EitherOrBoth::Right(*r),
                (None, None) => unreachable!(),
            }
        }
    }

    pub fn left(timestamp: Timestamp, value: f64) -> Self {
        JoinValue {
            timestamp,
            value: EitherOrBoth::Left(value)
        }
    }
    pub fn right(timestamp: Timestamp, value: f64) -> Self {
        JoinValue {
            timestamp,
            value: EitherOrBoth::Right(value)
        }
    }

    pub fn both(timestamp: Timestamp, l: f64, r: f64) -> Self {
        JoinValue {
            timestamp,
            value: EitherOrBoth::Both(l, r)
        }
    }
}

impl From<&EitherOrBoth<&Sample, &Sample>> for JoinValue {
    fn from(value: &EitherOrBoth<&Sample, &Sample>) -> Self {
        match value {
            EitherOrBoth::Both(l, r) => {
                Self::both(l.timestamp, l.value, r.value)
            }
            EitherOrBoth::Left(l) => Self::left(l.timestamp, l.value),
            EitherOrBoth::Right(r) => Self::right(r.timestamp, r.value)
        }
    }
}

impl From<EitherOrBoth<&Sample, &Sample>> for JoinValue {
    fn from(value: EitherOrBoth<&Sample, &Sample>) -> Self {
        (&value).into()
    }
}
