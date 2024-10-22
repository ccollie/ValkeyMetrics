use std::fmt::Display;
use std::time::Duration;
use joinkit::EitherOrBoth;
use metricsql_common::humanize::humanize_duration;
use crate::common::types::{Sample, Timestamp};
use crate::series::types::{AggregationOptions, ValueFilter};

mod join_inner_iter;
mod join_full_iter;
mod join_right_exclusive_iter;
mod join_left_exclusive_iter;
mod join_left_iter;
mod join_right_iter;
mod join_asof_iter;
mod join_iter;
pub mod asof;

pub use join_iter::*;
use crate::join::asof::AsOfJoinStrategy;
use crate::series::TimestampRange;
use crate::series::join_reducer::JoinReducer;

#[derive(Clone, PartialEq, Debug)]
pub struct JoinValue {
    pub timestamp: Timestamp,
    pub other_timestamp: Option<Timestamp>,
    pub value: EitherOrBoth<f64, f64>,
}

impl JoinValue {
    pub fn new(timestamp: Timestamp, left: Option<f64>, right: Option<f64>) -> Self {
        JoinValue {
            timestamp,
            other_timestamp: None,
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
            other_timestamp: None,
            value: EitherOrBoth::Left(value)
        }
    }
    pub fn right(timestamp: Timestamp, value: f64) -> Self {
        JoinValue {
            other_timestamp: None,
            timestamp,
            value: EitherOrBoth::Right(value)
        }
    }

    pub fn both(timestamp: Timestamp, l: f64, r: f64) -> Self {
        JoinValue {
            timestamp,
            other_timestamp: None,
            value: EitherOrBoth::Both(l, r)
        }
    }
}

impl From<&EitherOrBoth<&Sample, &Sample>> for JoinValue {
    fn from(value: &EitherOrBoth<&Sample, &Sample>) -> Self {
        match value {
            EitherOrBoth::Both(l, r) => {
                let mut value = Self::both(l.timestamp, l.value, r.value);
                value.other_timestamp = Some(r.timestamp);
                value
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

#[derive(Debug, Default, Copy, Clone)]
pub enum JoinType {
    Left(bool),
    Right(bool),
    #[default]
    Inner,
    Full,
    AsOf(AsOfJoinStrategy, Duration),
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
                    AsOfJoinStrategy::Next => write!(f, " NEXT")?,
                    AsOfJoinStrategy::Prior => write!(f, " PRIOR")?,
                }
                if !tolerance.is_zero() {
                    write!(f, " TOLERANCE {}", humanize_duration(tolerance))?;
                }
            }
        }
        Ok(())
    }
}


#[derive(Debug, Default)]
pub struct JoinOptions {
    pub join_type: JoinType,
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
    pub transform_op: Option<JoinReducer>,
    pub aggregation: Option<AggregationOptions>,
}


pub(crate) fn convert_join_item(item: EitherOrBoth<&Sample, &Sample>) -> JoinValue {
    match item {
        EitherOrBoth::Both(l, r) => JoinValue::both(l.timestamp, l.value, r.value),
        EitherOrBoth::Left(l) => JoinValue::left(l.timestamp, l.value),
        EitherOrBoth::Right(r) => JoinValue::right(r.timestamp, r.value),
    }
}