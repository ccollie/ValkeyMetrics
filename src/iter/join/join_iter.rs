use crate::common::types::Sample;
use crate::iter::join::join_asof_iter::JoinAsOfIter;
use crate::iter::join::join_full_iter::JoinFullIter;
use crate::iter::join::join_inner_iter::JoinInnerIter;
use crate::iter::join::join_left_exclusive_iter::JoinLeftExclusiveIter;
use crate::iter::join::join_left_iter::JoinLeftIter;
use crate::iter::join::join_right_exclusive_iter::JoinRightExclusiveIter;
use crate::iter::join::join_right_iter::JoinRightIter;
use crate::module::types::{JoinType, JoinValue};
use joinkit::EitherOrBoth;
use metricsql_parser::prelude::BinopFunc;


pub enum JoinIterator<'a> {
    Left(JoinLeftIter<'a>),
    LeftExclusive(JoinLeftExclusiveIter<'a>),
    Right(JoinRightIter<'a>),
    RightExclusive(JoinRightExclusiveIter<'a>),
    Inner(JoinInnerIter<'a>),
    Full(JoinFullIter<'a>),
    AsOf(JoinAsOfIter<'a>),
}

impl<'a> JoinIterator<'a> {
    pub(crate) fn new(left: &'a [Sample], right: &'a [Sample], join_type: JoinType) -> Self {
        match join_type {
            JoinType::AsOf(dir, tolerance) => {
                Self::AsOf(JoinAsOfIter::new(left, right, dir, tolerance))
            }
            JoinType::Left(exclusive) => if exclusive {
                Self::LeftExclusive(JoinLeftExclusiveIter::new(left, right))
            } else {
                Self::Left(JoinLeftIter::new(left, right))
            }
            JoinType::Right(exclusive) => if exclusive {
                Self::RightExclusive(JoinRightExclusiveIter::new(left, right))
            } else {
                Self::Right(JoinRightIter::new(left, right))
            }
            JoinType::Inner => Self::Inner(JoinInnerIter::new(left, right)),
            JoinType::Full => Self::Full(JoinFullIter::new(left, right)),
        }
    }
}

impl<'a> Iterator for JoinIterator<'a> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            JoinIterator::Left(iter) => iter.next(),
            JoinIterator::LeftExclusive(iter) => iter.next(),
            JoinIterator::Right(iter) => iter.next(),
            JoinIterator::RightExclusive(iter) => iter.next(),
            JoinIterator::Inner(iter) => iter.next(),
            JoinIterator::Full(iter) => iter.next(),
            JoinIterator::AsOf(iter) => iter.next(),
        }
    }
}

fn transform_join_value(item: &JoinValue, f: BinopFunc) -> JoinValue {
    match item.value {
        EitherOrBoth::Both(l, r) => JoinValue::left(item.timestamp, f(l, r)),
        EitherOrBoth::Left(l) => JoinValue::left(item.timestamp, f(l, f64::NAN)),
        EitherOrBoth::Right(r) => JoinValue::left(item.timestamp, f(f64::NAN, r)),
    }
}