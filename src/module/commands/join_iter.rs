use crate::common::types::Sample;
use crate::module::commands::join::transform_join_value;
use crate::module::commands::join_asof_iter::JoinAsOfIter;
use crate::module::commands::join_full_iter::JoinFullIter;
use crate::module::commands::join_inner_iter::JoinInnerIter;
use crate::module::commands::join_left_exclusive_iter::JoinLeftExclusiveIter;
use crate::module::commands::join_left_iter::JoinLeftIter;
use crate::module::commands::join_right_exclusive_iter::JoinRightExclusiveIter;
use crate::module::commands::join_right_iter::JoinRightIter;
use metricsql_parser::ast::Operator;
use metricsql_parser::binaryop::get_scalar_binop_handler;
use metricsql_parser::prelude::BinopFunc;
use crate::module::types::{JoinOptions, JoinType, JoinValue};

pub struct JoinTransformIter<'a> {
    inner: Box<JoinIterator<'a>>,
    func: BinopFunc
}

impl<'a> JoinTransformIter<'a> {
    pub fn new(base: JoinIterator<'a>, op: Operator) -> Self {
        Self {
            inner: Box::new(base),
            func: get_scalar_binop_handler(op, true)
        }
    }
}

impl<'a> Iterator for JoinTransformIter<'a> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(val) => Some(transform_join_value(&val, self.func)),
            None => None
        }
    }
}

pub enum JoinIterator<'a> {
    Left(JoinLeftIter<'a>),
    LeftExclusive(JoinLeftExclusiveIter<'a>),
    Right(JoinRightIter<'a>),
    RightExclusive(JoinRightExclusiveIter<'a>),
    Inner(JoinInnerIter<'a>),
    Full(JoinFullIter<'a>),
    AsOf(JoinAsOfIter<'a>),
    Transform(JoinTransformIter<'a>)
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

    pub(crate) fn new_from_options(left: &'a [Sample], right: &'a [Sample], options: &JoinOptions) -> Self {
        let iter = Self::new(left, right, options.join_type);
        if let Some(transform_op) = options.transform_op {
            JoinIterator::Transform(JoinTransformIter::new(iter, transform_op))
        } else {
            iter
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
            JoinIterator::Transform(iter) => iter.next(),
        }
    }
}