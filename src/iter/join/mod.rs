use joinkit::EitherOrBoth;
use crate::common::types::Sample;
use crate::module::types::JoinValue;

mod join_inner_iter;
mod join_full_iter;
mod join_right_exclusive_iter;
mod join_left_exclusive_iter;
mod join_left_iter;
mod join_right_iter;
mod join_asof_iter;
mod join_iter;

pub use join_iter::*;


pub(crate) fn convert_join_item(item: EitherOrBoth<&Sample, &Sample>) -> JoinValue {
    match item {
        EitherOrBoth::Both(l, r) => JoinValue::both(l.timestamp, l.value, r.value),
        EitherOrBoth::Left(l) => JoinValue::left(l.timestamp, l.value),
        EitherOrBoth::Right(r) => JoinValue::right(r.timestamp, r.value),
    }
}