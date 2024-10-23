use crate::common::types::Sample;
use joinkit::Joinkit;
use super::JoinValue;

// todo: accept iter instead of slices
pub struct JoinLeftExclusiveIter<'a> {
    inner: Box<dyn Iterator<Item=&'a Sample> + 'a>
}

impl<'a> JoinLeftExclusiveIter<'a> {
    pub fn new(left: &'a [Sample], right: &'a [Sample]) -> Self {
        let iter = left.iter()
            .merge_join_left_excl_by(right, |x, y| x.timestamp.cmp(&y.timestamp));

        Self {
            inner: Box::new(iter),
        }
    }
}

impl<'a> Iterator for JoinLeftExclusiveIter<'a> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| JoinValue::left(item.timestamp, item.value))
    }
}