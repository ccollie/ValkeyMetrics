//! # TimeSeries Index Representation
use chrono::NaiveDateTime;
use serde::Serialize;
use std::cmp;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::hash::Hash;
use std::ops::Index;

/// a HashableIndex<TDate> serves as the index for a timeseries, it requires that the index element
/// be Serializable (via serde), Hashable, Cloneable, Equatable, and Orderable.
#[derive(Clone, Debug)]
pub struct HashableIndex<TIndex: Serialize + Hash + Clone + Eq + Ord> {
    pub values: Vec<TIndex>,
}

//SRC:: https://stackoverflow.com/questions/64262297/rust-how-to-find-n-th-most-frequent-element-in-a-collection
fn most_frequent<T>(array: &Vec<T>) -> Vec<(usize, T)>
where
    T: Hash + Eq + Ord + Clone,
{
    #![allow(clippy::ptr_arg)]
    let mut map = HashMap::new();
    for x in array {
        *map.entry(x).or_default() += 1;
    }
    let k = map.len();
    let mut heap = BinaryHeap::with_capacity(k);
    for (x, count) in map.into_iter() {
        heap.push(cmp::Reverse((count, x.clone())));
    }
    heap.into_sorted_vec().iter().map(|r| r.0.clone()).collect()
}

impl<TIndex: Serialize + Hash + Clone + Eq + Ord> HashableIndex<TIndex> {
    /// Create new index from a vec of values of type TIndex
    ///
    /// # Example
    ///
    /// ```
    /// use super::index::HashableIndex;
    ///
    /// let values = vec![1, 2, 3, 4];
    /// let index = HashableIndex::new(values);
    /// assert_eq!(index.len(), 4);
    /// ```
    pub fn new(values: Vec<TIndex>) -> HashableIndex<TIndex> {
        HashableIndex { values }
    }


    /// test the monotonicity test for an index
    ///
    /// # Example
    ///
    /// ```
    /// use super::index::HashableIndex;
    ///
    /// let vs = HashableIndex::new(vec![1, 2, 3, 4]);
    /// let xs = HashableIndex::new(vec![1, 2, 3, 3]);
    /// let ys = HashableIndex::new(vec![1, 2, 3, 2]);
    /// assert_eq!(vs.is_monotonic(), true);
    /// assert_eq!(xs.is_monotonic(), false);
    /// assert_eq!(ys.is_monotonic(), false);
    /// ```
    pub fn is_monotonic(&self) -> bool {
        self.values
            .iter()
            .zip(self.values.iter().skip(1))
            .all(|(x, y)| x < y)
    }


    /// get length of the index
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// is the index empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// ref to the last value of an index
    pub fn last(&self) -> std::option::Option<&TIndex> {
        self.values.last()
    }

    /// very slow, tests if index is unique by generating a hashset of the index keys and then comparing lengths
    pub fn is_unique(&self) -> bool {
        let set: HashSet<&TIndex> = self.iter().collect();
        set.len() == self.len()
    }

    /// generate and iterator for the index
    pub fn iter(&self) -> std::slice::Iter<TIndex> {
        self.values.iter()
    }
}


impl<TIndex: Serialize + Hash + Clone + Eq + Ord> Index<usize> for HashableIndex<TIndex> {
    type Output = TIndex;

    fn index(&self, pos: usize) -> &Self::Output {
        &self.values[pos]
    }
}

impl<TIndex: Serialize + Hash + Clone + Eq + Ord> PartialEq for HashableIndex<TIndex> {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

/// -----------------------------------------------------------------------------------------------------------------------------------------
/// Unit Test Area
/// -----------------------------------------------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_empty() {
        let index: HashableIndex<NaiveDateTime> = HashableIndex::new(vec![]);
        assert!(index.is_monotonic());
    }
}