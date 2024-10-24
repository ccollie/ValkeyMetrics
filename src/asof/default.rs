// Copyright (c) 2020 Ritchie Vink
// Some portions Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
// https://github.com/pola-rs/polars/blob/main/crates/polars-ops/src/frame/join/asof/default.rs
use crate::common::types::{Sample};
use super::{
    AsofJoinBackwardState,
    AsofJoinForwardState,
    AsofJoinNearestState,
    AsofJoinState,
    AsofStrategy,
    IdxSize
};


fn join_asof_impl<'a, T, S, F>(left: &'a [T], right: &'a [T], mut filter: F) -> Vec<(&'a T, &'a T)>
where
    S: AsofJoinState<'a, T>,
    F: FnMut(&T, &T) -> bool,
{
    let mut out = Vec::with_capacity(left.len());
    let mut state = S::default();

    for left_val in left.iter() {
        if let Some(r_idx) = state.next(
            &left_val,
            // SAFETY: next() only calls with indices < right.len().
            |j| Some(unsafe { right.get_unchecked(j as usize) }),
            right.len() as IdxSize,
        ) {
            // SAFETY: r_idx is non-null and valid.
            let right_val = unsafe { right.get_unchecked(r_idx as usize) };
            if filter(left_val, right_val) {
                out.push((left_val, right_val));
            }
        }
    }

    out
}

pub fn join_asof_forward<'a, T, F>(left: &'a [T], right: &'a [T], filter: F) -> Vec<(&'a T, &'a T)>
where
    T: PartialOrd,
    F: FnMut(&T, &T) -> bool,
{
    join_asof_impl::<T, AsofJoinForwardState, _>(left, right, filter)
}

pub fn join_asof_backward<'a, T, F>(left: &'a [T], right: &'a [T], filter: F) -> Vec<(&'a T, &'a T)>
where
    T: PartialOrd,
    F: FnMut(&T, &T) -> bool,
{
    join_asof_impl::<T, AsofJoinBackwardState, _>(left, right, filter)
}

pub fn join_asof_nearest<'a, T, F>(left: &'a [T], right: &'a [T], filter: F) -> Vec<(&'a T, &'a T)>
where
    F: FnMut(&T, &T) -> bool,
{
    join_asof_impl::<T, AsofJoinNearestState, _>(left, right, filter)
}


pub(crate) fn join_asof_samples<'a>(
    left: &'a [Sample],
    right: &'a [Sample],
    strategy: AsofStrategy,
    tolerance: Option<i64>,
) -> Vec<(&'a Sample, &'a Sample)> {
    if let Some(t) = tolerance {
        let abs_tolerance = t.abs_diff(0);
        let filter = |l: &Sample, r: &Sample| l.timestamp.abs_diff(r.timestamp) <= abs_tolerance;
        match strategy {
            AsofStrategy::Forward => join_asof_forward::<Sample, _>(left, right, filter),
            AsofStrategy::Backward => join_asof_backward::<Sample, _>(left, right, filter),
            AsofStrategy::Nearest => join_asof_nearest::<Sample, _>(left, right, filter),
        }
    } else {
        let filter = |_l: &Sample, _r: &Sample| true;
        match strategy {
            AsofStrategy::Forward => join_asof_forward::<Sample, _>(left, right, filter),
            AsofStrategy::Backward => join_asof_backward::<Sample, _>(left, right, filter),
            AsofStrategy::Nearest => join_asof_nearest::<Sample, _>(left, right, filter),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_asof_backward() {
        let samples_a = vec![
            Sample { timestamp: -1, value: 1. },
            Sample { timestamp: 2, value: 2. },
            Sample { timestamp: 3, value: 3. },
            Sample { timestamp: 3, value: 4. },
            Sample { timestamp: 3, value: 6. },
            Sample { timestamp: 4, value: 5. },
        ];

        let b = vec![
            Sample { timestamp: 1, value: 1. },
            Sample { timestamp: 2, value: 2. },
            Sample { timestamp: 3, value: 3. },
            Sample { timestamp: 3, value: 4. },
        ];

        let tuples = join_asof_samples(&samples_a, &b, AsofStrategy::Backward, None);
        assert_eq!(tuples.len(), samples_a.len());
        let expected_right = &[1, 3, 3, 3, 3];
        // for (l, r) in tuples.into_iter() {
        //     assert_eq!(l, &b[i]);
        //     assert_eq!(r, samples_b[i].value);
        // }
        // assert_eq!(
        //     &[None, Some(1), Some(3), Some(3), Some(3), Some(3)]
        // );

        let b = [1, 2, 4, 5];
        let samples_b = vec![
            Sample { timestamp: 1000, value: 1. },
            Sample { timestamp: 2000, value: 2. },
            Sample { timestamp: 4000, value: 4. },
            Sample { timestamp: 5000, value: 5. },
        ];
        let tuples = join_asof_samples(&samples_a, &samples_b, AsofStrategy::Backward, None);
        // assert_eq!(
        //     &[None, Some(1), Some(1), Some(1), Some(1), Some(2)]
        // );

        let a = [2, 4, 4, 4];
        let b = [1, 2, 3, 3];
        let a = vec![
            Sample { timestamp: -1, value: 1.0 },
            Sample { timestamp: 2000, value: 2.0 },
            Sample { timestamp: 3000, value: 3.0 },
            Sample { timestamp: 3000, value: 4.0 }
        ];

        let b = vec![
            Sample { timestamp: 1000, value: 1.0 },
            Sample { timestamp: 2000, value: 2.0 },
            Sample { timestamp: 3000, value: 3.0 },
            Sample { timestamp: 3000, value: 4.0 }
        ];

        let tuples = join_asof_samples(&a, &b, AsofStrategy::Backward, None);
        //assert_eq!(tuples.to_vec(), &[Some(1000), Some(3000), Some(3000), Some(3000)]);
    }

    #[test]
    fn test_asof_backward_tolerance() {
        let a = [-1, 20, 25, 30, 30, 40];
        let b = [10, 20, 30, 30];

        let a = vec![
            Sample { timestamp: -1, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 25, value: 3.0 },
            Sample { timestamp: 30, value: 4.0 },
            Sample { timestamp: 30, value: 5.0 },
            Sample { timestamp: 40, value: 6.0 }
        ];

        let b = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 30, value: 3.0 },
            Sample { timestamp: 30, value: 4.0 }
        ];
        let tuples = join_asof_samples(&a, &b, AsofStrategy::Backward, Some(4));
        // assert_eq!(
        //     &[None, Some(1), None, Some(3), Some(3), None]
        // );
    }

    #[test]
    fn test_asof_forward_tolerance() {
        let a = [1, 20, 25, 30, 30, 40, 52];
        let b = [10, 20, 33, 55];
        let tuples = join_asof_forward::<u32, _>(&a, &b, |l, r| l.abs_diff(r) <= 4u32);
        // assert_eq!(
        //     &[Some(1), None, Some(2), Some(2), None, Some(3)]
        // );
    }

    #[test]
    fn test_asof_forward() {
        let a = vec![
            Sample { timestamp: -1, value: 0.1 },
            Sample { timestamp: 1, value: 0.2 },
            Sample { timestamp: 2, value: 0.3 },
            Sample { timestamp: 4, value: 0.4 },
            Sample { timestamp: 6, value: 0.5 },
        ];

        let b = vec![
            Sample { timestamp: 1, value: 1.0 },
            Sample { timestamp: 2, value: 2.0 },
            Sample { timestamp: 4, value: 4.0 },
            Sample { timestamp: 5, value: 5.0 },
        ];

        let tuples = join_asof_samples(&a, &b, AsofStrategy::Forward, None);
        assert_eq!(tuples.len(), a.len());
       // assert_eq!(tuples.to_vec(), &[Some(0), Some(0), Some(1), Some(2), None]);
    }
    /////

#[test]
fn test_asof_forward_no_matches() {
    let left = vec![
        Sample { timestamp: 1, value: 1.0 },
        Sample { timestamp: 2, value: 2.0 },
        Sample { timestamp: 3, value: 3.0 },
    ];

    let right = vec![
        Sample { timestamp: 4, value: 4.0 },
        Sample { timestamp: 5, value: 5.0 },
        Sample { timestamp: 6, value: 6.0 },
    ];

    let tuples = join_asof_samples(&left, &right, AsofStrategy::Forward, Some(1));
    assert_eq!(tuples.len(), 0);
}
}