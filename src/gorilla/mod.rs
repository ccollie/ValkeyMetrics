// MIT License

// Copyright (c) 2016 Jerome Froelich

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! A crate for time series compression based upon Facebook's white paper
//! [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
//! `tsz` provides functionality for compressing a stream of `DataPoint`s, which are composed of a
//! time and value, into bytes, and decompressing a stream of bytes into `DataPoint`s.
//!

use std::cmp::Ordering;

/// Bit
///
/// An enum used to represent a single bit, can be either `Zero` or `One`.
#[derive(Debug, PartialEq)]
pub enum Bit {
    Zero,
    One,
}

impl Bit {
    /// Convert a bit to u64, so `Zero` becomes 0 and `One` becomes 1.
    pub fn to_u64(&self) -> u64 {
        match self {
            Bit::Zero => 0,
            Bit::One => 1,
        }
    }
}

/// DataPoint
///
/// Struct used to represent a single datapoint. Consists of a time and value.
#[derive(Debug, Copy, serde::Deserialize, serde::Serialize)]
pub struct DataPoint {
    time: u64,
    value: f64,
}

impl Clone for DataPoint {
    fn clone(&self) -> DataPoint {
        *self
    }
}

impl DataPoint {
    // Create a new DataPoint from a time and value.
    pub fn new(time: u64, value: f64) -> Self {
        DataPoint { time, value }
    }

    // Get the time for this DataPoint.
    pub fn get_time(&self) -> u64 {
        self.time
    }

    // Get the value for this DataPoint.
    pub fn get_value(&self) -> f64 {
        self.value
    }
}

impl PartialEq for DataPoint {
    #[inline]
    fn eq(&self, other: &DataPoint) -> bool {
        if self.time == other.time {
            return if self.value.is_nan() {
                other.value.is_nan()
            } else {
                self.value == other.value
            }
        }
        false
    }
}

impl Eq for DataPoint {}

impl Ord for DataPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}

impl PartialOrd for DataPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub mod stream;
pub mod encoder;
pub mod decoder;

#[cfg(test)]
mod tests {
    extern crate test_case;

    use crate::common::types::Sample;
use std::vec::Vec;
    use crate::gorilla::DataPoint;
    use super::encoder::{Encode, StdEncoder};
    use super::decoder::{Decode, Error, StdDecoder};
    use super::stream::{BufferedReader, BufferedWriter};

    // A representative time series.
    const DATA_1: &'static str = "1482892270,1.76
1482892280,7.78
1482892288,7.95
1482892292,5.53
1482892310,4.41
1482892323,5.30
1482892334,5.30
1482892341,2.92
1482892350,0.73
1482892360,-1.33
1482892370,-1.78
1482892390,-12.45
1482892401,-34.76
1482892490,78.9
1482892500,335.67
1482892800,12908.12
";

    // A time series where there is relatively large variation in times.
    const DATA_2: &'static str = "0,0.0
1,0.0
5000,0.0";

    #[test_case::test_case(1482892260, DATA_1 ; "a representative time series")]
    #[test_case::test_case(0, DATA_2 ; "a time series with relatively large variation in times")]
    fn integration_test(start_time: u64, data: &str) {
        let w = BufferedWriter::new();
        let mut encoder = StdEncoder::new(start_time, w);

        let mut original_datapoints = Vec::new();

        for line in data.lines() {
            let substrings: Vec<&str> = line.split(",").collect();
            let t = substrings[0].parse::<i64>().unwrap();
            let v = substrings[1].parse::<f64>().unwrap();
            let dp = Sample::new(t, v);
            original_datapoints.push(dp);
        }

        for dp in &original_datapoints {
            let dp = DataPoint::new(dp.timestamp as u64, dp.value);
            encoder.encode(dp);
        }

        let bytes = encoder.close();
        let r = BufferedReader::new(&bytes);
        let mut decoder = StdDecoder::new(r);

        let mut new_datapoints = Vec::new();

        let mut done = false;
        loop {
            if done {
                break;
            }

            match decoder.next() {
                Ok(dp) => new_datapoints.push(Sample {
                    timestamp: dp.get_time() as i64,
                    value: dp.get_value(),
                }),
                Err(err) => {
                    if err == Error::EndOfStream {
                        done = true;
                    } else {
                        panic!("Received an error from decoder: {:?}", err);
                    }
                }
            };
        }

        assert_eq!(original_datapoints, new_datapoints);
    }
}