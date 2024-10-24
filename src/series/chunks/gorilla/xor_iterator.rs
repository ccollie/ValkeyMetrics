use crate::common::types::Sample;
use crate::common::{read_uvarint, read_varint, NomBitInput};
use crate::error::{TsdbError, TsdbResult};
use nom::{
    bytes,
    number::complete::be_f64,
    sequence::tuple
};
use crate::series::chunks::gorilla::varbit::read_varbit_int;
use super::varbit_ts::read_varbit_ts;
use super::varbit_xor::read_varbit_xor;
use super::XOREncoder;

#[derive(Debug)]
pub struct XORIterator<'a> {
    buf: &'a [u8],
    cursor: NomBitInput<'a>,
    idx: usize,
    num_samples: usize,
    leading_bits_count: u8,
    trailing_bits_count: u8,
    timestamp_delta: u64,
    timestamp: i64,
    value: f64,
    last_timestamp: i64,
    last_value: f64,
}

impl XORIterator<'_> {
    pub fn new(encoder: &XOREncoder) -> XORIterator {
        let buf = encoder.buf();
        let num_samples = encoder.num_samples;
        let cursor = (buf, 0);
        let last_timestamp = encoder.timestamp;
        let last_value = encoder.value;

        XORIterator {
            buf,
            cursor,
            idx: 0,
            num_samples,
            timestamp: 0,
            value: 0.0,
            leading_bits_count: 0,
            trailing_bits_count: 0,
            timestamp_delta: 0,
            last_timestamp,
            last_value,
        }
    }

    fn read_first_sample(&mut self) -> TsdbResult<Sample> {
        let (remaining_input, (timestamp, value)) = tuple((read_varint, be_f64))(self.buf)
            .map_err(|_| {
                TsdbError::DecodingError("XOR encoder".to_string())
            })?;

        self.timestamp = timestamp;
        self.value = value;
        self.cursor.0 = remaining_input;
        self.cursor.1 = 0;

        self.idx += 1;
        Ok(Sample {
            timestamp: self.timestamp,
            value: self.value,
        })
    }

    fn read_second_sample(&mut self) -> TsdbResult<Sample> {
        let value = self.value;
        let leading_bits = self.leading_bits_count;
        let trailing_bits = self.trailing_bits_count;

        let (remaining_input, (timestamp_delta, (value, new_leading_bits_count, new_trailing_bits_count)))
            = tuple((bytes(read_uvarint), read_varbit_xor(value, leading_bits, trailing_bits)))(self.cursor)
            .map_err(|_| {
                TsdbError::DecodingError("XOR encoder".to_string())
            })?;

        self.timestamp += i64::try_from(timestamp_delta).map_err(|_| {
            TsdbError::DecodingError("Timestamp delta too large".to_string())
        })?;

        self.value = value;
        self.leading_bits_count = new_leading_bits_count;
        self.trailing_bits_count = new_trailing_bits_count;
        self.timestamp_delta = timestamp_delta;
        self.cursor = remaining_input;

        self.idx += 1;
        Ok(Sample {
            timestamp: self.timestamp,
            value: self.value,
        })
    }

    fn read_n_sample(&mut self) -> TsdbResult<Sample> {
        let previous_timestamp = self.timestamp;
        let previous_value = self.value;
        let previous_leading_bits_count = self.leading_bits_count;
        let previous_trailing_bits_count = self.trailing_bits_count;
        let previous_timestamp_delta = self.timestamp_delta;

        let (
            remaining_input,
            (timestamp_delta_of_delta, (value, new_leading_bits_count, new_trailing_bits_count)),
        ) = tuple((
            read_varbit_int,
            //read_varbit_ts,
            read_varbit_xor(
                previous_value,
                previous_leading_bits_count,
                previous_trailing_bits_count,
            ),
        ))(self.cursor)
            .map_err(|e| {
                println!("{:?}", e);
                TsdbError::DecodingError("XOR encoder".to_string())
            })?;

        let timestamp_delta = ((previous_timestamp_delta as i64) + timestamp_delta_of_delta) as u64;

        self.cursor = remaining_input;
        self.timestamp_delta = timestamp_delta;
        self.timestamp = previous_timestamp + timestamp_delta as i64;
        self.leading_bits_count = new_leading_bits_count;
        self.trailing_bits_count = new_trailing_bits_count;
        self.value = value;
        self.idx += 1;

        Ok(Sample {
            timestamp: self.timestamp,
            value: self.value,
        })
    }

    fn read_last_sample(&mut self) -> TsdbResult<Sample> {
        self.idx += 1;
        Ok(Sample {
            timestamp: self.last_timestamp,
            value: self.last_value,
        })
    }
}

impl<'a> Iterator for XORIterator<'a> {
    type Item = TsdbResult<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.num_samples {
            return None;
        } else if self.idx == self.num_samples - 1 {
            return Some(self.read_last_sample());
        }
        Some(match self.idx {
            0 => self.read_first_sample(),
            1 => self.read_second_sample(),
            _ => self.read_n_sample()
        })
    }
}

#[cfg(test)]
mod tests {
}