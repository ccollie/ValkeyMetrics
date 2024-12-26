// Portions Copyright (c)
// Copyright (c) 2016 Google Inc. (lewinb@google.com) -- though not an official
// Google product or in any way related!
// Copyright (c) 2018-2020 Lewin Bormann (lbo@spheniscida.de)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

// Portions Copyright (c) Apache 2.0
// https://github.com/SINTEF/rusty-chunkenc
use super::required_encoded_space_unsigned;
use crate::common::{read_uvarint, write_uvarint};
use nom::IResult;

/// How many bytes an integer uses when being encoded as a VarInt.
#[inline]
fn required_encoded_space_signed(v: i64) -> usize {
    required_encoded_space_unsigned(zigzag_encode(v))
}

#[inline]
fn zigzag_encode(from: i64) -> u64 {
    ((from << 1) ^ (from >> 63)) as u64
}

// see: http://stackoverflow.com/a/2211086/56332
// casting required because operations like unary negation
// cannot be performed on unsigned integers
#[inline]
fn zigzag_decode(from: u64) -> i64 {
    ((from >> 1) ^ (-((from & 1) as i64)) as u64) as i64
}

/// Parses a Golang varint.
pub fn read_varint(input: &[u8]) -> IResult<&[u8], i64> {
    let (remaining_input, uvarint_value) = read_uvarint(input)?;

    let value = (uvarint_value >> 1) as i64;
    if uvarint_value & 1 != 0 {
        Ok((remaining_input, !value))
    } else {
        Ok((remaining_input, value))
    }
}

/// Write an i64 as a Golang varint.
pub fn write_varint<W: std::io::Write>(value: i64, writer: &mut W) -> std::io::Result<()> {
    let x = value;
    let mut ux = (x as u64) << 1;
    if x < 0 {
        ux = !ux;
    }
    write_uvarint(ux, writer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    #[test]
    fn test_with_boring_values() {
        let input = b"\x00";
        let (_, value) = read_varint(input).unwrap();
        assert_eq!(value, 0);

        let input = b"\x01";
        let (_, value) = read_varint(input).unwrap();
        assert_eq!(value, -1);

        let input = b"\x02";
        let (_, value) = read_varint(input).unwrap();
        assert_eq!(value, 1);

        let input = b"\x7f";
        let (_, value) = read_varint(input).unwrap();
        assert_eq!(value, -64);

        let input = b"\x80\x01";
        let (_, value) = read_varint(input).unwrap();
        assert_eq!(value, 64);

        let input = b"\xff\x01";
        let (_, value) = read_varint(input).unwrap();
        assert_eq!(value, -128);

        let input = b"\xac\x02";
        let (_, value) = read_varint(input).unwrap();
        assert_eq!(value, 150);

        let input = b"\x80\x80\x01";
        let (_, value) = read_varint(input).unwrap();
        assert_eq!(value, 8192);

        let input = b"\x80\x80\x02";
        let (_, value) = read_varint(input).unwrap();
        assert_eq!(value, 16384);

        let input = b"\x81\x80\x02";
        let (_, value) = read_varint(input).unwrap();
        assert_eq!(value, -16385);
    }

    #[test]
    fn test_with_weird_data() {
        let input = "hello world".as_bytes();
        let (_, value) = read_varint(input).unwrap();
        assert_eq!(value, 52);
    }

    #[test]
    fn test_with_overflows() {
        // Classic overflow
        let input = b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01";
        let result = read_varint(input);
        assert!(result.is_err());

        // More subtle overflow
        let input = b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x02";
        let result = read_varint(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_varint() {
        let mut buffer: Vec<u8> = Vec::new();
        let mut writer = std::io::Cursor::new(&mut buffer);

        let mut numbers = vec![
            i64::MIN,
            -36028797018963968,
            -36028797018963967,
            -16777216,
            -16777215,
            -131072,
            -131071,
            -2048,
            -2047,
            -256,
            -255,
            -32,
            -31,
            -4,
            -3,
            -1,
            0,
            1,
            4,
            5,
            32,
            33,
            256,
            257,
            2048,
            2049,
            131072,
            131073,
            16777216,
            16777217,
            36028797018963968,
            36028797018963969,
            i64::MAX,
        ];

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        // Add some random numbers
        for _ in 0..100 {
            let number: i64 = rng.gen();
            numbers.push(number);
        }

        // Write
        for number in &numbers {
            write_varint(*number, &mut writer).unwrap();
        }

        // Read
        let mut cursor = &buffer[..];
        for number in numbers {
            let (new_cursor, read_number) = read_varint(cursor).unwrap();
            assert_eq!(read_number, number);
            cursor = new_cursor;
        }
    }
}
