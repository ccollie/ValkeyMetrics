use nom::IResult;
use crate::common::{read_uvarint, write_uvarint};

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
    use rand::{Rng, SeedableRng};
    use super::*;

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