use nom::{bytes::complete::take, IResult};

/// Most-significant byte, == 0x80
pub const MSB: u8 = 0b1000_0000;
/// All bits except for the most significant. Can be used as bitmask to drop the most-signficant
/// bit using `&` (binary-and).
const DROP_MSB: u8 = 0b0111_1111;

/// How many bytes an integer uses when being encoded as a VarInt.
#[inline]
pub(super) fn required_encoded_space_unsigned(mut v: u64) -> usize {
    if v == 0 {
        return 1;
    }

    let mut logcounter = 0;
    while v > 0 {
        logcounter += 1;
        v >>= 7;
    }
    logcounter
}

/// Decode a value from the slice. Returns the value and the number of bytes read from the
/// slice (can be used to read several consecutive values from a big slice)
/// return None if all bytes has MSB set.
#[inline]
pub fn decode_var(src: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;

    let mut success = false;
    for b in src.iter() {
        let msb_dropped = b & DROP_MSB;
        result |= (msb_dropped as u64) << shift;
        shift += 7;

        if b & MSB == 0 || shift > (9 * 7) {
            success = b & MSB == 0;
            break;
        }
    }

    if success {
        Some((result, shift / 7))
    } else {
        None
    }
}

/// Encode a value into the slice. The slice must be at least `required_space()` bytes long.
/// The number of bytes taken by the encoded integer is returned.
#[inline]
pub fn encode_var(val: u64, dst: &mut [u8]) -> usize {
    debug_assert!(dst.len() >= required_encoded_space_unsigned(val));
    let mut n = val;
    let mut i = 0;

    while n >= 0x80 {
        dst[i] = MSB | (n as u8);
        i += 1;
        n >>= 7;
    }

    dst[i] = n as u8;
    i + 1
}


/// Write a u64 as a Golang uvarint.
pub fn write_uvarint<W: std::io::Write>(value: u64, writer: &mut W) -> std::io::Result<()> {
    let mut x: u64 = value;
    while x >= 0x80 {
        let enc_buf = [0x80 | (x as u8)];
        writer.write_all(&enc_buf)?;
        x >>= 7;
    }
    writer.write_all(&[x as u8])?;

    Ok(())
}

/// Parses a Golang uvarint.
pub fn read_uvarint(input: &[u8]) -> IResult<&[u8], u64> {
    let mut input_pointer = input;
    let mut x: u64 = 0;
    let mut s: usize = 0;

    for i in 0..10 {
        let (new_input_pointer, byte_buffer) = take(1usize)(input_pointer)?;
        input_pointer = new_input_pointer;
        let byte = byte_buffer[0];

        if byte < 0x80 {
            if i == 9 && byte > 1 {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::TooLarge,
                )));
            }
            return Ok((input_pointer, x | (byte as u64) << s));
        }

        x |= ((byte & 0x7f) as u64) << s;
        s += 7;
    }

    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::TooLarge,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    #[test]
    fn test_with_boring_values() {
        let input = b"\x00";
        let (_, value) = read_uvarint(input).unwrap();
        assert_eq!(value, 0);

        let input = b"\x01";
        let (_, value) = read_uvarint(input).unwrap();
        assert_eq!(value, 1);

        let input = b"\x7f";
        let (_, value) = read_uvarint(input).unwrap();
        assert_eq!(value, 127);

        let input = b"\x80\x01";
        let (_, value) = read_uvarint(input).unwrap();
        assert_eq!(value, 128);

        let input = b"\xff\x01";
        let (_, value) = read_uvarint(input).unwrap();
        assert_eq!(value, 255);

        let input = b"\xac\x02";
        let (_, value) = read_uvarint(input).unwrap();
        assert_eq!(value, 300);

        let input = b"\x80\x80\x01";
        let (_, value) = read_uvarint(input).unwrap();
        assert_eq!(value, 16384);
    }

    #[test]
    fn test_with_weird_data() {
        let input = "hello world".as_bytes();
        let (_, value) = read_uvarint(input).unwrap();
        assert_eq!(value, 104);
    }

    #[test]
    fn test_with_overflows() {
        // Classic overflow
        let input = b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01";
        let result = read_uvarint(input);
        assert!(result.is_err());

        // More subtle overflow
        let input = b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x02";
        let result = read_uvarint(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_uvarint() {
        let mut buffer: Vec<u8> = Vec::new();
        let mut writer = std::io::Cursor::new(&mut buffer);

        let mut numbers = vec![
            0,
            1,
            7,
            8,
            63,
            64,
            511,
            512,
            4095,
            4096,
            262143,
            262144,
            33554431,
            33554432,
            72057594037927935,
            72057594037927936,
            u64::MAX,
        ];

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        // Add some random numbers
        for _ in 0..100 {
            let number: u64 = rng.gen();
            numbers.push(number);
        }

        // Write
        for number in &numbers {
            write_uvarint(*number, &mut writer).unwrap();
        }

        // Read
        let mut cursor = &buffer[..];
        for number in numbers {
            let (new_cursor, read_number) = read_uvarint(cursor).unwrap();
            assert_eq!(read_number, number);
            cursor = new_cursor;
        }
    }
}
