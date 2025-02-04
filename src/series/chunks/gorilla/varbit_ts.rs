use super::traits::{BitRead, BitWrite};
use super::utils::{read_bits, read_bool, sign_extend};

/// Writes a i64 as a Prometheus varbit timestamp.
pub fn write_varbit_ts<W: BitWrite>(value: i64, writer: &mut W) -> std::io::Result<()> {
    match value {
        0 => writer.write_bit(false)?,
        // 1 to 14 bits
        -8191..=8192 => {
            writer.write_out::<2, u8>(0b10)?;
            writer.write_out::<14, u64>(value as u64 & 0x3FFF)?;
        }
        // 15 to 17 bits
        -65535..=65536 => {
            writer.write_out::<3, u8>(0b110)?;
            writer.write_out::<17, u64>(value as u64 & 0x1FFFF)?;
        }
        // 18 to 20 bits
        -524287..=524288 => {
            writer.write_out::<4, u8>(0b1110)?;
            writer.write_out::<20, u64>(value as u64 & 0x0FFFFF)?;
        }
        _ => {
            writer.write_out::<4, u8>(0b1111)?;
            writer.write_out::<64, u64>(value as u64)?;
        }
    }
    Ok(())
}

/// Reads a varbit-encoded integer from the input.
///
/// Prometheus' varbitint starts with a bucket category of variable length.
/// It consists of 1 bits and a final 0, up to 8 bits.
/// When it's 8 bits long, the final 0 is skipped.
///
/// It consists of 9 categories.
fn read_varbit_ts_bucket<R: BitRead>(reader: &mut R) -> std::io::Result<u8> {
    for i in 0..4 {
        let bit = read_bool(reader)?;
        // If we read a 0, it's a sign that we reached the end of the bucket category.
        if !bit {
            return Ok(i);
        }
    }

    // If we read 4 bits already, there is no final 0.
    Ok(4)
}

#[inline]
fn varbit_ts_bucket_to_num_bits(bucket: u8) -> u8 {
    match bucket {
        0 => 0,
        1 => 14,
        2 => 17,
        3 => 20,
        4 => 64,
        _ => unreachable!("Invalid bucket value"),
    }
}

/// Reads a Prometheus varbit timestamp encoded number from the input.
fn read_varbit_ts<R: BitRead>(input: &mut R) -> std::io::Result<i64> {
    let bucket= read_varbit_ts_bucket(input)?;
    let num_bits = varbit_ts_bucket_to_num_bits(bucket);

    // Shortcut for the 0 use case as nothing more has to be read.
    if bucket == 0 {
        return Ok(0);
    }

    let value = read_bits(input,  num_bits as u32)?;
    if num_bits != 64 && value > (1 << (num_bits - 1)) {
        return Ok(sign_extend(value, num_bits as u32))
    }

    Ok(value as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::chunks::gorilla::buffered_read::BufferedReader;
    use crate::series::chunks::gorilla::buffered_writer::BufferedWriter;
    use crate::series::chunks::gorilla::utils::generate_random_test_data;

    #[test]
    fn test_write_varbit_ts() {
        let mut test_cases = generate_random_test_data(42);

        // add just a test case with the weird clamping
        test_cases.push(vec![i64::MAX, 0, i64::MIN, i64::MAX, i64::MIN]);

        for test_case in test_cases {
            // Writing first
            let mut bit_writer = BufferedWriter::new();

            for number in &test_case {
                write_varbit_ts(*number, &mut bit_writer).unwrap();
            }

            let cursor = bit_writer.get_ref();
            // Read again
            let mut cursor = BufferedReader::new(&cursor);
            for number in test_case {
                let new_value = read_varbit_ts(&mut cursor).unwrap();
                assert_eq!(new_value, number);
            }
        }
    }
}
