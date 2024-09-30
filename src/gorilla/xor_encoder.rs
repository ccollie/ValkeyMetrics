use std::mem::size_of_val;
use crate::common::bitwriter::{BigEndian, BitWrite, BitWriter};
use crate::common::types::Sample;
use crate::common::{write_uvarint, write_varbit_ts, write_varbit_xor, write_varint};
use crate::gorilla::xor_iterator::XORIterator;
use bitstream_io::BitQueue;
use get_size::GetSize;
use smallvec::SmallVec;
use std::io::Write;
use valkey_module::error::Error as ValkeyError;
use valkey_module::raw;


#[derive(Debug)]
pub struct XOREncoder {
    pub writer: BitWriter<Vec<u8>, BigEndian>,
    pub num_samples: usize,
    pub timestamp: i64,
    pub value: f64,
    pub leading_bits_count: u8,
    pub trailing_bits_count: u8,
    pub timestamp_delta: i64,
}

impl GetSize for XOREncoder {
    fn get_size(&self) -> usize {
        self.writer.writer.get_size()
            + size_of_val(&self.num_samples)
            + size_of_val(&self.timestamp)
            + size_of_val(&self.value)
            + size_of_val(&self.leading_bits_count)
            + size_of_val(&self.trailing_bits_count)
            + size_of_val(&self.timestamp_delta)
    }
}

impl Clone for XOREncoder {
    fn clone(&self) -> Self {
        let buf = self.writer.writer.clone();
        let writer = BitWriter::endian(buf, BigEndian);
        XOREncoder {
            writer,
            num_samples: self.num_samples,
            timestamp: self.timestamp,
            value: self.value,
            leading_bits_count: self.leading_bits_count,
            trailing_bits_count: self.trailing_bits_count,
            timestamp_delta: self.timestamp_delta,
        }
    }
}

impl XOREncoder {
    pub fn new() -> XOREncoder {
        let writer = BitWriter::endian(vec![], BigEndian);

        XOREncoder {
            writer,
            num_samples: 0,
            timestamp: 0,
            value: 0.0,
            leading_bits_count: 0,
            trailing_bits_count: 0,
            timestamp_delta: 0,
        }
    }

    pub fn clear(&mut self) {
        let mut buf = std::mem::take(&mut self.writer.writer);
        buf.clear();
        self.writer = BitWriter::endian(buf, BigEndian);
        self.num_samples = 0;
        self.timestamp = 0;
        self.value = 0.0;
        self.leading_bits_count = 0;
        self.trailing_bits_count = 0;
        self.timestamp_delta = 0;
    }

    pub fn add_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        match self.num_samples {
            0 => self.write_first_sample(sample),
            1 => self.write_second_sample(sample),
            _ => self.write_n_sample(sample),
        }
    }

    fn write_first_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        let mut bytes: Vec<u8> = Vec::with_capacity(32);
        write_varint(sample.timestamp, &mut bytes)?;
        // Classic Float64 for the value
        bytes.write_all(&sample.value.to_be_bytes())?;

        self.writer = BitWriter::endian(bytes, BigEndian);
        self.timestamp = sample.timestamp;
        self.value = sample.value;
        self.num_samples += 1;
        Ok(())
    }

    fn write_second_sample(&mut self, sample: &Sample) -> std::io::Result<()> {

        let timestamp_delta = sample.timestamp - self.timestamp;
        if timestamp_delta < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "samples aren't sorted by timestamp ascending",
            ));
        }

        // I didn't find a more beautiful way to write the uvarint in the bitstream directly,
        // we use SmallVec so at least it's allocated on the stack and not the heap.
        let mut uvarint_bytes = SmallVec::<u8, 9>::new();
        write_uvarint(timestamp_delta as u64, &mut uvarint_bytes)?;
        self.writer.write_bytes(&uvarint_bytes)?;

        let (leading, trailing) = write_varbit_xor(
            sample.value,
            self.value,
            0xff,
            0,
            &mut self.writer
        )?;

        self.timestamp = sample.timestamp;
        self.value = sample.value;
        self.leading_bits_count = leading;
        self.trailing_bits_count = trailing;
        self.timestamp_delta = timestamp_delta;

        self.num_samples += 1;

        Ok(())
    }

    fn write_n_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        let timestamp = sample.timestamp;
        let value = sample.value;

        let timestamp_delta = timestamp - self.timestamp;
        if timestamp_delta < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "samples aren't sorted by timestamp ascending",
            ));
        }
        let timestamp_delta_of_delta = timestamp_delta - self.timestamp_delta;

        write_varbit_ts(timestamp_delta_of_delta, &mut self.writer)?;

        let (leading_bits_count, trailing_bits_count) = write_varbit_xor(
            value,
            self.value,
            self.leading_bits_count,
            self.trailing_bits_count,
            &mut self.writer,
        )?;

        self.timestamp = timestamp;
        self.value = value;
        self.leading_bits_count = leading_bits_count;
        self.trailing_bits_count = trailing_bits_count;
        self.timestamp_delta = timestamp_delta;

        self.num_samples += 1;

        Ok(())
    }

    pub fn buf_len(&self) -> usize {
        self.writer.writer.len()
    }

    pub fn iter(&self) -> XORIterator {
        XORIterator::new(&self)
    }

    pub(super) fn buf(&self) -> &[u8] {
        &self.writer.writer
    }

    pub fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        save_bitwriter_to_rdb(rdb, &self.writer);
        raw::save_unsigned(rdb, self.num_samples as u64);
        raw::save_signed(rdb, self.timestamp);
        raw::save_double(rdb, self.value);
        raw::save_unsigned(rdb, self.leading_bits_count as u64);
        raw::save_unsigned(rdb, self.trailing_bits_count as u64);
        raw::save_signed(rdb, self.timestamp_delta);
    }

    pub fn rdb_load(rdb: *mut raw::RedisModuleIO) -> Result<XOREncoder, ValkeyError> {
        let writer = load_bitwriter_from_rdb(rdb)?;
        let num_samples = raw::load_unsigned(rdb)? as usize;
        let timestamp = raw::load_signed(rdb)?;
        let value = raw::load_double(rdb)?;
        let leading_bits_count = raw::load_unsigned(rdb)? as u8;
        let trailing_bits_count = raw::load_unsigned(rdb)? as u8;
        let timestamp_delta = raw::load_signed(rdb)?;

        Ok(XOREncoder {
            writer,
            num_samples,
            timestamp,
            value,
            leading_bits_count,
            trailing_bits_count,
            timestamp_delta,
        })
    }
}

impl PartialEq<Self> for XOREncoder {
    fn eq(&self, other: &Self) -> bool {
        if self.num_samples != other.num_samples {
            return false;
        }
        if self.timestamp != other.timestamp {
            return false;
        }
        if self.value != other.value {
            return false;
        }
        if self.leading_bits_count != other.leading_bits_count {
            return false;
        }
        if self.trailing_bits_count != other.trailing_bits_count {
            return false;
        }
        if self.timestamp_delta != other.timestamp_delta {
            return false;
        }
        bitwriter_equals(&self.writer, &other.writer)
    }
}

impl Eq for XOREncoder {}

fn save_bitwriter_to_rdb(rdb: *mut raw::RedisModuleIO, writer: &BitWriter<Vec<u8>, BigEndian>) {
    let bytes = &writer.writer;
    raw::save_slice(rdb, &bytes);

    // this ridiculous hack is necessary because the bitqueue is private
    let value_clone = writer.bit_queue.clone();
    let bits_clone = writer.bit_queue.clone();

    // compress bitqueue value and bits into a u64
    let value = value_clone.value();
    let bits = bits_clone.len();

    let encoded_bitqueue = (value as u64) << 8 | (bits as u64);

    raw::save_unsigned(rdb, encoded_bitqueue);
}

fn load_bitwriter_from_rdb(rdb: *mut raw::RedisModuleIO) -> Result<BitWriter<Vec<u8>, BigEndian>, ValkeyError> {
    // the load_string_buffer does not return an Err, so we can unwrap
    let bytes = raw::load_string_buffer(rdb)?
        .as_ref()
        .to_vec();

    let encoded_queue = raw::load_unsigned(rdb)?;

    let value = (encoded_queue & 0xFF) as u8;
    let bits = (encoded_queue >> 8) as u32;

    let mut writer = BitWriter::endian(bytes, BigEndian);
    writer.bit_queue = BitQueue::from_value(value, bits);

    Ok(writer)
}

fn bitwriter_equals(a: &BitWriter<Vec<u8>, BigEndian>, b: &BitWriter<Vec<u8>, BigEndian>) -> bool {
    if a.writer != b.writer {
        return false;
    }
    // ridiculous, but necessary because the bitqueue is private
    let a_value = a.bit_queue.clone().value();
    let b_value = b.bit_queue.clone().value();
    if a_value != b_value {
        return false;
    }
    let a_bits = a.bit_queue.clone().len();
    let b_bits = b.bit_queue.clone().len();
    if a_bits != b_bits {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use crate::common::types::Sample;
    use rand::{Rng, SeedableRng};

    fn generate_random_test_data(seed: u64) -> Vec<Vec<Sample>> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        let mut test_cases = Vec::with_capacity(128);
        for _ in 0..128 {
            let mut timestamp: i64 = rng.gen_range(1234567890..1357908642);
            let vec_size = rng.gen_range(1..129);
            let mut vec = Vec::with_capacity(vec_size);

            let mut value: f64 = if rng.gen_bool(0.5) {
                rng.gen_range(-100000000.0..1000000.0)
            } else {
                rng.gen_range(-10000.0..10000.0)
            };
            vec.push(Sample { timestamp, value });

            for _ in 1..vec_size {
                timestamp += rng.gen_range(1..30);
                if rng.gen_bool(0.33) {
                    value += 1.0;
                } else if rng.gen_bool(0.33) {
                    value = rng.gen();
                }
                vec.push(Sample { timestamp, value });
            }
            test_cases.push(vec);
        }
        test_cases
    }
}