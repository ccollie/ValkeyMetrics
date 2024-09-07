use std::mem::size_of;
use crate::common::current_time_millis;
use crate::common::types::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::gorilla::decoder::{Decode, Error, StdDecoder};
use crate::gorilla::encoder::{Encode, StdEncoder};
use crate::gorilla::stream::{BufferedReader, BufferedWriter};
use crate::gorilla::DataPoint;
use crate::storage::chunk::Chunk;
use crate::storage::utils::trim_vec_data;
use crate::storage::{DuplicatePolicy, Sample, DEFAULT_CHUNK_SIZE_BYTES};
use get_size::GetSize;
use metricsql_common::pool::{get_pooled_vec_f64, get_pooled_vec_i64};
use valkey_module::raw;

static F64_SIZE: usize = size_of::<f64>();
static I64_SIZE: usize = size_of::<i64>();
static SAMPLE_SIZE: usize = F64_SIZE + I64_SIZE;

pub(crate) type ChunkEncoder = StdEncoder<BufferedWriter>;

/// `GorillaChunk` holds information about location and time range of a block of compressed data.
#[derive(Debug, Clone, PartialEq)]
#[derive(GetSize)]
pub struct GorillaChunk {
    encoder: ChunkEncoder,
    first_timestamp: Timestamp,
    last_timestamp: Timestamp,
    last_value: f64,
    pub max_size: usize,
}

impl Default for GorillaChunk {
    fn default() -> Self {
        Self::with_max_size(DEFAULT_CHUNK_SIZE_BYTES)
    }
}

impl GorillaChunk {
    pub fn with_max_size(max_size: usize) -> Self {
        let now = current_time_millis();
        let encoder = create_encoder(now, Some(max_size));
        Self {
            encoder,
            first_timestamp: now,
            last_timestamp: now,
            last_value: f64::NAN,
            max_size,
        }
    }

    pub fn with_values(
        max_size: usize,
        timestamps: &[Timestamp],
        values: &[f64],
    ) -> TsdbResult<Self> {
        debug_assert_eq!(timestamps.len(), values.len());
        let mut res = Self::default();
        res.max_size = max_size;
        res.compress(timestamps, values)?;
        Ok(res)
    }

    pub fn len(&self) -> usize {
        self.num_samples()
    }

    pub fn is_empty(&self) -> bool {
        self.num_samples() == 0
    }

    pub fn is_full(&self) -> bool {
        let usage = self.encoder.w.usage();
        usage >= self.max_size
    }

    pub fn clear(&mut self) {
        self.encoder.clear();
        self.first_timestamp = 0;
        self.last_timestamp = 0;
        self.last_value = f64::NAN;
    }

    pub fn set_data(&mut self, timestamps: &[i64], values: &[f64]) -> TsdbResult<()> {
        debug_assert_eq!(timestamps.len(), values.len());
        self.compress(timestamps, values)?;
        // todo: complain if size > max_size
        Ok(())
    }

    pub(super) fn compress(&mut self, timestamps: &[Timestamp], values: &[f64]) -> TsdbResult<()> {
        debug_assert_eq!(timestamps.len(), values.len());
        let mut encoder = create_encoder(timestamps[0], Some(self.max_size));
        for (ts, value) in timestamps.iter().zip(values.iter()) {
            encoder.encode(DataPoint::new(*ts as u64, *value));
        }
        self.encoder = encoder;
        Ok(())
    }

    pub(super) fn decompress(
        &self,
        timestamps: &mut Vec<Timestamp>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        if self.is_empty() {
            return Ok(());
        }
        timestamps.reserve(self.num_samples());
        values.reserve(self.num_samples());
        for sample in self.iter() {
            timestamps.push(sample.timestamp);
            values.push(sample.value);
        }
        Ok(())
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = self.encoder.w.len();
        let uncompressed_size = self.num_samples() * (I64_SIZE + F64_SIZE);
        (uncompressed_size / compressed_size) as f64
    }

    pub(crate) fn process_range<F, State, R>(
        &self,
        start: Timestamp,
        end: Timestamp,
        state: &mut State,
        mut f: F,
    ) -> TsdbResult<R>
    where
        F: FnMut(&mut State, &[i64], &[f64]) -> TsdbResult<R>,
    {
        let mut handle_empty = |state: &mut State| -> TsdbResult<R> {
            let mut timestamps = vec![];
            let mut values = vec![];
            f(state, &mut timestamps, &mut values)
        };

        if self.is_empty() {
            return handle_empty(state);
        }

        let count = self.num_samples();
        let mut timestamps = get_pooled_vec_i64(count);
        let mut values = get_pooled_vec_f64(count);
        self.decompress(&mut timestamps, &mut values)?;
        // special case of range exceeding block range
        if start <= self.first_timestamp() && end >= self.last_timestamp() {
            return f(state, &timestamps, &values);
        }

        trim_vec_data(&mut timestamps, &mut values, start, end);
        f(state, &timestamps, &values)
    }

    pub fn data_size(&self) -> usize {
        self.encoder.get_size()
    }

    pub fn bytes_per_sample(&self) -> usize {
        let count = self.num_samples();
        if  count == 0 {
            return 0;
        }
        self.data_size() / count
    }

    /// estimate remaining capacity based on the current data size and chunk max_size
    pub fn remaining_capacity(&self) -> usize {
        self.max_size - self.encoder.w.usage()
    }

    /// Estimate the number of samples that can be stored in the remaining capacity
    /// Note that for low sample counts this will be very inaccurate
    pub fn remaining_samples(&self) -> usize {
        if self.num_samples() == 0 {
            return 0;
        }
        self.remaining_capacity() / self.bytes_per_sample()
    }

    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() +
        self.get_heap_size()
    }

    pub fn iter(&self) -> impl Iterator<Item = Sample> + '_ {
        ChunkIter::new(self)
    }

    fn buf(&self) -> &[u8] {
        self.encoder.w.bytes()
    }

    fn rdb_load_encoded(rdb: *mut raw::RedisModuleIO) -> Result<ChunkEncoder, valkey_module::error::Error> {
        let mut encoder = ChunkEncoder {
            time: raw::load_unsigned(rdb)?,
            delta: raw::load_unsigned(rdb)?,
            value_bits: raw::load_unsigned(rdb)?,
            val: raw::load_double(rdb)?,
            leading_zeroes: raw::load_unsigned(rdb)? as u32,
            trailing_zeroes: raw::load_unsigned(rdb)? as u32,
            first: raw::load_unsigned(rdb)? == 1,
            count: raw::load_unsigned(rdb)? as usize,
            w: BufferedWriter::new(),
        };
        let temp = raw::load_string_buffer(rdb)?;
        let buf: Vec<u8> = Vec::from(temp.as_ref());
        let pos = raw::load_unsigned(rdb)? as u32;
        encoder.w.buf = buf;
        encoder.w.pos = pos;
        Ok(encoder)
    }

    fn rdb_save_encoded(&self, rdb: *mut raw::RedisModuleIO) {
        let encoder = &self.encoder;

        raw::save_unsigned(rdb, encoder.time);
        raw::save_unsigned(rdb, encoder.delta);
        raw::save_unsigned(rdb, encoder.value_bits);
        raw::save_double(rdb, encoder.val);

        raw::save_unsigned(rdb, encoder.leading_zeroes as u64);
        raw::save_unsigned(rdb, encoder.trailing_zeroes as u64);
        raw::save_unsigned(rdb, if encoder.first {
            1
        } else {
            0
        });
        raw::save_unsigned(rdb, encoder.count as u64);

        raw::save_slice(rdb, &encoder.w.buf);
        raw::save_unsigned(rdb, encoder.w.pos as u64);
    }
}

impl Chunk for GorillaChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.first_timestamp
    }
    fn last_timestamp(&self) -> Timestamp {
        self.encoder.time as i64
    }
    fn num_samples(&self) -> usize {
        self.encoder.count
    }
    fn last_value(&self) -> f64 {
        self.encoder.val
    }
    fn size(&self) -> usize {
        self.data_size()
    }
    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        if self.is_empty() {
            return Ok(0);
        }

        let mut ts = self.first_timestamp();
        if start_ts <= self.first_timestamp() {
            if end_ts >= self.last_timestamp() {
                self.clear();
                return Ok(0);
            } else {
                ts = end_ts;
            }
        }

        let mut encoder: ChunkEncoder = create_encoder(ts, Some(self.max_size));

        let mut iter = self.iter();

        for sample in iter.by_ref() {
            if sample.timestamp < start_ts {
                encoder.encode(DataPoint::new(sample.timestamp as u64, sample.value));
            } else {
                break;
            }
        }

        for sample in iter.by_ref() {
            if sample.timestamp >= end_ts {
                break;
            }
        }

        for sample in iter.by_ref() {
            encoder.encode(DataPoint::new(sample.timestamp as u64, sample.value));
        }

        drop(iter);

        let old_count = self.encoder.count;

        // todo: ensure first_timestamp and last_timestamp are updated
        self.encoder = encoder;
        Ok(self.num_samples() - old_count)
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_full() {
            return Err(TsdbError::CapacityFull(self.max_size));
        }
        self.encoder.encode(DataPoint::new(sample.timestamp as u64, sample.value));

        if sample.timestamp >= self.last_timestamp {
            self.last_value = sample.value;
            self.last_timestamp = sample.timestamp;
        }
        self.first_timestamp = self.first_timestamp.min(sample.timestamp);

        Ok(())
    }

    fn get_range(
        &self,
        start: Timestamp,
        end: Timestamp,
        timestamps: &mut Vec<Timestamp>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        if self.is_empty() {
            return Ok(());
        }

        for sample in self.iter() {
            if sample.timestamp >= start {
                timestamps.push(sample.timestamp);
                values.push(sample.value);
            }
            if sample.timestamp > end {
                break;
            }
        }
        Ok(())
    }

    fn upsert_sample(
        &mut self,
        sample: &mut Sample,
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;

        if self.is_empty() {
            self.add_sample(sample)?;
            return Ok(1)
        }

        let count = self.num_samples();
        let mut encoder: ChunkEncoder = create_encoder(ts, Some(self.max_size));

        for sample in self.iter() {
            if sample.timestamp == ts {
                duplicate_found = true;
                let value = dp_policy.value_on_duplicate(ts, sample.value, sample.value)?;
                encoder.encode(DataPoint::new(sample.timestamp as u64, value));
            } else {
                encoder.encode(DataPoint::new(sample.timestamp as u64, sample.value));
            }
        }

        // todo: do a self.encoder.buf.take()
        self.encoder = encoder;
        let size = if duplicate_found { count } else { count + 1 };
        Ok(size)
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let mut left_chunk = create_encoder(self.first_timestamp, Some(self.max_size));
        let mut right_chunk = GorillaChunk::default();

        if self.is_empty() {
            return Ok(self.clone());
        }

        let mid = self.num_samples() / 2;
        for (i, sample) in self.iter().enumerate() {
            if i < mid {
                // todo: handle min and max timestamps
                left_chunk.encode(DataPoint::new(sample.timestamp as u64, sample.value));
            } else {
                right_chunk.add_sample(&sample)?;
            }
        }
        self.encoder = left_chunk;

        Ok(right_chunk)
    }

    fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        raw::save_unsigned(rdb, self.max_size as u64);
        raw::save_signed(rdb, self.first_timestamp);
        raw::save_signed(rdb, self.last_timestamp);
        raw::save_double(rdb, self.last_value);
        self.rdb_save_encoded(rdb);
    }

    fn rdb_load(rdb: *mut raw::RedisModuleIO) -> Result<Self, valkey_module::error::Error> {
        let max_size = raw::load_unsigned(rdb)? as usize;
        let first_timestamp = raw::load_signed(rdb)?;
        let last_timestamp = raw::load_signed(rdb)?;
        let last_value = raw::load_double(rdb)?;
        let encoder = Self::rdb_load_encoded(rdb)?;
        let chunk = GorillaChunk {
            encoder,
            first_timestamp,
            last_timestamp,
            last_value,
            max_size,
        };
        Ok(chunk)
    }
}

fn create_encoder(ts: Timestamp, cap: Option<usize>) -> ChunkEncoder {
    let writer = if let Some(cap) = cap {
        BufferedWriter::with_capacity(cap)
    } else {
        BufferedWriter::new()
    };
    ChunkEncoder::new(ts as u64, writer)
}

pub(crate) struct ChunkIter<'a> {
    decoder: StdDecoder<BufferedReader<'a>>,
}

impl<'a> ChunkIter<'a> {
    pub fn new(chunk: &'a GorillaChunk) -> Self {
        let buf = chunk.buf();
        let reader = BufferedReader::new(buf);
        let decoder = StdDecoder::new(reader);
        Self { decoder }
    }
}
impl<'a> Iterator for ChunkIter<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        match self.decoder.next() {
            Ok(dp) => Some(Self::Item {
                timestamp: dp.get_time() as i64,
                value: dp.get_value(),
            }),
            Err(Error::EndOfStream) => None,
          //  Err(Error::Stream(crate::gorilla::stream::Error::EOF)) => None, // is this an error ?
            Err(err) => {
                #[cfg(debug_assertions)]
                eprintln!("Error decoding sample: {:?}", err);
                None
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::time::Duration;

    use crate::error::TsdbError;
    use crate::storage::chunk::Chunk;
    use crate::storage::gorilla_chunk::GorillaChunk;
    use crate::storage::series_data::SeriesData;
    use crate::storage::{DuplicatePolicy, Sample};
    use crate::tests::generators::{create_rng, generate_series_data, generate_timestamps, GeneratorOptions, RandAlgo};

    fn decompress(chunk: &GorillaChunk) -> SeriesData {
        let mut timestamps = vec![];
        let mut values = vec![];
        chunk.decompress(&mut timestamps, &mut values).unwrap();
        SeriesData::new_with_data(timestamps, values)
    }

    pub(crate) fn saturate_compressed_chunk(chunk: &mut GorillaChunk) {
        let mut rng = create_rng(None).unwrap();
        let mut ts: i64 = 1;
        loop {
            let sample = Sample {
                timestamp: ts,
                value: rng.gen_range(0.0..100.0),
            };
            ts += rng.gen_range(1000..20000);
            match chunk.add_sample(&sample) {
                Ok(_) => {}
                Err(TsdbError::CapacityFull(_)) => {
                    break
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        }
    }

    fn populate_series_data(chunk: &mut GorillaChunk, samples: usize) {
        let mut options = GeneratorOptions::default();
        options.samples = samples;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();
    }

    fn compare_chunks(chunk1: &GorillaChunk, chunk2: &GorillaChunk) {
        assert_eq!(chunk1.encoder, chunk2.encoder, "xor chunks do not match");
        assert_eq!(chunk1.max_size, chunk2.max_size);
    }

    #[test]
    fn test_chunk_compress() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let mut options = GeneratorOptions::default();
        options.samples = 1000;
        options.range = 0.0..100.0;
        options.typ = RandAlgo::MackeyGlass;
  //    options.significant_digits = Some(8);
        let data = generate_series_data(&options).unwrap();
        for sample in data.iter() {
            chunk.add_sample(&sample).unwrap();
        }
        assert_eq!(chunk.num_samples(), data.len());
        assert_eq!(chunk.first_timestamp(), data.timestamps[0]);
        assert_eq!(chunk.last_timestamp(), data.timestamps[data.len() - 1]);
        assert_eq!(chunk.last_value(), data.values[data.len() - 1]);
    }

    #[test]
    fn test_compress_decompress() {
        let mut chunk = GorillaChunk::default();
        let timestamps = generate_timestamps(1000, 1000, Duration::from_secs(5));
        let values = timestamps.iter().map(|x| *x as f64).collect::<Vec<f64>>();

        chunk.set_data(&timestamps, &values).unwrap();
        let mut timestamps2 = vec![];
        let mut values2 = vec![];
        chunk.decompress(&mut timestamps2, &mut values2).unwrap();
        assert_eq!(timestamps, timestamps2);
        assert_eq!(values, values2);
    }

    #[test]
    fn test_clear() {
        let mut chunk = GorillaChunk::default();
        let mut options = GeneratorOptions::default();
        options.samples = 500;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();
        assert_eq!(chunk.num_samples(), data.len());
        chunk.clear();
        assert_eq!(chunk.num_samples(), 0);
        assert_eq!(chunk.first_timestamp(), 0);
        assert_eq!(chunk.last_timestamp(), 0);
    }

    #[test]
    fn test_upsert() {
        for chunk_size in (64..8192).step_by(64) {
            let mut options = GeneratorOptions::default();
            options.samples = 500;
            let data = generate_series_data(&options).unwrap();
            let mut chunk = GorillaChunk::with_max_size(chunk_size);

            for mut sample in data.iter() {
                chunk.upsert_sample(&mut sample, DuplicatePolicy::KeepLast).unwrap();
            }
            assert_eq!(chunk.num_samples(), data.len());
        }
    }

    #[test]
    fn test_upsert_while_at_capacity() {
        let mut chunk = GorillaChunk::with_max_size(4096);
        saturate_compressed_chunk(&mut chunk);

        let timestamp = chunk.last_timestamp();

        // return an error on insert
        let mut sample = Sample {
            timestamp: 0,
            value: 1.0,
        };

        assert!(chunk.upsert_sample(&mut sample, DuplicatePolicy::KeepLast).is_err());

        // should update value for duplicate timestamp
        sample.timestamp = timestamp;
        let res = chunk.upsert_sample(&mut sample, DuplicatePolicy::KeepLast);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }

    #[test]
    fn test_split() {
        let mut chunk = GorillaChunk::default();
        let mut options = GeneratorOptions::default();
        options.samples = 500;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();

        let count = data.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.num_samples(), mid);
        assert_eq!(right.num_samples(), mid);

        let (l_times, r_times) = data.timestamps.split_at(mid);
        let (l_values, r_values) = data.values.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed.timestamps, r_times);
        assert_eq!(right_decompressed.values, r_values);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed.timestamps, l_times);
        assert_eq!(left_decompressed.values, l_values);
    }

    #[test]
    fn test_split_odd() {
        let mut chunk = GorillaChunk::default();
        let mut options = GeneratorOptions::default();
        options.samples = 51;
        let data = generate_series_data(&options).unwrap();
        chunk.set_data(&data.timestamps, &data.values).unwrap();

        let count = data.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.num_samples(), mid);
        assert_eq!(right.num_samples(), mid + 1);

        let (l_times, r_times) = data.timestamps.split_at(mid);
        let (l_values, r_values) = data.values.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed.timestamps, r_times);
        assert_eq!(right_decompressed.values, r_values);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed.timestamps, l_times);
        assert_eq!(left_decompressed.values, l_values);
    }

}