use std::error::Error;
use std::fmt::Display;
use std::mem::size_of;
use std::str::FromStr;

use get_size::GetSize;
use metricsql_encoding::encoders::{
    float::decode as gorilla_decompress,
    timestamp::decode as timestamp_decompress,
    timestamp::encode as timestamp_compress,
};
use metricsql_encoding::marshal::{marshal_var_i64, unmarshal_var_i64};
use metricsql_runtime::Timestamp;
use pco::ChunkConfig;
use pco::standalone::{simple_compress, simple_decompress_into};
use rand_distr::num_traits::Zero;
use serde::{Deserialize, Serialize};
use tsz::{DataPoint, Decode, Encode, StdDecoder, StdEncoder};
use tsz::decode::Error::EndOfStream;
use tsz::stream::{BufferedReader, BufferedWriter};

use crate::error::{TsdbError, TsdbResult};

/// Rough estimate for when we have insufficient data to calculate
const BASE_COMPRESSION_RATE: f64 = 0.35;
const OVERFLOW_THRESHOLD: f64 = 0.2;

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[derive(GetSize)]
pub enum ValueEncoding {
    Pco = 0x01,
    Gorilla = 0x02,
}

impl ValueEncoding {
    pub fn name(&self) -> &'static str {
        match self {
            ValueEncoding::Pco => "pco",
            ValueEncoding::Gorilla => "gorilla",
        }
    }
}

impl Display for ValueEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl TryFrom<&str> for ValueEncoding {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            value if value.eq_ignore_ascii_case("pco") => Ok(ValueEncoding::Pco),
            value if value.eq_ignore_ascii_case("gorilla") => Ok(ValueEncoding::Gorilla),
            _ => Err("Invalid CompressionType value".into()),
        }
    }
}

impl TryFrom<u8> for ValueEncoding {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let encoding = value >> 4;
        match encoding {
            0x1 => Ok(ValueEncoding::Pco),
            0x2 => Ok(ValueEncoding::Gorilla),
            _ => Err("Invalid CompressionType value".into()),
        }
    }
}

impl Default for ValueEncoding {
    fn default() -> Self {
        ValueEncoding::Pco
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[derive(GetSize)]
pub enum TimestampEncoding {
    Basic = 0x01,
    Pco = 0x02,
}

impl TimestampEncoding {
    pub fn name(&self) -> &'static str {
        match self {
            TimestampEncoding::Basic => "basic",
            TimestampEncoding::Pco => "pco",
        }
    }
}

impl Default for TimestampEncoding {
    fn default() -> Self {
        TimestampEncoding::Basic
    }
}

impl FromStr for TimestampEncoding {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("basic") => Ok(TimestampEncoding::Basic),
            s if s.eq_ignore_ascii_case("pco") => Ok(TimestampEncoding::Pco),
            _ => Err("Invalid TimestampEncoding value".into()),
        }
    }
}

impl Display for TimestampEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[derive(GetSize)]
#[non_exhaustive]
pub enum CompressionOptimization {
    #[default]
    Speed,
    Size,
}

impl Display for CompressionOptimization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionOptimization::Speed => write!(f, "speed"),
            CompressionOptimization::Size => write!(f, "size"),
        }
    }
}

impl TryFrom<&str> for CompressionOptimization {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            value if value.eq_ignore_ascii_case("speed") => Ok(CompressionOptimization::Speed),
            value if value.eq_ignore_ascii_case("size") => Ok(CompressionOptimization::Size),
            _ => Err("Invalid CompressionOptimization value".into()),
        }
    }
}


#[derive(GetSize, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressionOptions {
    pub value_encoding: ValueEncoding,
    pub timestamp_encoding: TimestampEncoding,
    pub optimization: CompressionOptimization,
    pub delta_encoding_order: Option<usize>,
}

impl Default for CompressionOptions {
    fn default() -> Self {
        CompressionOptions {
            value_encoding: ValueEncoding::default(),
            timestamp_encoding: TimestampEncoding::default(),
            optimization: CompressionOptimization::default(),
            delta_encoding_order: None,
        }
    }
}

// todo: make this configurable
const DEFAULT_COMPRESSION_OPTIMIZATION: CompressionOptimization = CompressionOptimization::Size;
/// items above this count will cause value and timestamp encoding/decoding to happen in parallel
pub(super) const COMPRESSION_PARALLELIZATION_THRESHOLD: usize = 1024;

pub(super) const DEFAULT_DATA_PAGE_SIZE: usize = 2048;


pub(super) fn get_segment_meta(buf: &[u8]) -> TsdbResult<(Timestamp, Timestamp, usize, usize)> {
    let mut compressed = buf;
    let mut first_timestamp: Option<Timestamp> = None;
    let mut last_timestamp: Option<Timestamp> = None;
    let mut count = 0;
    let mut first = true;
    let mut last_segment_offset: usize = 0;

    while !compressed.is_empty() {
        if let Some((start_ts, end_ts)) = read_date_range(&mut compressed)? {
            let save_buf = compressed;
            let data_size = read_usize(&mut compressed, "data segment length")?;
            let sample_count = read_usize(&mut compressed, "sample count")?;

            if first {
                first_timestamp = Some(start_ts);
                first = false;
            }
            last_timestamp = Some(end_ts);

            last_segment_offset = save_buf.as_ptr() as usize - buf.as_ptr() as usize;
            count += sample_count;

            compressed = &save_buf[data_size..];
        }
    }
    let first_ts = first_timestamp.ok_or(TsdbError::CannotDeserialize("no data segments found".to_string()))?;
    let last_ts = last_timestamp.ok_or(TsdbError::CannotDeserialize("no data segments found".to_string()))?;

    Ok((first_ts, last_ts, count, last_segment_offset))
}

pub(super) fn write_data_segment(
    dest: &mut Vec<u8>,
    timestamps: &[i64],
    values: &[f64],
    config: &CompressionOptions,
) -> TsdbResult<usize> {
    // Each page consists of
    // 1. timestamp min and max (for fast decompression filtering)
    // 2. Total data size (timestamps and values). Allows for fast seeking by date range.
    // 3. sample count
    // 4. compressed timestamp data
    // 5. compressed values page

    let page_size = timestamps.len();
    let len = timestamps.len();
    // 1.
    let t_min = timestamps[0];
    let t_max = timestamps[len - 1];
    write_timestamp(dest, t_min);
    write_timestamp(dest, t_max);

    // add placeholder for total data size
    let placeholder_offset = dest.len();

    write_usize(dest, 0);
    let data_size_start_offset = dest.len();

    // 3.
    write_usize(dest, page_size);

    // 4
    write_timestamp_data(dest, timestamps, &config)?;
    // 5
    write_value_data(dest, values, &config)?;

    // 2
    // patch in the data size
    let data_size = dest.len() - data_size_start_offset;

    write_usize_in_place(dest, placeholder_offset, data_size);

    Ok(data_size)
}

pub(super) fn read_date_range(compressed: &mut &[u8]) -> TsdbResult<Option<(i64, i64)>> {
    if (*compressed).is_empty() {
        return Ok(None);
    }
    let first_ts = read_timestamp(compressed)?;
    let last_ts = read_timestamp(compressed)?;
    Ok(Some((first_ts, last_ts)))
}


pub(super) struct BlockMeta<'a> {
    pub start_ts: i64,
    pub end_ts: i64,
    pub count: usize,
    pub offset: usize,
    pub data_size: usize,
    pub start: &'a [u8],
}

pub fn find_data_page<'a>(compressed: &'a mut &[u8], timestamp: i64) -> TsdbResult<BlockMeta<'a>> {
    let mut found = false;
    let mut block_start = *compressed;
    let mut compressed = *compressed;
    let mut count: usize = 0;
    let mut start_timestamp: i64 = 0;
    let mut end_timestamp: i64 = 0;
    let mut data_size: usize = 0;

    while !compressed.is_empty() {
        if let Some((start_ts, end_ts)) = read_date_range(&mut compressed)? {
            if timestamp >= start_ts {
                start_timestamp = start_ts;
                end_timestamp = end_ts;
                found = timestamp <= end_ts;
                break;
            }
            if timestamp > end_ts {
                start_timestamp = start_ts;
                end_timestamp = end_ts;
                block_start = compressed;
                break;
            }


            data_size = read_usize(&mut compressed, "data segment length")?;
            let saved = &compressed[data_size..];
            // we want to return sample count, but also the start of the block that contains the timestamp
            count = read_usize(&mut compressed, "sample count")?;
            compressed = saved;
        } else {
            break;
        }
    }

    let meta = BlockMeta {
        start_ts: start_timestamp,
        end_ts: end_timestamp,
        count,
        offset: block_start.as_ptr() as usize - compressed.as_ptr() as usize,
        start: block_start,
        data_size
    };

    Ok(meta)
}


pub(super) fn read_data_segment<'a>(
    compressed: &mut &'a [u8],
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
) -> TsdbResult<usize> {
    if compressed.is_empty() {
        return Ok(0);
    }

    // size of data segment (timestamps and values)
    let segment_length = read_usize(compressed, "data segment length")?;
    if segment_length.is_zero() {
        return Ok(0);
    }

    let sample_count = read_usize(compressed, "sample count")?;

    if sample_count == 0 {
        return Ok(0);
    }

    let ofs = timestamps.len();
    let new_size = ofs + sample_count;

    timestamps.resize(new_size,  0);
    values.resize(new_size, 0.0);

    let ts_count = read_timestamp_page(compressed, timestamps)?;
    let value_count= read_values_page(compressed, values)?;

    if ts_count != value_count {
        return Err(TsdbError::CannotDeserialize("incomplete/corrupt data page".to_string()));
    }

    Ok(sample_count)
}


fn write_value_data(
    dest: &mut Vec<u8>,
    values: &[f64],
    config: &CompressionOptions,
) -> TsdbResult<usize> {
    let start = dest.len();
    write_usize(dest, 0);
    compress_values(dest, values, config)?;
    let bytes_written = dest.len() - start;

    write_usize_in_place(dest, start, bytes_written);

    Ok(bytes_written)
}

fn write_timestamp_data(
    dest: &mut Vec<u8>,
    values: &[Timestamp],
    config: &CompressionOptions,
) -> TsdbResult<usize> {
    let start = dest.len();
    write_usize(dest, 0);
    compress_timestamps(dest, values, config)?;
    let bytes_written = dest.len() - start;

    write_usize_in_place(dest, start, bytes_written);

    Ok(bytes_written)
}

fn read_timestamp_page<'a>(
    compressed: &mut &'a [u8],
    dst: &mut Vec<i64>,
) -> TsdbResult<usize> {
    let size = read_usize(compressed, "timestamp data size")?;
    let start = dst.len();
    decompress_timestamps(compressed, dst)?;

    *compressed = &compressed[size..];
    let count = dst.len() - start;
    Ok(count)
}

pub(super) fn read_values_page<'a>(
    compressed: &mut &'a [u8],
    dst: &mut Vec<f64>,
) -> TsdbResult<usize> {
    let size= read_usize(compressed, "value data size")?;
    let len = dst.len();
    decompress_values(compressed, dst)?;

    *compressed = &compressed[size..];
    let count = dst.len() - len;

    Ok(count)
}

pub(crate) fn compress_data(
    buf: &mut Vec<u8>,
    timestamps: &[Timestamp],
    values: &[f64],
    page_size: Option<usize>,
    options: &CompressionOptions
) -> TsdbResult<()> {

    let page_size = page_size.unwrap_or(DEFAULT_DATA_PAGE_SIZE);

    let mut count = timestamps.len();

    // todo: if the last segment has less than a minimum number of samples, we should merge it with the previous one
    let remainder = count % page_size;
    // if remainder is less than 20% of the page size, merge it with the previous page
    let threshold = (page_size as f64 * OVERFLOW_THRESHOLD).ceil() as usize;


    // todo: use rayon if the number of samples is large
    // let page_count = count / page_size + 1;

    // write out value chunk metadata
    let mut value_offset = 0;
    while count > 0 {
        let page_size = count.min(page_size);
        if page_size == 0 {
            break;
        }
        count -= page_size;

        let end_offset = value_offset + if count < threshold {
            count = 0;
            page_size + remainder
        } else {
            page_size
        };
        let ts_slice = &timestamps[value_offset..end_offset];
        let value_slice = &values[value_offset..end_offset];
        write_data_segment(buf, ts_slice, value_slice, options)?;

        value_offset += page_size;
    }

    Ok(())
}

pub(super) fn read_timestamp(compressed: &mut &[u8]) -> TsdbResult<i64> {
    let (value, remaining) = unmarshal_var_i64(compressed)
        .map_err(|e| TsdbError::CannotDeserialize(format!("timestamp: {}", e.to_string())))?;
    *compressed = remaining;
    Ok(value)
}

pub(super) fn write_timestamp(dest: &mut Vec<u8>, ts: i64) {
    marshal_var_i64(dest, ts);
}

pub(super) fn write_usize(slice: &mut Vec<u8>, size: usize) {
    slice.extend_from_slice(&size.to_le_bytes());
}

pub(super) fn write_usize_in_place(vec: &mut Vec<u8>, index: usize, value: usize) {
    let bytes = value.to_le_bytes();
    let end = index + bytes.len();
    if end > vec.len() {
        panic!("Index out of bounds");
    }
    vec[index..end].copy_from_slice(&bytes);
}

pub(super) fn read_usize<'a>(input: &mut &'a [u8], field: &str) -> TsdbResult<usize> {
    let (int_bytes, rest) = input.split_at(size_of::<usize>());
    let buf = int_bytes
        .try_into()
        .map_err(|_| TsdbError::CannotDeserialize(format!("invalid usize reading {}", field).to_string()))?;

    *input = rest;
    Ok(usize::from_le_bytes(buf))
}


fn get_pco_compressor_config(options: &CompressionOptions, for_timestamps: bool) -> ChunkConfig {
    let mut res = ChunkConfig::default();
    res.compression_level = match options.optimization {
        CompressionOptimization::Speed => 4,
        CompressionOptimization::Size => 8,
    };
    if for_timestamps {
        res.delta_encoding_order = options.delta_encoding_order;
    }
    res
}

#[inline]
fn encode_timestamp_compression_type(compression_type: TimestampEncoding) -> u8 {
    // first 4 high bits used for encoding type
    (compression_type as u8) << 4
}

#[inline]
fn encode_value_compression_type(compression_type: ValueEncoding) -> u8 {
    // first 4 high bits used for encoding type
    (compression_type as u8) << 4
}

#[inline]
fn write_value_compression_type(dst: &mut Vec<u8>, compression_type: ValueEncoding) {
    // first 4 high bits used for encoding type
    dst.push(encode_value_compression_type(compression_type));
}

#[inline]
fn write_timestamp_compression_type(dst: &mut Vec<u8>, compression_type: TimestampEncoding) {
    // first 4 high bits used for encoding type
    dst.push(encode_timestamp_compression_type(compression_type));
}

pub fn compress_timestamps(
    dst: &mut Vec<u8>,
    src: &[i64],
    config: &CompressionOptions,
) -> TsdbResult<()> {
    fn handle_err(_e: Box<dyn Error>) -> TsdbError {
        TsdbError::CannotSerialize("timestamps".to_string())
    }

    match config.timestamp_encoding {
        TimestampEncoding::Basic => {
            write_timestamp_compression_type(dst, TimestampEncoding::Basic);
            timestamp_compress(src, dst).map_err(handle_err)?;
        }
        TimestampEncoding::Pco => {
            write_timestamp_compression_type(dst, TimestampEncoding::Pco);
            let compressor_config = get_pco_compressor_config(config, true);
            let vec = simple_compress(src, &compressor_config)
                .map_err(|_e| {
                    //
                    TsdbError::CannotSerialize("timestamps".to_string())
                })?;
            dst.extend_from_slice(&vec);
        }
    }

    Ok(())
}

pub fn decompress_timestamps(src: &[u8], dst: &mut Vec<i64>) -> TsdbResult<()> {
    fn handle_err(_e: Box<dyn Error>) -> TsdbError {
        let msg = _e.to_string();
        TsdbError::CannotDeserialize(msg)
    }

    if src.is_empty() {
        return Ok(());
    }

    let encoding = &src[0] >> 4;
    match encoding {
        encoding if encoding == TimestampEncoding::Basic as u8 => {
            timestamp_decompress(&src[1..], dst).map_err(handle_err)
        }
        encoding if encoding == TimestampEncoding::Pco as u8 => {
            let progress = simple_decompress_into(&src[1..], dst)
                .map_err(|e| TsdbError::CannotDeserialize(e.to_string()))?;
            if !progress.finished {
                Err(TsdbError::CannotDeserialize("incomplete timestamp data".to_string()))
            } else {
                Ok(())
            }
        },
        _ => Err(TsdbError::CannotDeserialize("unknown compression type decoding timestamps".to_string())),
    }
}

pub fn compress_values(
    dst: &mut Vec<u8>,
    src: &[f64],
    option: &CompressionOptions,
) -> TsdbResult<()> {

    fn handle_err(_e: Box<dyn Error>) -> TsdbError {
        TsdbError::CannotSerialize("values".to_string())
    }

    let compressor_config = get_pco_compressor_config(&option, false);
    write_value_compression_type(dst, ValueEncoding::Pco);
    let vec = simple_compress(src, &compressor_config)
        .map_err(|e|
            TsdbError::CannotSerialize(e.to_string())
        )?;
    dst.extend_from_slice(&vec);

    Ok(())
}

pub fn decompress_values(src: &[u8], dst: &mut Vec<f64>) -> TsdbResult<()> {
    if src.is_empty() {
        return Ok(());
    }

    fn handle_err(_e: Box<dyn Error>) -> TsdbError {
        TsdbError::CannotDeserialize("values".to_string())
    }

    let encoding = &src[0] >> 4;
    match encoding {
        encoding if encoding == ValueEncoding::Gorilla as u8 => {
            gorilla_decompress(&src[1..], dst).map_err(handle_err)?
        }
        encoding if encoding == ValueEncoding::Pco as u8 => {
            simple_decompress_into(&src[1..], dst)
                .map_err(|_| TsdbError::CannotDeserialize("Error decompressing data".to_string()))?;
        }
        _ => {
            return Err(TsdbError::CannotDeserialize(
                "unknown compression type decoding values".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn gorilla_compress(dst: &mut Vec<u8>, timestamps: &[Timestamp], values: &[f64]) -> TsdbResult<()> {
    let w = BufferedWriter::new();
    // 1482892260 is the Unix timestamp of the start of the stream
    let first_ts = timestamps[0];
    let mut encoder = StdEncoder::new(first_ts as u64, w);
    for (ts, value) in timestamps.iter().zip(values.iter()) {
        encoder.encode(DataPoint::new(*ts as u64, *value));
    }
    let res = encoder.close();
    Ok(())
}

pub fn gorilla_decompress_(src: &[u8], timestamps: &mut Vec<i64>, values: &mut Vec<f64>) -> TsdbResult<()> {
    let r = BufferedReader::new(src.into());
    let mut decoder = StdDecoder::new(r);

    let mut done = false;
    loop {
        if done {
            break;
        }

        match decoder.next() {
            Ok(dp) => {
                timestamps.push(dp.get_time() as i64);
                values.push(dp.get_value());
            },
            Err(err) => {
                if err == EndOfStream {
                    done = true;
                } else {
                    panic!("Received an error from decoder: {:?}", err);
                }
            }
        };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    // Single series
}
