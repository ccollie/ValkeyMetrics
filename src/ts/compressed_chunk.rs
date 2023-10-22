use std::fmt::Display;
use ahash::AHashSet;
use crate::error::{TsdbError, TsdbResult};
use crate::ts::{DEFAULT_CHUNK_SIZE_BYTES, DuplicatePolicy, DuplicateStatus, handle_duplicate_sample};
use metricsql_encoding::encoders::{
    float::decode as gorilla_decompress,
    qcompress::decode as quantile_decompress, qcompress::encode as quantile_compress,
    qcompress::encode_with_options as quantile_compress_with_options,
    timestamp::decode as timestamp_decompress, timestamp::encode as timestamp_compress,
};
use metricsql_common::{get_pooled_vec_f64, get_pooled_vec_i64};
use serde::{Deserialize, Serialize};
use crate::common::types::{Sample, Timestamp};
use crate::ts::chunk::Chunk;
use crate::ts::merge::{DataBlock, merge_into};
use crate::ts::utils::{get_timestamp_index_bounds, trim_vec_data};

type CompressorConfig = metricsql_encoding::encoders::qcompress::CompressorConfig;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CompressionType {
    Basic = 0,
    Quantile = 1,
    Gorilla = 2,
}

impl Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::Basic => write!(f, "basic"),
            CompressionType::Quantile => write!(f, "quantile"),
            CompressionType::Gorilla => write!(f, "gorilla"),
        }
    }
}

impl TryFrom<&str> for CompressionType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "basic" => Ok(CompressionType::Basic),
            "quantile" => Ok(CompressionType::Quantile),
            "gorilla" => Ok(CompressionType::Gorilla),
            _ => Err("Invalid CompressionType value".into()),
        }
    }
}

impl TryFrom<u8> for CompressionType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressionType::Basic),
            1 => Ok(CompressionType::Quantile),
            2 => Ok(CompressionType::Gorilla),
            _ => Err("Invalid CompressionType value".into()),
        }
    }
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::Quantile
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CompressionOptimization {
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
        match value.to_lowercase().as_str() {
            "speed" => Ok(CompressionOptimization::Speed),
            "size" => Ok(CompressionOptimization::Size),
            _ => Err("Invalid CompressionOptimization value".into()),
        }
    }
}

// todo: make this configurable
const DEFAULT_COMPRESSION_OPTIMIZATION: CompressionOptimization = CompressionOptimization::Size;
/// items above this count will cause value and timestamp encoding/decoding to happen in parallel
const COMPRESSION_PARALLELIZATION_THRESHOLD: usize = 64;

/// `CompressedBlock` holds information about location and time range of a block of compressed data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompressedChunk {
    pub min_time: i64,
    pub max_time: i64,
    pub max_size: usize,
    pub last_value: f64,
    pub optimization: Option<CompressionOptimization>,
    /// number of compressed samples
    pub count: usize,
    pub timestamps: Vec<u8>,
    pub values: Vec<u8>,
}

impl Default for CompressedChunk {
    fn default() -> Self {
        Self {
            min_time: 0,
            max_time: i64::MAX,
            max_size: DEFAULT_CHUNK_SIZE_BYTES,
            last_value: 0.0,
            optimization: None,
            count: 0,
            timestamps: Vec::new(),
            values: Vec::new(),
        }
    }
}

impl CompressedChunk {
    pub fn with_values(
        max_size: usize,
        timestamps: &[Timestamp],
        values: &[f64],
    ) -> TsdbResult<Self> {
        let mut res = Self::default();
        res.max_size = max_size;
        res.compress(timestamps, values)?;
        Ok(res)
    }

    /// Determines if this block overlaps the provided time range.
    pub fn overlaps(&self, start: Timestamp, end: Timestamp) -> bool {
        self.min_time <= end && start <= self.max_time
    }

    pub fn contains_timestamp(&self, ts: Timestamp) -> bool {
        ts >= self.min_time && ts <= self.max_time
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn is_full(&self) -> bool {
        self.data_size() >= self.max_size
    }

    pub fn clear(&mut self) {
        self.count = 0;
        self.timestamps.clear();
        self.values.clear();
        self.min_time = 0;
        self.max_time = 0;
        self.last_value = f64::NAN; // todo - use option instead
    }

    pub fn compress(
        &mut self,
        timestamps: &[Timestamp],
        values: &[f64],
    ) -> TsdbResult<()> {
        if timestamps.is_empty() {
            return Ok(());
        }
        assert_eq!(timestamps.len(), values.len());
        // todo: validate range
        self.min_time = timestamps[0];
        self.max_time = timestamps[timestamps.len() - 1];
        self.count = timestamps.len();
        self.last_value = values[values.len() - 1];
        if timestamps.len() > COMPRESSION_PARALLELIZATION_THRESHOLD {
            // use rayon to run compression in parallel
            // first we steal the result buffers to avoid allocation and issues with the BC
            let mut t_data = std::mem::take(&mut self.timestamps);
            let mut v_data = std::mem::take(&mut self.values);

            t_data.clear();
            v_data.clear();

            // then we compress in parallel
            let _ = rayon::join(
                || compress_timestamps(self.optimization, timestamps, &mut t_data).ok(),
                || compress_values(self.optimization, values, &mut v_data).ok(),
            );
            // then we put the buffers back
            self.timestamps = t_data;
            self.values = v_data;
        } else {
            self.timestamps.clear();
            self.values.clear();
            compress_timestamps(self.optimization, timestamps, &mut self.timestamps)?;
            compress_values(self.optimization, values, &mut self.values)?;
        }

        self.timestamps.shrink_to_fit();
        self.values.shrink_to_fit();
        Ok(())
    }

    pub fn decompress(
        &self,
        timestamps: &mut Vec<Timestamp>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        if self.is_empty() {
            return Ok(());
        }
        timestamps.reserve(self.count);
        values.reserve(self.count);
        // todo: dynamically calculate cutoff
        if self.values.len() > 1024 {
            // todo: return errors as appropriate
            let _ = rayon::join(
                || decompress_timestamps(&self.timestamps, timestamps).ok(),
                || decompress_values(&self.values, values).ok(),
            );
        } else {
            decompress_timestamps(&self.timestamps, timestamps)?;
            decompress_values(&self.values, values)?
        }
        Ok(())
    }


    pub(crate) fn process_range<F, State>(
        &mut self,
        start: Timestamp,
        end: Timestamp,
        state: &mut State,
        mut f: F,
    ) -> TsdbResult<()>
        where
            F: FnMut(&mut State, &[i64], &[f64]) -> TsdbResult<()>,
    {
        let mut handle_empty = |state: &mut State| -> TsdbResult<()> {
            let mut timestamps = vec![];
            let mut values = vec![];
            return f(state, &mut timestamps, &mut values);
        };

        if self.is_empty() {
            return handle_empty(state);
        }

        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values= get_pooled_vec_f64(self.count);
        self.decompress(&mut timestamps, &mut values)?;
        // special case of range exceeding block range
        if start <= self.min_time && end >= self.max_time {
            return f(state, &timestamps, &values);
        }

        trim_vec_data(&mut timestamps, &mut values, start, end);
        f(state, &timestamps, &values)
    }

    // todo: move to trait ?
    pub fn merge_samples(
        &mut self,
        samples: &[Sample],
        min_timestamp: Timestamp,
        duplicate_policy: DuplicatePolicy,
        duplicates: &mut AHashSet<Timestamp>) -> TsdbResult<usize> {
        if samples.is_empty() {
            return Ok(0);
        }

        let mut timestamps = get_pooled_vec_i64(samples.len());
        let mut values= get_pooled_vec_f64(samples.len());

        for sample in samples {
            timestamps.push(sample.timestamp);
            values.push(sample.value);
        }

        let mut block = DataBlock::new(&mut timestamps, &mut values);

        let mut src_timestamps = get_pooled_vec_i64(self.count);
        let mut src_values= get_pooled_vec_f64(self.count);
        self.decompress(&mut src_timestamps, &mut src_values)?;

        let mut dst = DataBlock::new(&mut src_timestamps, &mut src_values);
        let res = merge_into(&mut dst, &mut block, min_timestamp, duplicate_policy, duplicates);
        self.compress(&dst.timestamps, &dst.values)?;

        Ok(res)
    }

    fn data_size(&self) -> usize {
        self.timestamps.len() + self.values.len()
    }

    pub fn total_size(&self) -> usize {
        // data
        self.data_size() +
            // self.min_time
            std::mem::size_of::<i64>() +
            // self.max_time
            std::mem::size_of::<i64>() +
            // self.max_size
            std::mem::size_of::<usize>() +
            // count
            std::mem::size_of::<usize>() +
            // last_value
            std::mem::size_of::<f64>() +
            // optimization
            std::mem::size_of::<Option<CompressionOptimization>>()
    }
}

impl Chunk for CompressedChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.min_time
    }

    fn last_timestamp(&self) -> Timestamp {
        self.max_time
    }

    fn num_samples(&self) -> usize {
        self.count
    }

    fn last_value(&self) -> f64 {
        self.last_value
    }

    fn size(&self) -> usize {
        self.data_size()
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        if self.is_empty() {
            return Ok(0);
        }
        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values= get_pooled_vec_f64(self.count);
        self.decompress(&mut timestamps, &mut values)?;
        let (start_idx, end_idx) = get_timestamp_index_bounds(&timestamps, start_ts, end_ts);
        if start_idx > end_idx {
            self.count = 0;
            self.timestamps.clear();
            self.values.clear();
            self.min_time = 0;
            self.max_time = 0;
            return Ok(0);
        }

        let save_count = self.count;
        let timestamp_slice = &timestamps[start_idx..end_idx];
        let value_slice = &values[start_idx..end_idx];
        self.compress(timestamp_slice, value_slice)?;
        Ok(save_count - self.count)
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values= get_pooled_vec_f64(self.count);
        self.decompress(&mut timestamps, &mut values)?;
        timestamps.push(sample.timestamp);
        values.push(sample.value);
        self.compress(&timestamps, &values)
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
        self.decompress(timestamps, values)?;
        trim_vec_data(timestamps, values, start, end);
        Ok(())
    }

    fn upsert_sample(&mut self, sample: &mut Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;

        // this compressed method does not do streaming compressed, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old one
        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values= get_pooled_vec_f64(self.count);
        self.decompress(&mut timestamps, &mut values)?;

        match timestamps.binary_search(&ts) {
            Ok(pos) => {
                let old_sample = Sample {
                    timestamp: timestamps[pos],
                    value: values[pos],
                };
                let cr = handle_duplicate_sample(dp_policy, old_sample, sample);
                if cr != DuplicateStatus::Ok {
                    return Err(TsdbError::DuplicateSample(sample.timestamp.to_string()));
                }
                duplicate_found = true;
                values[pos] = sample.value;
            },
            Err(idx) => {
                timestamps.insert(idx, ts);
                values.insert(idx, sample.value);
            },
        };

        let mut size = timestamps.len();
        if duplicate_found {
            size -= 1;
        }

        self.compress(&timestamps, &values)?;

        Ok(size)
    }

    fn split(&mut self) -> TsdbResult<Self> where Self: Sized {
        let mid = self.num_samples() / 2;

        // this compression method does not do streaming compression, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old
        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values= get_pooled_vec_f64(self.count);
        self.decompress(&mut timestamps, &mut values)?;

        {
            let (left_ts, _) = timestamps.split_at(mid);
            let (left_values, _) = values.split_at(mid);

            self.compress(left_ts, left_values)?;
        }

        let mut result = Self::default();
        result.max_size = self.max_size;
        result.optimization = self.optimization;

        let remaining = timestamps.len() - mid;

        values.rotate_left(mid);
        values.truncate(remaining);
        timestamps.rotate_left(mid);
        timestamps.truncate(remaining);

        result.compress(&timestamps, &values)?;

        Ok(result)
    }
}

fn get_compressor_config(option: Option<CompressionOptimization>) -> Option<CompressorConfig> {
    match option {
        Some(CompressionOptimization::Speed) => {
            let mut res = CompressorConfig::default();
            res.compression_level = 4;
            res.use_gcds = false;
            res.delta_encoding_order = 0;
            return Some(res);
        }
        Some(CompressionOptimization::Size) => {
            let mut res = CompressorConfig::default();
            res.compression_level = 8;
            res.use_gcds = true;
            res.delta_encoding_order = 2;
            return Some(res);
        }
        None => None,
    }
}

fn compress_values(
    option: Option<CompressionOptimization>,
    src: &[f64],
    dst: &mut Vec<u8>,
) -> TsdbResult<()> {
    match get_compressor_config(option) {
        Some(config) => {
            dst.push(CompressionType::Quantile as u8);
            quantile_compress_with_options(src, dst, config)
                .map_err(|_e| TsdbError::CannotSerialize("values".to_string()))
        }
        None => {
            dst.push(CompressionType::Quantile as u8);
            quantile_compress(src, dst)
                .map_err(|_e| TsdbError::CannotSerialize("values".to_string()))
        }
    }
}

fn decompress_values(src: &[u8], dst: &mut Vec<f64>) -> TsdbResult<()> {
    if src.is_empty() {
        return Ok(());
    }
    let first = src[0];
    match CompressionType::try_from(first) {
        Ok(CompressionType::Basic) | Ok(CompressionType::Gorilla) => {
            gorilla_decompress(&src[1..], dst)
                .map_err(|_e| TsdbError::CannotDeserialize("values".to_string()))?;
        }
        Ok(CompressionType::Quantile) => {
            quantile_decompress(&src[1..], dst)
                .map_err(|_e| TsdbError::CannotDeserialize("values".to_string()))?;
        }
        Err(e) => {
            return Err(TsdbError::CannotDeserialize("values".to_string()));
        }
    }
    Ok(())
}

fn compress_timestamps(
    option: Option<CompressionOptimization>,
    src: &[i64],
    dst: &mut Vec<u8>,
) -> TsdbResult<()> {
    match get_compressor_config(option).as_mut() {
        Some(config) => {
            dst.push(CompressionType::Quantile as u8);
            config.delta_encoding_order = 2;
            quantile_compress_with_options(src, dst, config.to_owned())
                .map_err(|_e| TsdbError::CannotSerialize("timestamps".to_string()))?;
        }
        None => {
            dst.push(CompressionType::Basic as u8);
            timestamp_compress(src, dst)
                .map_err(|_e| TsdbError::CannotSerialize("timestamps".to_string()))?;
        }
    }
    Ok(())
}

fn decompress_timestamps(src: &[u8], dst: &mut Vec<i64>) -> TsdbResult<()> {
    if src.is_empty() {
        return Ok(());
    }
    let first = src[0];
    match CompressionType::try_from(first) {
        Ok(CompressionType::Basic) => {
            timestamp_decompress(&src[1..], dst)
                .map_err(|_e| TsdbError::CannotDeserialize("timestamps".to_string()))?;
        }
        Ok(CompressionType::Quantile) => {
            quantile_decompress(&src[1..], dst)
                .map_err(|_e| TsdbError::CannotDeserialize("timestamps".to_string()))?;
        }
        Err(_e) => {
            return Err(TsdbError::CannotDeserialize("timestamps".to_string()));
        }
        _ => {
            return Err(TsdbError::CannotDeserialize(
                "unknown compression type decoding timestamps".to_string(),
            ));
        }
    }
    Ok(())
}
