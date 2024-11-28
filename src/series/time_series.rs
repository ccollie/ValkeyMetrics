use super::{validate_chunk_size, Chunk, ChunkCompression, SampleAddResult, TimeSeriesOptions};
use crate::common::rounding::RoundingStrategy;
use crate::common::types::{IntMap, Label, Sample, Timestamp};
use crate::common::METRIC_NAME_LABEL;
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::series::merge::merge_by_capacity;
use crate::series::types::ValueFilter;
use crate::series::utils::{filter_samples_by_date_range, filter_samples_by_value, format_prometheus_metric_name};
use crate::series::DuplicatePolicy;
use crate::series::TimeSeriesChunk;
use ahash::HashMapExt;
use get_size::GetSize;
use smallvec::SmallVec;
use std::hash::Hash;
use std::mem::size_of;
use std::time::Duration;
use std::vec;
use valkey_module::{logging, ValkeyError, ValkeyResult};
use crate::config::{DEFAULT_CHUNK_COMPRESSION, DEFAULT_CHUNK_SIZE_BYTES, DEFAULT_DUPLICATE_POLICY, DEFAULT_RETENTION_PERIOD};

pub(super) const TIMESTAMP_TYPE_U64: &str = "u64";
pub(super) const TIMESTAMP_TYPE_U32: &str = "u32";

cfg_if::cfg_if! {
    if #[cfg(feature = "id64")] {
        pub type TimeseriesId = u64;
        pub const TIMESTAMP_TYPE: &str = TIMESTAMP_TYPE_U64;
    } else {
        pub type TimeseriesId = u32;
        pub const TIMESTAMP_TYPE: &str = TIMESTAMP_TYPE_U64;
    }
}

/// Represents a time series. The time series consists of time series blocks, each containing BLOCK_SIZE_FOR_TIME_SERIES
/// data points.
#[derive(Clone, Debug, PartialEq)]
#[derive(GetSize)]
pub struct TimeSeries {
    /// fixed internal id used in indexing
    pub id: TimeseriesId,

    /// Name of the metric
    /// For example, given `http_requests_total{method="POST", status="500"}`
    /// the metric name is `http_requests_total`, and the labels are method="POST" and status="500"
    /// Metric names must match the regex [a-zA-Z_:][a-zA-Z0-9_:]*
    pub metric_name: String,
    pub labels: Vec<Label>,

    pub retention: Duration,
    pub dedupe_interval: Option<Duration>,
    pub dedupe_value_delta: Option<f64>,
    pub duplicate_policy: DuplicatePolicy,
    pub chunk_compression: ChunkCompression,
    pub rounding: Option<RoundingStrategy>,
    pub chunk_size_bytes: usize,
    pub chunks: Vec<TimeSeriesChunk>,
    // meta
    pub total_samples: usize,
    // todo:
    //  last_sample: Option<Sample>,
    pub first_timestamp: Timestamp,
    pub last_timestamp: Timestamp,
    pub last_value: f64,
}

/// Hash based on metric name, which should be unique in the db
impl Hash for TimeSeries {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.metric_name.hash(state);
        self.labels.hash(state);
    }
}

impl TimeSeries {
    /// Create a new empty time series.
    pub fn new() -> Self {
        TimeSeries::default()
    }

    pub fn with_options(options: TimeSeriesOptions) -> TsdbResult<Self> {
        let mut options = options;

        options.set_defaults_from_config();

        let mut res = Self::new();
        if let Some(chunk_size) = options.chunk_size {
            validate_chunk_size(chunk_size)?;
            res.chunk_size_bytes = chunk_size;
        }

        res.chunk_compression = options.chunk_compression.unwrap_or(DEFAULT_CHUNK_COMPRESSION);

        res.duplicate_policy = options.duplicate_policy.unwrap_or(DEFAULT_DUPLICATE_POLICY);

        res.retention = options.retention.unwrap_or(DEFAULT_RETENTION_PERIOD);

        if let Some(dedupe_interval) = options.dedupe_interval {
            res.dedupe_interval = Some(dedupe_interval);
        }

        // todo: make sure labels are sorted and dont contain __name__
        let label = options.labels
            .iter()
            .find(|x| x.name == METRIC_NAME_LABEL); // better error

        if let Some(label) = label {
            res.metric_name.clone_from(&label.value);
        } else {
            return Err(TsdbError::InvalidMetric("ERR missing metric name".to_string()));
        }

        options.labels.retain(|x| x.name != METRIC_NAME_LABEL);

        res.labels = options.labels;
        res.rounding = options.rounding;

        Ok(res)
    }

    pub fn len(&self) -> usize {
        self.total_samples
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the full metric name of the time series, including labels in Prometheus format.
    /// For example,
    ///
    /// `http_requests_total{method="POST", status="500"}`
    ///
    /// Note that for our internal purposes, we store the metric name and labels separately, and
    /// assume that the labels are sorted by name.
    pub fn prometheus_metric_name(&self) -> String {
        format_prometheus_metric_name(&self.metric_name, &self.labels)
    }

    pub fn label_value(&self, name: &str) -> Option<&String> {
        if name == METRIC_NAME_LABEL {
            return Some(&self.metric_name);
        }
        if let Some(label) = self.labels.iter().find(|x| x.name == name) {
            return Some(&label.value);
        }
        None
    }

    fn adjust_value(&self, value: f64) -> f64 {
        self.rounding.as_ref().map_or(value, |r| r.round(value))
    }

    pub fn add(
        &mut self,
        ts: Timestamp,
        value: f64,
        dp_override: Option<DuplicatePolicy>,
    ) -> SampleAddResult {
        if self.is_older_than_retention(ts) {
            return SampleAddResult::TooOld;
        }

        let sample = Sample {
            value: self.adjust_value(value),
            timestamp: ts
        };

        let last_ts = self.last_timestamp;

        if !self.is_empty() {
            if ts >= last_ts {
                let res = self.validate_ignores(ts, sample.value, last_ts, self.last_value, dp_override);
                if !res.is_ok() {
                    return res;
                }
            }
            if ts <= last_ts {
                return self.upsert_sample(sample, dp_override);
            }
        }

        self.add_sample(sample)
    }

    pub(crate) fn validate_ignores(&self,
                            timestamp: Timestamp,
                            value: f64,
                            last_ts: Timestamp,
                            last_value: f64,
                            duplicate_policy: Option<DuplicatePolicy>) -> SampleAddResult {

        let policy = duplicate_policy.unwrap_or(self.duplicate_policy);

        if timestamp >= last_ts && policy == DuplicatePolicy::KeepLast {
            if let Some(dedup_interval) = self.dedupe_interval {
                let millis = dedup_interval.as_millis() as i64;
                if millis > 0 && (timestamp - last_ts) < millis {
                    return SampleAddResult::Ignored(last_ts);
                }
            }
            if let Some(dedup_value_delta) = self.dedupe_value_delta {
                if (last_value - value).abs() < dedup_value_delta {
                    return SampleAddResult::Ignored(last_ts);
                }
            }
        }
        SampleAddResult::Ok(timestamp)
    }

    pub(crate) fn validate_sample(&self,
                           timestamp: Timestamp,
                           value: f64,
                           last_ts: Timestamp,
                           last_value: f64,
                           on_duplicate: Option<DuplicatePolicy>) -> SampleAddResult {

        if self.is_older_than_retention(timestamp) {
            return SampleAddResult::TooOld;
        }

        self.validate_ignores(timestamp, value, last_ts, last_value, on_duplicate)
    }

    pub(super) fn add_sample(&mut self, sample: Sample) -> SampleAddResult {

        let was_empty = self.is_empty();
        let chunk = self.get_last_chunk();
        match chunk.add_sample(&sample) {
            Err(TsdbError::CapacityFull(_)) => {
                match self.add_chunk_with_sample(&sample) {
                    Ok(_) => {},
                    Err(TsdbError::DuplicateSample(_)) => return SampleAddResult::Duplicate,
                    Err(_) => return SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE),
                }
            },
            Err(_e) => return SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE),
            _ => {},
        }

        if was_empty {
            self.first_timestamp = sample.timestamp;
        }

        self.last_value = sample.value;
        self.last_timestamp = sample.timestamp;
        self.total_samples += 1;
        SampleAddResult::Ok(sample.timestamp)
    }

    /// (Possibly) add a new chunk and append the given sample.
    fn add_chunk_with_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        let min_timestamp = self.get_min_timestamp();

        // arrrgh! rust treats vecs as a single unit wrt borrowing, but the following iterator trick
        // seems to work
        let mut iter = self.chunks.iter_mut().rev();
        let last_chunk = iter.next().unwrap();

        // check if previous chunk has capacity, and if so merge into it
        if let Some(prev_chunk) = iter.next() {
            if let Some(deleted_count) = merge_by_capacity(
                prev_chunk,
                last_chunk,
                min_timestamp,
                None,
            )? {
                self.total_samples -= deleted_count;
                last_chunk.add_sample(sample)?;
                return Ok(());
            }
        }

        let mut chunk = self.create_chunk();
        chunk.add_sample(sample)?;
        self.chunks.push(chunk);

        Ok(())
    }

    fn append_chunk(&mut self) {
        let new_chunk = self.create_chunk();
        self.chunks.push(new_chunk);
    }

    fn create_chunk(&mut self) -> TimeSeriesChunk {
        TimeSeriesChunk::new(self.chunk_compression, self.chunk_size_bytes)
    }

    #[inline]
    fn get_last_chunk(&mut self) -> &mut TimeSeriesChunk {
        if self.chunks.is_empty() {
            self.append_chunk();
        }
        self.chunks.last_mut().unwrap()
    }

    fn upsert(chunk: &mut TimeSeriesChunk, sample: Sample, policy: DuplicatePolicy) -> (usize, SampleAddResult) {
        match chunk.upsert_sample(sample, policy) {
            Ok(size) => (size, SampleAddResult::Ok(sample.timestamp)),
            Err(TsdbError::DuplicateSample(_)) => {
                (0, SampleAddResult::Duplicate)
            },
            Err(_) => {
                (0, SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE))
            },
        }
    }

    fn upsert_sample(
        &mut self,
        sample: Sample,
        duplicate_policy_override: Option<DuplicatePolicy>,
    ) -> SampleAddResult {
        let dp_policy = duplicate_policy_override.unwrap_or(self.duplicate_policy);

        let (pos, _) = get_chunk_index(&self.chunks, sample.timestamp);
        let chunk = self.chunks.get_mut(pos).unwrap(); // todo: get_unchecked, since pos is always valid

        if chunk.should_split() {
            let mut new_chunk = match chunk.split() {
                Ok(chunk) => chunk,
                Err(_) => {
                    return SampleAddResult::Error(error_consts::CHUNK_SPLIT)
                },
            };

            let (size, res) = Self::upsert(&mut new_chunk, sample, dp_policy);
            if !res.is_ok() {
                return res;
            }

            // todo: do this in background so ingestion is not blocked
            if let Err(e) = self.trim() {
                #[cfg(not(test))] // so we can run unit tests
                logging::log_warning(format!("Error trimming time series: {:?}", e));
            }
            let insert_at = self.chunks.partition_point(|chunk| chunk.first_timestamp() <= new_chunk.first_timestamp());
            self.chunks.insert(insert_at, new_chunk);
            self.total_samples += size;
            if sample.timestamp == self.last_timestamp {
                self.last_value = sample.value;
            }
            return SampleAddResult::Ok(sample.timestamp)
        }

        let (size, res) = Self::upsert(chunk, sample, dp_policy);
        if !res.is_ok() {
            return res;
        }
        self.total_samples += size;
        if sample.timestamp == self.last_timestamp {
            self.last_value = sample.value;
        }
        SampleAddResult::Ok(sample.timestamp)
    }

    pub fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        let dp_policy = dp_policy.unwrap_or(self.duplicate_policy);

        let mut samples = samples.iter().map(|sample| {
            Sample {
                value: self.adjust_value(sample.value),
                timestamp: sample.timestamp,
            }
        }).collect::<Vec<Sample>>();
        samples.sort();

        #[derive(Default)]
        struct GroupingState {
            samples: SmallVec<Sample, 6>,
            indexes: SmallVec<usize, 6>,
        }

        let mut grouping: IntMap<usize, GroupingState> = IntMap::new();

        let earliest_ts = self.get_min_timestamp();
        let mut res: Vec<SampleAddResult> = Vec::with_capacity(samples.len());

        for (index, sample) in samples.iter().enumerate() {
            if sample.timestamp < earliest_ts {
                res.push(SampleAddResult::TooOld);
            } else {
                let (chunk_index, _) = find_last_ge_index(&self.chunks, sample.timestamp);
                let entry = grouping.entry(chunk_index).or_default();
                entry.samples.push(*sample);
                entry.indexes.push(index);
                // push temporary result
                res.push(SampleAddResult::Ok(sample.timestamp));
            }
        }

        // todo: parallelize
        for (chunk_index, group) in grouping {
            let chunk = self.chunks.get_mut(chunk_index).unwrap();
            let results = chunk.merge_samples(&group.samples, Some(dp_policy))?;
            for (i, result) in group.indexes.iter().zip(results) {
                res[*i] = result;
            }
        }

        Ok(res)
    }


    /// Get the time series between given start and end time (both inclusive).
    pub fn get_range(&self, start_time: Timestamp, end_time: Timestamp) -> Vec<Sample> {
        // todo: if we span across multiple chunks, we can use rayon to fetch samples
        // in parallel.
        let min_timestamp = self.get_min_timestamp().max(start_time);
        if !self.overlaps(min_timestamp, end_time) {
            return Vec::new();
        }
        self.range_iter(min_timestamp, end_time).collect()
    }

    pub fn get_sample(&self, start_time: Timestamp) -> ValkeyResult<Option<Sample>> {
        let (index, found) = get_chunk_index(&self.chunks, start_time);
        if found {
            let chunk = &self.chunks[index];
            // todo: better error handling
            let mut samples = chunk.get_range(start_time, start_time)
                .map_err(|_e| ValkeyError::Str(error_consts::ERROR_FETCHING_SAMPLE))?;
            Ok(samples.pop())
        } else {
            Ok(None)
        }
    }


    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() || timestamps.is_empty() {
            return Ok(vec![]);
        }

        let mut samples = Vec::with_capacity(timestamps.len());
        let mut map: IntMap<usize, SmallVec<Timestamp, 6>> = Default::default();

        for &ts in timestamps {
            let (index, _) = get_chunk_index(&self.chunks, ts);
            if index < self.chunks.len() {
                map.entry(index).or_default().push(ts);
            }
        }

        for (index, ts_list) in map {
            let chunk = &self.chunks[index];
            let sub_samples = chunk.samples_by_timestamps(&ts_list)?;
            samples.extend(sub_samples);
        }

        samples.sort_by_key(|sample| sample.timestamp);
        Ok(samples)
    }

    pub fn iter(&self) -> SeriesSampleIterator {
        SeriesSampleIterator::new(self, self.first_timestamp, self.last_timestamp, &None, &None)
    }

    pub fn range_iter(
        &self,
        start: Timestamp,
        end: Timestamp,
    ) -> SeriesSampleIterator {
        SeriesSampleIterator::new(self, start, end, &None, &None)
    }

    pub fn overlaps(&self, start_ts: Timestamp, end_ts: Timestamp) -> bool {
        self.last_timestamp >= start_ts && self.first_timestamp <= end_ts
    }

    pub fn is_older_than_retention(&self, timestamp: Timestamp) -> bool {
        let min_ts = self.get_min_timestamp();
        timestamp < min_ts
    }

    pub(super) fn trim(&mut self) -> TsdbResult<()> {
        let min_timestamp = self.get_min_timestamp();
        if self.first_timestamp == min_timestamp {
            return Ok(());
        }

        let mut deleted_count = 0;

        // Remove entire chunks that are before min_timestamp
        self.chunks.retain(|chunk| {
            let last_ts = chunk.last_timestamp();
            if last_ts <= min_timestamp {
                deleted_count += chunk.len();
                false
            } else {
                true
            }
        });

        // Handle partial chunk
        if let Some(chunk) = self.chunks.first_mut() {
            if chunk.first_timestamp() < min_timestamp {
                match chunk.remove_range(0, min_timestamp) {
                    Ok(count) => deleted_count += count,
                    Err(_) => {
                        return Err(TsdbError::RemoveRangeError);
                    }
                }
            }
        }

        self.total_samples -= deleted_count;

        if deleted_count > 0 {
            // Update first_timestamp and last_timestamp
            if !self.chunks.is_empty() {
                self.first_timestamp = self.chunks.first().unwrap().first_timestamp();
                self.last_timestamp = self.chunks.last().unwrap().last_timestamp();
            } else {
                self.first_timestamp = 0;
                self.last_timestamp = 0;
                self.last_value = f64::NAN;
            }
        }

        Ok(())
    }

    pub fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {

        if end_ts < self.first_timestamp || start_ts > self.last_timestamp {
            return Ok(0);
        }

        let (index, _) = get_chunk_index(&self.chunks, start_ts);

        let chunks = &mut self.chunks[index..];
        let mut deleted_chunks: usize = 0;
        let mut deleted_samples = 0;

        for chunk in chunks.iter_mut() {
            let chunk_last_ts = chunk.last_timestamp();

            // Should we delete the entire chunk?
            if chunk.is_contained_by_range(start_ts, end_ts) {
                deleted_samples += chunk.len();
                chunk.clear();
                deleted_chunks += 1;
            } else if chunk_last_ts <= end_ts {
                deleted_samples += chunk.remove_range(start_ts, end_ts)?;
            } else {
                break
            }
        }

        self.total_samples -= deleted_samples;

        // if all chunks were deleted,
        if deleted_chunks == self.chunks.len() {
            // todo: have a flag to determine whether to have an empty root chunk
        }

        // Remove empty chunks
        self.chunks.retain(|chunk| !chunk.is_empty());

        // Update last timestamp and last value if necessary
        if !self.is_empty() {
            if start_ts <= self.first_timestamp {
                let first_chunk = self.chunks.first_mut().unwrap();
                self.first_timestamp = first_chunk.first_timestamp();
            }
            if end_ts >= self.last_timestamp {
                let last_chunk = self.chunks.last_mut().unwrap();
                self.last_timestamp = last_chunk.last_timestamp();
                self.last_value = last_chunk.last_value();
            }
        } else {
            self.first_timestamp = 0;
            self.last_timestamp = 0;
            self.last_value = f64::NAN;
        }

        Ok(deleted_samples)
    }

    pub fn data_size(&self) -> usize {
        self.chunks.iter().map(|x| x.size()).sum()
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() +
            self.get_heap_size()
    }

    pub(crate) fn get_min_timestamp(&self) -> Timestamp {
        if self.retention.is_zero() {
            return self.first_timestamp;
        }
        let retention_millis = self.retention.as_millis() as i64;
        (self.last_timestamp - retention_millis).min(0)
    }
}

impl Default for TimeSeries {
    fn default() -> Self {
        Self {
            id: 0,
            metric_name: "".to_string(),
            labels: vec![],
            retention: Default::default(),
            duplicate_policy: DuplicatePolicy::KeepLast,
            chunk_compression: Default::default(),
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            dedupe_interval: Default::default(),
            chunks: vec![],
            total_samples: 0,
            first_timestamp: 0,
            last_timestamp: 0,
            last_value: f64::NAN,
            rounding: None,
            dedupe_value_delta: None,
        }
    }
}

fn binary_search_chunks_by_timestamp(chunks: &[TimeSeriesChunk], ts: Timestamp) -> (usize, bool) {
    match chunks.binary_search_by(|probe| {
        if ts < probe.first_timestamp() {
            std::cmp::Ordering::Greater
        } else if ts > probe.last_timestamp() {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Equal
        }
    }) {
        Ok(pos) => (pos, true),
        Err(pos) => (pos, false),
    }
}

/// Find the index of the first chunk in which the timestamp belongs. Assumes!chunks.is_empty()
pub(super) fn find_start_chunk_index(arr: &[TimeSeriesChunk], ts: Timestamp) -> usize {
    if arr.is_empty() {
        // If the vector is empty, return the first index.
        return 0;
    }
    if ts <= arr[0].first_timestamp() {
        // If the timestamp is less than the first chunk's start timestamp, return the first index.
        return 0;
    }
    if arr.len() <= 16 {
        // If the vectors are small, perform a linear search.
        return arr.iter().position(|x| ts >= x.first_timestamp()).unwrap_or(arr.len());
    }
    let (pos, _) = binary_search_chunks_by_timestamp(arr, ts);
    pos
}


/// Return the index of the chunk in which the timestamp belongs. Assumes !chunks.is_empty()
fn get_chunk_index(chunks: &[TimeSeriesChunk], timestamp: Timestamp) -> (usize, bool) {
    if chunks.len() <= 16 {
        return chunks.iter().enumerate().find_map(|(i, chunk)| {
            if timestamp >= chunk.first_timestamp() && timestamp <= chunk.last_timestamp() {
                Some((i, true))
            } else {
                None
            }
        }).unwrap_or((chunks.len(), false));
    }

    binary_search_chunks_by_timestamp(chunks, timestamp)
}

fn find_last_ge_index(chunks: &[TimeSeriesChunk], ts: Timestamp) -> (usize, bool) {
    if chunks.len() <= 16 {
        return chunks.iter().rposition(|x| ts >= x.last_timestamp())
            .map_or((0, false), |idx| {
                let chunk = &chunks[idx];
                if chunk.is_timestamp_in_range(ts) {
                    (idx, true)
                } else {
                    (idx.saturating_sub(1), false)
                }
            });
    }
    binary_search_chunks_by_timestamp(chunks, ts)
}

pub struct SeriesSampleIterator<'a> {
    value_filter: &'a Option<ValueFilter>,
    ts_filter: &'a Option<Vec<Timestamp>>, // box instead
    chunk_iter: std::slice::Iter<'a, TimeSeriesChunk>,
    sample_iter: vec::IntoIter<Sample>,
    chunk: Option<&'a TimeSeriesChunk>,
    is_init: bool,
    pub(crate) start: Timestamp,
    pub(crate) end: Timestamp,
}

impl<'a> SeriesSampleIterator<'a> {
    pub(crate) fn new(series: &'a TimeSeries,
                      start: Timestamp,
                      end: Timestamp,
                      value_filter: &'a Option<ValueFilter>,
                      ts_filter: &'a Option<Vec<Timestamp>>, // box instead
    ) -> Self {
        let chunk_index = find_start_chunk_index(&series.chunks, start);

        let chunk_iter = if chunk_index < series.chunks.len() {
            series.chunks[chunk_index..].iter()
        } else {
            Default::default()
        };

        Self {
            start,
            end,
            value_filter,
            ts_filter,
            chunk_iter,
            sample_iter: Default::default(),
            chunk: None,
            is_init: false,
        }
    }

    fn get_iter(&mut self, start: Timestamp, end: Timestamp) -> vec::IntoIter<Sample> {
        self.is_init = true;
        self.chunk = self.chunk_iter.next();
        match self.chunk {
            Some(chunk) => {
                let samples = chunk.get_range_filtered(start, end, self.ts_filter, self.value_filter);
                self.start = chunk.last_timestamp();
                samples.into_iter()
            }
            None => Default::default(),
        }
    }

}

// todo: implement next_chunk
impl Iterator for SeriesSampleIterator<'_> {
    type Item = Sample;
    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_init {
            self.sample_iter = self.get_iter(self.start, self.end);
        }
        if let Some(sample) = self.sample_iter.next() {
            Some(sample)
        } else {
            self.sample_iter = self.get_iter(self.start, self.end);
            self.sample_iter.next()
        }
    }
}


// todo: move elsewhere, better name
pub(crate) fn get_series_range_filtered(
    series: &TimeSeries,
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
    timestamp_filter: &Option<Vec<Timestamp>>,
    value_filter: &Option<ValueFilter>
) -> Vec<Sample> {

    match (timestamp_filter, value_filter) {
        (Some(ts_filter), Some(value_filter)) => {
            let mut samples = series.samples_by_timestamps(ts_filter)
                .unwrap_or_default()
                .into_iter()
                .collect();

            filter_samples_by_date_range(&mut samples, start_timestamp, end_timestamp);
            filter_samples_by_value(&mut samples, value_filter);
            samples
        }
        (None, Some(value_filter)) => {
            let mut samples = series.get_range(start_timestamp, end_timestamp);
            filter_samples_by_value(&mut samples, value_filter);
            samples
        }
        (Some(ts_filter), None) => {
            let mut samples = series.samples_by_timestamps(ts_filter)
                .unwrap_or_default()
                .into_iter()
                .collect();

            filter_samples_by_date_range(&mut samples, start_timestamp, end_timestamp);
            samples
        }
        (None, None) => {
            series.get_range(start_timestamp, end_timestamp)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::test_utils::generate_random_samples;

    #[test]
    fn test_one_entry() {
        let mut ts = TimeSeries::new();
        assert!(ts.add(100, 200.0, None).is_ok());

        assert_eq!(ts.get_last_chunk().len(), 1);
        let last_block = ts.get_last_chunk();
        let samples = last_block.get_range(0, 1000).unwrap();

        let data_point = samples.get(0).unwrap();
        assert_eq!(data_point.timestamp, 100);
        assert_eq!(data_point.value, 200.0);
        assert_eq!(ts.total_samples, 1);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp, 100);
    }

    #[test]
    fn test_1000_entries() {
        let mut ts = TimeSeries::new();
        let data = generate_random_samples(0, 1000);

        for sample in data.iter() {
            assert!(ts.add(sample.timestamp, sample.value, None).is_ok());
        }

        assert_eq!(ts.total_samples, 1000);
        assert_eq!(ts.first_timestamp, data[0].timestamp);
        assert_eq!(ts.last_timestamp, data[data.len() - 1].timestamp);

        for (sample, orig) in ts.iter().zip(data.iter()) {
            assert_eq!(sample.timestamp, orig.timestamp);
            assert_eq!(sample.value, orig.value);
        }
    }

    const BLOCK_SIZE_FOR_TIME_SERIES: usize = 1000;

    #[test]
    fn test_block_size_entries() {
        let mut ts = TimeSeries::new();
        for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
            assert!(ts.add(i as i64, i as f64, None).is_ok());
        }

        // All the entries will go to 'last', as we have pushed exactly BLOCK_SIZE_FOR_TIME_SERIES entries.
        assert_eq!(ts.chunks.len(), 2);
        assert_eq!(
            ts.get_last_chunk().len(),
            BLOCK_SIZE_FOR_TIME_SERIES
        );

        for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
            let last_block = ts.get_last_chunk();
            let samples = last_block.get_range(0, 7000).unwrap();
            let data_point = samples.get(i).unwrap();
            assert_eq!(data_point.timestamp, i as i64);
            assert_eq!(data_point.value, i as f64);
        }
    }

    #[test]
    fn test_last_chunk_overflow() {
        todo!();

    }
}
