use crate::common::types::{Sample, Timestamp};
use crate::series::index::{KeyType, TimeSeriesIndex};
use crate::series::time_series::TimeSeries;
use crate::series::TimeSeriesOptions;
use async_trait::async_trait;
use metricsql_runtime::prelude::{
    Deadline,
    MetricName,
    MetricStorage,
    QueryResult,
    QueryResults,
    RuntimeResult,
    SearchQuery
};
use std::collections::HashMap;
use std::sync::RwLock;
use valkey_module::{ValkeyError, ValkeyResult};

/// Interface between the time series database and the metricsql runtime.
/// Testing only
pub(super) struct TestMetricStorage {
    index: TimeSeriesIndex,
    series: RwLock<HashMap<KeyType, TimeSeries>>
}

impl TestMetricStorage {

    pub fn new() -> Self {
        TestMetricStorage {
            index: TimeSeriesIndex::new(),
            series: RwLock::new(HashMap::new())
        }
    }

    fn add_by_key(&mut self, key: &str, ts: Timestamp, val: f64) -> ValkeyResult<()> {
        let key = string_to_key(key);
        self.with_mutable_series(&key, |series| {
            series.add(ts, val, None)
        })
    }

    pub fn add(&mut self, metric: &str, ts: Timestamp, value: f64) -> ValkeyResult<()> {
        let mn = match MetricName::parse(metric) {
            Ok(mn) => mn,
            Err(_) => return Err(ValkeyError::String("Invalid metric name".to_string()))
        };
        let key = mn.to_string().into_bytes().into_boxed_slice();
        self.with_mutable_series(&key, |series| {
            series.add(ts, value, None)
        })
    }

    pub fn add_sample(&mut self, mn: &MetricName, sample: &Sample) -> ValkeyResult<()> {
        let key = match self.get_key_from_metric_name(mn) {
            Some(k) => k,
            None => {
                self.insert_series_from_metric_name(mn);
                let mn_str = mn.to_string();
                string_to_key(mn_str.as_str())
            }
        };
        self.with_mutable_series(&key, |series| {
            series.add(sample.timestamp, sample.value, None)
        })
    }

    fn get_key_from_metric_name(&self, mn: &MetricName) -> Option<KeyType> {
        if let Ok(key) = self.index.get_key_by_name_and_labels(&mn.measurement, &mn.labels) {
            return key;
        }
        None
    }

    fn insert_series_from_metric_name(&mut self, mn: &MetricName) {
        let mut time_series = self.create_series(mn);
        let mut map = self.series.write().unwrap();
        let key = timeseries_key(&time_series);
        self.index.index_time_series(&mut time_series, &key).unwrap();
        map.insert(key, time_series);
    }

    fn create_series(&self, mn: &MetricName) -> TimeSeries {
        let chunk_compression = crate::series::ChunkCompression::Uncompressed;
        let options = TimeSeriesOptions {
            chunk_size: Some(4096),
            chunk_compression: Some(chunk_compression),
            ..Default::default()
        };
        let mut time_series = TimeSeries::with_options(options).unwrap();
        time_series.metric_name = mn.measurement.clone();
        time_series.labels = mn.labels.clone();
        time_series.chunk_compression = chunk_compression;
        time_series
    }

    fn with_mutable_series<F>(&mut self, key: &KeyType, mut f: F) -> ValkeyResult<()>
    where F: FnMut(&mut TimeSeries) -> ValkeyResult<()>
    {
        let mut map = self.series.write().unwrap();
        match map.get_mut(key) {
            Some(series) => f(series),
            None => Ok(())
        }
    }

    fn get_series_range(&self, key: &KeyType, start_ts: Timestamp, end_ts: Timestamp) -> Option<(MetricName, Vec<Sample>)> {
        let map = self.series.read().unwrap();
        match map.get(key) {
            Some(series) => {
                let metric = super::to_metric_name(series);
                let samples = series.get_range(start_ts, end_ts);
                Some((metric, samples))
            }
            None => None
        }
    }

    fn get_series(&self, key: &KeyType, start_ts: Timestamp, end_ts: Timestamp) -> RuntimeResult<Option<QueryResult>> {
        if let Some((metric_name, samples)) = self.get_series_range(key, start_ts, end_ts) {

            let count = samples.len();
            let (mut timestamps, mut values) = if count > 0 {
                (Vec::with_capacity(count), Vec::with_capacity(count))
            } else {
                (vec![], vec![])
            };

            for Sample { timestamp, value } in samples {
                timestamps.push(timestamp);
                values.push(value);
            }

            Ok(Some(QueryResult::new(metric_name, timestamps, values)))

        } else {
            Ok(None)
        }
    }

    fn get_series_data(
        &self,
        search_query: SearchQuery,
    ) -> RuntimeResult<Vec<QueryResult>> {
        let map = self.index.series_keys_by_matchers_internal(&[search_query.matchers]);
        let mut results: Vec<QueryResult> = Vec::with_capacity(map.len());
        let start_ts = search_query.start;
        let end_ts = search_query.end;

        // use rayon ?
        for key in map.iter() {
            if let Some(result) = self.get_series(key, start_ts, end_ts)? {
                results.push(result);
            }
        }
        Ok(results)
    }
}

fn timeseries_key(ts: &TimeSeries) -> KeyType {
    let key = ts.prometheus_metric_name();
    string_to_key(key.as_str())
}

fn string_to_key(s: &str) -> KeyType {
    s.as_bytes().to_vec().into_boxed_slice()
}

#[async_trait]
impl MetricStorage for TestMetricStorage {
    async fn search(&self, sq: SearchQuery, _deadline: Deadline) -> RuntimeResult<QueryResults> {
        let data = self.get_series_data(sq)?;
        let result = QueryResults::new(data);
        Ok(result)
    }
}
