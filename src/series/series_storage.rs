use valkey_module::ValkeyResult;
use crate::series::{TimeSeries, TimeseriesId};

pub trait SeriesStorage<'a> {
    fn insert_series(&mut self, series: TimeSeries) -> ValkeyResult;
    fn get_series(&self, id: TimeseriesId) -> Option<&'a TimeSeries>;
    fn get_series_by_key(&self, key: &str) -> Option<&'a TimeSeries>;
    fn remove_series(&mut self, key: &str);
}