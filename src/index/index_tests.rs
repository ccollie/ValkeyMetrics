#[cfg(test)]
mod tests {
    use valkey_module::ValkeyString;
    use crate::index::TimeSeriesIndex;
    use crate::storage::Label;
    use crate::storage::time_series::TimeSeries;

    fn create_valkey_string(s: &str) -> ValkeyString {
        ValkeyString::create(None, s.as_bytes())
    }

    fn index_time_series(index: &mut TimeSeriesIndex, ts: &TimeSeries, name: &str) {
        index.index_time_series(ts, name.as_bytes());
    }

    fn create_series() -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.id = TimeSeriesIndex::next_id();
        ts
    }

    #[test]
    fn test_index_series() {
        let mut ts = create_series();
        ts.metric_name = "latency".to_string();
        ts.labels = vec![
            Label {
                name: "region".to_string(),
                value: "us-east1".to_string(),
            },
            Label {
                name: "env".to_string(),
                value: "qa".to_string(),
            },
        ];

        let mut index = TimeSeriesIndex::new();
        index_time_series(&mut index, &ts, "time-series-1");

        assert_ne!(ts.id, 0);
        assert_eq!(index.label_count(), 3 /* metric_name + region + env */);
        assert_eq!(index.series_count(), 1);
    }

    #[test]
    fn test_remove_series() {
        let mut ts = create_series();

        ts.metric_name = "latency".to_string();
        ts.labels = vec![
            Label {
                name: "region".to_string(),
                value: "us-east1".to_string(),
            },
            Label {
                name: "env".to_string(),
                value: "dev".to_string(),
            },
        ];

        let mut ts2 = ts.clone();
        ts2.id = TimeSeriesIndex::next_id();

        ts2.labels[1].value = "qa".to_string();
        ts2.labels.push(Label {
            name: "version".to_string(),
            value: "1.0.0".to_string(),
        });

        let mut index = TimeSeriesIndex::new();

        index_time_series(&mut index, &ts, "time-series-1");
        index_time_series(&mut index, &ts2, "time-series-2");

        assert_eq!(index.series_count(), 2);
        assert_eq!(index.label_count(), 4);

        index.remove_series(&ts2);

        assert_eq!(index.series_count(), 1);
        assert_eq!(index.label_count(), 3);
    }

    #[test]
    fn test_get_label_values() {
        let mut index = TimeSeriesIndex::new();

        let mut ts = create_series();

        ts.metric_name = "latency".to_string();
        ts.labels = vec![
            Label {
                name: "region".to_string(),
                value: "us-east-1".to_string(),
            },
            Label {
                name: "env".to_string(),
                value: "dev".to_string(),
            },
        ];

        let mut ts2 = ts.clone();
        ts2.id = TimeSeriesIndex::next_id();

        ts2.labels[0].value = "us-east-2".to_string();
        ts2.labels[1].value = "qa".to_string();

        let mut ts3 = ts.clone();
        ts3.labels[1].value = "prod".to_string();

        index_time_series(&mut index, &ts, "time-series-1");
        index_time_series(&mut index, &ts2, "time-series-2");
        index_time_series(&mut index, &ts3, "time-series-3");

        let values = index.get_label_values("region");
        assert_eq!(values.len(), 2);
        assert!(values.contains(&"us-east-1".to_string()));
        assert!(values.contains(&"us-east-2".to_string()));

        let values = index.get_label_values("env");
        assert_eq!(values.len(), 3);
        assert!(values.contains(&"dev".to_string()));
        assert!(values.contains(&"qa".to_string()));
        assert!(values.contains(&"prod".to_string()));

    }
}