#[cfg(test)]
mod tests {
    use crate::index::TimeSeriesIndex;
    use crate::storage::Label;
    use crate::storage::time_series::TimeSeries;

    #[test]
    fn test_index_series() {
        let mut ts = TimeSeries::new();
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
        index.index_time_series(&mut ts, "time-series-1".to_string());

        assert_ne!(ts.id, 0);
        assert_eq!(index.label_count(), 3 /* metric_name + region + env */);
        assert_eq!(index.series_count(), 1);
    }

    #[test]
    fn test_remove_series() {
        let mut ts = TimeSeries::new();
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
        ts2.labels[1].value = "qa".to_string();
        ts2.labels.push(Label {
            name: "version".to_string(),
            value: "1.0.0".to_string(),
        });

        let mut index = TimeSeriesIndex::new();
        index.index_time_series(&mut ts, "time-series-1".to_string());
        index.index_time_series(&mut ts2, "time-series-2".to_string());

        assert_eq!(index.series_count(), 2);
        assert_eq!(index.label_count(), 4);

        index.remove_series(&ts2);

        assert_eq!(index.series_count(), 1);
        assert_eq!(index.label_count(), 3);
    }

    #[test]
    fn test_get_label_values() {
        let mut index = TimeSeriesIndex::new();

        let mut ts = TimeSeries::new();
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
        ts2.labels[0].value = "us-east-2".to_string();
        ts2.labels[1].value = "qa".to_string();

        let mut ts3 = ts.clone();
        ts3.labels[1].value = "prod".to_string();

        index.index_time_series(&mut ts, "time-series-1".to_string());
        index.index_time_series(&mut ts2, "time-series-2".to_string());
        index.index_time_series(&mut ts3, "time-series-3".to_string());

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