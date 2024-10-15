#[cfg(test)]
mod tests {
    use crate::common::types::{Label, Sample, Timestamp};
    use crate::error::TsdbError;
    use crate::series::{Chunk, ChunkCompression, DuplicatePolicy, TimeSeries, TimeSeriesChunk};
    use std::time::Duration;
    use crate::series::test_utils::generate_random_samples;

    fn create_test_timeseries() -> TimeSeries {
        TimeSeries {
            id: 1,
            metric_name: "http_requests_total".to_string(),
            labels: vec![],
            last_timestamp: 0,
            retention: Duration::from_secs(60 * 60 * 24), // 1 day
            dedupe_interval: Some(Duration::from_secs(60)), // 1 minute
            significant_digits: None,
            ..Default::default()
        }
    }

    const ELEMENTS_PER_CHUNK: usize = 20;

    fn add_chunk_data(ts: &mut TimeSeries, chunk_count: usize) {
        let sample_count = chunk_count * ELEMENTS_PER_CHUNK;
        let mut samples = generate_random_samples(0, sample_count);
        for chunk_samples in samples.chunks(ELEMENTS_PER_CHUNK) {
            let mut chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);
            chunk.set_data(chunk_samples).unwrap();
            ts.chunks.push(chunk)
        }
        let first_timestamp = samples[0].timestamp;
        let last_timestamp = samples[sample_count - 1].timestamp;
        ts.last_timestamp = last_timestamp;
        ts.total_samples = samples.len();
        ts.first_timestamp = first_timestamp;
    }

    fn get_chunk_boundaries(ts: &mut TimeSeries, idx: usize) -> (Timestamp, Timestamp) {
        let chunk = ts.chunks.get(idx).unwrap();
        (chunk.first_timestamp(), chunk.last_timestamp())
    }

    #[test]
    fn test_add_sample_older_than_retention() {
        let mut ts = create_test_timeseries();
        let old_timestamp = 0; // Very old timestamp
        let result = ts.add(old_timestamp, 1.0, None);
        assert_eq!(result, Err(TsdbError::SampleTooOld));
    }

    #[test]
    fn test_add_sample_at_retention_boundary() {
        let mut ts = create_test_timeseries();
        let current_time = 1000;
        ts.last_timestamp = current_time;

        // Set retention to 1 hour (3600 seconds)
        ts.retention = Duration::from_secs(3600);

        // Add a sample exactly at the retention boundary
        let retention_boundary = current_time + 3600 * 1000; // Convert to milliseconds
        let result = ts.add_sample(retention_boundary, 42.0);

        assert!(result.is_ok());
        assert_eq!(ts.last_timestamp, retention_boundary);
        assert_eq!(ts.last_value, 42.0);
        assert_eq!(ts.total_samples, 1);
    }


    #[test]
    fn test_add_sample_within_dedupe_interval() {
        let mut ts = create_test_timeseries();
        let current_timestamp = 1000;
        ts.last_timestamp = current_timestamp;
        let result = ts.add(current_timestamp + 30, 1.0, None); // Within dedupe interval
        assert_eq!(result, Err(TsdbError::DuplicateSample("New sample encountered in less than dedupe interval".to_string())));
    }

    #[test]
    fn test_add_sample_with_timestamp_less_than_last() {
        let mut ts = create_test_timeseries();
        let current_timestamp = 1000;
        ts.last_timestamp = current_timestamp;
        let result = ts.add(current_timestamp - 1, 1.0, None); // Timestamp less than last
        assert!(result.is_ok());
    }

    #[test]
    fn test_add_sample_with_valid_timestamp() {
        let mut ts = create_test_timeseries();
        let current_timestamp = 1000;
        ts.last_timestamp = current_timestamp;
        let result = ts.add(current_timestamp + 100, 1.0, None); // Valid timestamp
        assert!(result.is_ok());
        assert!(ts.last_timestamp > current_timestamp);
        assert_eq!(ts.last_value, 1.0)
    }

    #[test]
    fn test_add_multiple_samples() {
        let mut ts = create_test_timeseries();
        let samples = vec![
            (100, 1.0),
            (200, 2.0),
            (300, 3.0),
            (400, 4.0),
            (500, 5.0),
        ];

        for (timestamp, value) in samples {
            ts.add_sample(timestamp, value).unwrap();
        }

        assert_eq!(ts.total_samples, 5);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp, 500);
        assert_eq!(ts.last_value, 5.0);
    }

    #[test]
    fn test_add_sample_with_identical_timestamps() {
        let mut ts = create_test_timeseries();
        let timestamp = 1000;

        // Test with KeepLast policy (default)
        ts.duplicate_policy = DuplicatePolicy::KeepLast;

        // Add first sample
        ts.add_sample(timestamp, 1.0).unwrap();
        assert_eq!(ts.total_samples, 1);
        assert_eq!(ts.last_value, 1.0);

        // Add second sample with same timestamp but different value
        ts.add_sample(timestamp, 2.0).unwrap();
        assert_eq!(ts.total_samples, 1);
        assert_eq!(ts.last_value, 2.0);

        // Verify that only the last sample is stored
        let samples = ts.get_range(timestamp, timestamp);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0], Sample { timestamp, value: 2.0 });

        // Test with KeepFirst policy
        ts = create_test_timeseries();
        ts.duplicate_policy = DuplicatePolicy::KeepFirst;

        // Add first sample
        ts.add_sample(timestamp, 1.0).unwrap();
        assert_eq!(ts.total_samples, 1);
        assert_eq!(ts.last_value, 1.0);

        // Add second sample with same timestamp but different value
        ts.add_sample(timestamp, 2.0).unwrap();
        assert_eq!(ts.total_samples, 1);
        assert_eq!(ts.last_value, 1.0);

        // Verify that only the first sample is stored
        let samples = ts.get_range(timestamp, timestamp);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0], Sample { timestamp, value: 1.0 });
    }


    #[test]
    fn test_add_sample_new_chunk() {
        let mut ts = create_test_timeseries();
        ts.chunk_size_bytes = 8; // Set a small chunk size to force new chunk creation

        // Add first sample
        ts.add_sample(1000, 1.0).unwrap();
        assert_eq!(ts.chunks.len(), 1);
        assert_eq!(ts.total_samples, 1);

        // Add second sample, which should create a new chunk
        ts.add_sample(2000, 2.0).unwrap();
        assert_eq!(ts.chunks.len(), 2);
        assert_eq!(ts.total_samples, 2);
        assert_eq!(ts.first_timestamp, 1000);
        assert_eq!(ts.last_timestamp, 2000);
        assert_eq!(ts.last_value, 2.0);
    }

    #[test]
    fn test_label_value_nonexistent_label() {
        let ts = create_test_timeseries();
        assert_eq!(ts.label_value("nonexistent_label"), None);
    }

    #[test]
    fn test_label_value_case_insensitive() {
        let mut ts = create_test_timeseries();
        ts.labels = vec![Label { name: "Status".to_string(), value: "200".to_string() }];

        assert_eq!(ts.label_value("status"), Some(&"200".to_string()));
        assert_eq!(ts.label_value("Status"), Some(&"200".to_string()));
        assert_eq!(ts.label_value("STATUS"), Some(&"200".to_string()));
    }

    #[test]
    fn test_remove_range_start_before_first_timestamp() {
        let mut ts = TimeSeries::default();
        ts.add_sample(100, 1.0).unwrap();
        ts.add_sample(200, 2.0).unwrap();
        ts.add_sample(300, 3.0).unwrap();

        assert_eq!(ts.total_samples, 3);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp, 300);

        ts.remove_range(0, 50).unwrap();

        assert_eq!(ts.total_samples, 3);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp, 300);
    }

    #[test]
    fn test_remove_range_end_after_last_timestamp() {
        let mut ts = TimeSeries::default();
        ts.add_sample(100, 1.0).unwrap();
        ts.add_sample(200, 2.0).unwrap();
        ts.add_sample(300, 3.0).unwrap();

        assert_eq!(ts.total_samples, 3);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp, 300);

        ts.remove_range(150, 400).unwrap();

        assert_eq!(ts.total_samples, 1);
        assert_eq!(ts.first_timestamp, 200);
        assert_eq!(ts.last_timestamp, 200);
    }

    #[test]
    fn test_remove_range_same_chunk() {
        // Arrange
        let mut ts = TimeSeries::default();
        ts.add_sample(1000, 1.0).unwrap();
        ts.add_sample(2000, 2.0).unwrap();
        ts.add_sample(3000, 3.0).unwrap();

        // Act
        let result = ts.remove_range(1500, 2500);

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1); // 1 sample removed
        assert_eq!(ts.total_samples, 2); // 2 samples remaining
        assert_eq!(ts.first_timestamp, 1000);
        assert_eq!(ts.last_timestamp, 3000);
        assert_eq!(ts.chunks.len(), 1); // 1 chunk remaining
    }

    #[test]
    fn test_remove_range_spanning_multiple_chunks() {
        let mut ts = TimeSeries::default();

        // Add samples to chunks
        ts.add_sample(100, 1.0).unwrap();
        ts.add_sample(200, 2.0).unwrap();
        ts.add_sample(300, 3.0).unwrap();
        ts.add_sample(400, 4.0).unwrap();
        ts.add_sample(500, 5.0).unwrap();
        ts.add_sample(600, 6.0).unwrap();

        // Remove range spanning multiple chunks
        ts.remove_range(250, 450).unwrap();

        // Assert samples removed
        assert_eq!(ts.chunks[0].num_samples(), 1); // [100, 1.0]
        assert_eq!(ts.chunks[1].num_samples(), 1); // [600, 6.0]

        // Assert total samples updated
        assert_eq!(ts.total_samples, 2);

        // Assert first and last timestamps updated
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp, 600);
    }


    #[test]
    fn test_remove_range_chunk_boundaries() {
        let mut ts = TimeSeries::default();

        add_chunk_data(&mut ts, 3);
        let (first_start, first_end) = get_chunk_boundaries(&mut ts, 0);
        let (second_start, second_end) = get_chunk_boundaries(&mut ts, 1);
        let (third_start, third_end) = get_chunk_boundaries(&mut ts, 2);

        // Remove range overlapping chunk boundaries
        ts.remove_range(120, 230).unwrap();

        // Validate chunks after removal
        assert_eq!(ts.chunks.len(), 2);
        assert_eq!(ts.chunks[0].first_timestamp(), 100);
        assert_eq!(ts.chunks[0].last_timestamp(), 150);
        assert_eq!(ts.chunks[1].first_timestamp(), 250);
        assert_eq!(ts.chunks[1].last_timestamp(), 300);
    }


    #[test]
    fn test_remove_range_entire_series() {
        let mut ts = TimeSeries::default();
        ts.add_sample(1000, 1.0).unwrap();
        ts.add_sample(2000, 2.0).unwrap();
        ts.add_sample(3000, 3.0).unwrap();

        let start_ts = 1000;
        let end_ts = 3000;

        ts.remove_range(start_ts, end_ts).unwrap();

        assert_eq!(ts.total_samples, 0);
        assert_eq!(ts.first_timestamp, 0);
        assert_eq!(ts.last_timestamp, 0);
        assert!(ts.last_value.is_nan());
        assert!(ts.chunks.is_empty());
    }

    #[test]
    fn test_remove_range_middle_chunk() {
        // Arrange
        let mut ts = TimeSeries::default();

        add_chunk_data(&mut ts, 3);
        let (first_start, first_end) = get_chunk_boundaries(&mut ts, 0);
        let (second_start, second_end) = get_chunk_boundaries(&mut ts, 1);
        let (third_start, third_end) = get_chunk_boundaries(&mut ts, 2);

        // Act
        ts.remove_range(second_start, second_end).unwrap();

        assert_eq!(ts.chunks.len(), 2);
        let first_samples = ts.chunks.get(0).unwrap()
            .iter().collect::<Vec<_>>();

        let third_samples = ts.chunks.get(1).unwrap()
            .iter().collect::<Vec<_>>();

        // Assert
        let actual: Vec<_> = ts.iter().collect();
        let (left, right) = actual.split_at(actual.len() / 2);

        assert_eq!(&first_samples, left);
        assert_eq!(&third_samples, right);
    }

}