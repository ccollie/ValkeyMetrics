#[cfg(test)]
mod tests {
    use crate::common::types::{Label, Sample, Timestamp};
    use crate::error_consts;
    use crate::series::test_utils::generate_random_samples;
    use crate::series::{Chunk, ChunkCompression, DuplicatePolicy, TimeSeries, TimeSeriesChunk};
    use std::time::Duration;
    use metricsql_runtime::prelude::TimestampTrait;
    use valkey_module::ValkeyError;

    fn create_test_timeseries() -> TimeSeries {
        TimeSeries {
            id: 1,
            metric_name: "http_requests_total".to_string(),
            labels: vec![],
            last_timestamp: 0,
            retention: Duration::from_secs(60 * 60 * 24), // 1 day
            dedupe_interval: Some(Duration::from_secs(60)), // 1 minute
            rounding: None,
            duplicate_policy: DuplicatePolicy::KeepLast,
            chunk_size_bytes: 1024,
            chunks: vec![],
            total_samples: 0,
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
        let retention_period = ts.retention.as_millis() as i64;
        let old_timestamp = ts.last_timestamp - retention_period - 1;

        let result = ts.add(old_timestamp, 42.0, None);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), error_consts::SAMPLE_TOO_OLD);
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
        match result {
            Ok(_) => assert!(false), // Should have failed due to dedupe interval
            Err(e) => assert_eq!(e.to_string(), error_consts::DUPLICATE_SAMPLE),
        }
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

    /// ----- ADD

    #[test]
    fn test_add_sample_updates_last_timestamp_and_value() {
        let mut ts = create_test_timeseries();
        let initial_last_timestamp = ts.last_timestamp;
        let initial_last_value = ts.last_value;

        // Add a sample with a timestamp greater than the last timestamp
        let new_timestamp = initial_last_timestamp + 1000;
        let new_value = 42.0;

        assert!(ts.add(new_timestamp, new_value, None).is_ok());

        // Check if the last timestamp and value are updated correctly
        assert_eq!(ts.last_timestamp, new_timestamp);
        assert_eq!(ts.last_value, new_value);
    }
    ///

    #[test]
    fn test_add_sample_at_dedupe_interval_boundary() {
        let mut ts = create_test_timeseries();
        ts.dedupe_interval = Some(Duration::from_secs(60));

        // Add initial sample
        let initial_timestamp = 1000;
        ts.add(initial_timestamp, 10.0, None).unwrap();

        // Add sample exactly at dedupe interval boundary
        let boundary_timestamp = initial_timestamp + ts.dedupe_interval.unwrap().as_millis() as i64;
        ts.add(boundary_timestamp, 20.0, None).unwrap();

        assert_eq!(ts.total_samples, 2);
        assert_eq!(ts.last_timestamp, boundary_timestamp);
        assert_eq!(ts.last_value, 20.0);
    }

    #[test]
    fn test_add_sample_with_overflow_timestamp() {
        let mut ts = create_test_timeseries();
        ts.dedupe_interval = Some(Duration::from_secs(60)); // 1 minute deduplication interval

        // Add a sample with a timestamp close to the maximum value for Timestamp
        let max_timestamp = Timestamp::MAX - 30; // 30 milliseconds before overflow
        ts.add(max_timestamp, 1.0, None).unwrap();

        // Try to add another sample with a timestamp that would cause overflow when calculating deduplication interval
        let overflow_timestamp = Timestamp::MAX - 20; // 20 milliseconds before overflow

        let result = ts.add(overflow_timestamp, 2.0, None);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), error_consts::DUPLICATE_SAMPLE);
    }

    #[test]
    fn test_add_sample_with_zero_dedupe_interval_and_equal_timestamp() {
        let mut ts = create_test_timeseries();
        ts.dedupe_interval = Some(Duration::from_secs(0)); // Set deduplication interval to zero

        // Add a sample to set the last_timestamp
        let initial_timestamp = 1000;
        ts.add(initial_timestamp, 1.0, None).unwrap();

        // Try to add another sample with the same timestamp
        let result = ts.add(initial_timestamp, 2.0, None);

        // Expect an error due to deduplication policy
        assert!(result.is_ok());
    }

    #[test]
    fn test_add_sample_with_equal_timestamp_block() {
        let mut ts = create_test_timeseries();
        ts.dedupe_interval = Some(Duration::from_secs(0)); // Set deduplication interval to zero

        // Add a sample to set the last_timestamp
        let initial_timestamp = 1000;
        ts.add(initial_timestamp, 1.0, None).unwrap();

        // Try to add another sample with the same timestamp
        let result = ts.add(initial_timestamp, 2.0, Some(DuplicatePolicy::Block));

        // Expect an error due to deduplication policy
        assert!(matches!(result, Err(ValkeyError::Str(err)) if err == error_consts::DUPLICATE_SAMPLE));
    }

    #[test]
    fn test_add_sample_with_duplicate_timestamp_keep_first() {
        let mut ts = create_test_timeseries();
        add_chunk_data(&mut ts, 1);
        let sample_value = 123.456;
        let duplicate_timestamp = ts.last_timestamp;
        let duplicate_sample = Sample {
            timestamp: duplicate_timestamp,
            value: sample_value,
        };
        let res = ts.add(duplicate_timestamp, sample_value, Some(DuplicatePolicy::KeepFirst));
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().to_string(),
            error_consts::DUPLICATE_SAMPLE.to_string()
        );
        assert_eq!(ts.last_value, 0.0);
    }

    #[test]
    fn test_add_duplicate_timestamps_keep_last() {
        let mut ts = create_test_timeseries();
        ts.duplicate_policy = DuplicatePolicy::KeepLast;

        let samples = vec![
            Sample { timestamp: 1, value: 10.0 },
            Sample { timestamp: 2, value: 20.0 },
            Sample { timestamp: 2, value: 25.0 },
            Sample { timestamp: 3, value: 30.0 },
            Sample { timestamp: 3, value: 35.0 },
        ];

        for sample in samples {
            ts.add(sample.timestamp, sample.value, None).unwrap();
        }

        assert_eq!(ts.total_samples, 3);
        assert_eq!(ts.last_value, 35.0);

        let expected_samples = vec![
            Sample { timestamp: 2, value: 25.0 },
            Sample { timestamp: 3, value: 35.0 },
        ];

        assert_eq!(ts.get_range(1, 3), expected_samples);
    }

    /***** TRIM ******/

    #[test]
    fn test_label_value_nonexistent_label() {
        let ts = create_test_timeseries();
        assert_eq!(ts.label_value("nonexistent_label"), None);
    }

    #[test]
    fn test_trim_all_chunks() {
        let mut ts = create_test_timeseries();
        add_chunk_data(&mut ts, 3);

        // Set retention to a very small value to ensure all chunks are older
        ts.retention = Duration::from_millis(1);

        // Store the original sample count
        let original_samples = ts.total_samples;

        // Advance the last_timestamp to make all chunks older than retention
        ts.last_timestamp += 1000;

        // Trim the time series
        ts.trim().unwrap();

        // Check if all chunks are removed
        assert!(ts.chunks.is_empty());

        // Check if total_samples is zero
        assert_eq!(ts.total_samples, 0);

        // Check if first_timestamp and last_timestamp are reset
        assert_eq!(ts.first_timestamp, 0);
        assert_eq!(ts.last_timestamp, 0);

        // Check if last_value is NaN
        assert!(ts.last_value.is_nan());

        // Ensure all samples were deleted
        assert_eq!(original_samples, ts.total_samples + original_samples);
    }

    #[test]
    fn test_trim_min_timestamp_equal_to_chunk_last_timestamp() {
        let mut ts = create_test_timeseries();
        add_chunk_data(&mut ts, 3);

        let (_, last_timestamp_chunk1) = get_chunk_boundaries(&mut ts, 0);
        ts.retention = Duration::from_secs((last_timestamp_chunk1 - ts.first_timestamp + 1) as u64);

        let initial_samples = ts.total_samples;
        let initial_chunks = ts.chunks.len();

        ts.trim().unwrap();

        assert_eq!(ts.chunks.len(), initial_chunks - 1, "First chunk should be removed");
        assert!(ts.total_samples < initial_samples, "Total samples should decrease");
        assert_eq!(ts.first_timestamp, last_timestamp_chunk1 + 1, "First timestamp should be updated");
        assert_eq!(ts.chunks[0].first_timestamp(), last_timestamp_chunk1 + 1, "New first chunk should start after previous chunk's last timestamp");
    }

    #[test]
    fn test_trim_partial_chunk() {
        let mut ts = create_test_timeseries();
        add_chunk_data(&mut ts, 2);

        // Set retention to remove part of the first chunk
        let retention = Duration::from_secs(30);
        ts.retention = retention;

        let original_total_samples = ts.total_samples;
        let original_first_timestamp = ts.first_timestamp;
        let (_, original_last_timestamp) = get_chunk_boundaries(&mut ts, 1);

        // Calculate the expected min_timestamp
        let expected_min_timestamp = original_last_timestamp - retention.as_millis() as i64;

        // Perform trim operation
        ts.trim().unwrap();

        // Check that only part of the first chunk was removed
        assert!(ts.chunks.len() == 2);
        assert!(ts.total_samples < original_total_samples);
        assert!(ts.first_timestamp > original_first_timestamp);
        assert!(ts.first_timestamp >= expected_min_timestamp);
        assert_eq!(ts.last_timestamp, original_last_timestamp);

        // Verify the first chunk's start time
        let (new_first_timestamp, _) = get_chunk_boundaries(&mut ts, 0);
        assert!(new_first_timestamp >= expected_min_timestamp);
    }

    #[test]
    fn test_trim_single_chunk_partial_removal() {
        let mut ts = create_test_timeseries();
        ts.retention = Duration::from_secs(100);

        // Add a single chunk with samples
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 30, value: 3.0 },
            Sample { timestamp: 40, value: 4.0 },
            Sample { timestamp: 50, value: 5.0 },
        ];
        let mut chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);
        chunk.set_data(&samples).unwrap();
        ts.chunks.push(chunk);
        ts.total_samples = samples.len();
        ts.first_timestamp = 10;
        ts.last_timestamp = 50;

        // Set last_timestamp to simulate passage of time
        ts.last_timestamp = 130;

        // Trim the time series
        ts.trim().unwrap();

        // Check the result
        assert_eq!(ts.total_samples, 3);
        assert_eq!(ts.first_timestamp, 30);
        assert_eq!(ts.last_timestamp, 50);

        let remaining_samples = ts.get_range(0, 1000);
        assert_eq!(remaining_samples.len(), 3);
        assert_eq!(remaining_samples[0], Sample { timestamp: 30, value: 3.0 });
        assert_eq!(remaining_samples[1], Sample { timestamp: 40, value: 4.0 });
        assert_eq!(remaining_samples[2], Sample { timestamp: 50, value: 5.0 });
    }

    #[test]
fn test_trim_updates_timestamps() {
    let mut ts = create_test_timeseries();
    add_chunk_data(&mut ts, 3);

    let original_first_timestamp = ts.first_timestamp;
    let original_last_timestamp = ts.last_timestamp;

    // Set retention to remove the first chunk
    ts.retention = Duration::from_secs((ts.last_timestamp - ts.first_timestamp) as u64 / 2);

    ts.trim().unwrap();

    assert!(ts.first_timestamp > original_first_timestamp, "first_timestamp should be updated after trimming");
    assert_eq!(ts.last_timestamp, original_last_timestamp, "last_timestamp should remain unchanged after trimming");
    assert!(ts.chunks.len() < 3, "Some chunks should be removed after trimming");
    assert!(!ts.chunks.is_empty(), "TimeSeries should not be empty after trimming");
}

    #[test]
    fn test_trim_single_sample() {
        let mut ts = create_test_timeseries();
        let now = Timestamp::now();
        ts.add(now, 42.0, None).unwrap();

        // Set retention to 1 second
        ts.retention = Duration::from_secs(1);

        // Advance time by 2 seconds
        let future = now + 2000;

        // Trim the series
        ts.trim().unwrap();

        // The single sample should be removed
        assert!(ts.is_empty());
        assert_eq!(ts.first_timestamp, 0);
        assert_eq!(ts.last_timestamp, 0);
        assert!(ts.last_value.is_nan());
        assert_eq!(ts.total_samples, 0);
        assert!(ts.chunks.is_empty());
    }

    #[test]
    fn test_trim_updates_total_samples() {
        let mut ts = create_test_timeseries();
        add_chunk_data(&mut ts, 3);  // Add 3 chunks of data
        let initial_samples = ts.total_samples;

        // Set retention to remove half of the data
        ts.retention = Duration::from_secs((ts.last_timestamp - ts.first_timestamp) as u64 / 2);

        ts.trim().unwrap();

        assert!(ts.total_samples < initial_samples, "Total samples should decrease after trimming");
        assert_eq!(ts.total_samples, ts.chunks.iter().map(|c| c.num_samples()).sum::<usize>(),
                   "Total samples should match the sum of samples in remaining chunks");
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