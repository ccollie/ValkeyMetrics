#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use crate::common::types::Sample;
    use crate::series::{Chunk, ChunkCompression, DuplicatePolicy, TimeSeriesChunk};

    const CHUNK_TYPES: [ChunkCompression; 3] = [
        ChunkCompression::Uncompressed,
        ChunkCompression::Gorilla,
        ChunkCompression::Pco,
    ];

    #[test]
    fn test_clear_chunk_with_multiple_samples() {
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 30, value: 3.0 },
            Sample { timestamp: 40, value: 4.0 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.num_samples(), 4);

            chunk.clear();

            assert_eq!(chunk.num_samples(), 0);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![]);
        }
    }

    #[test]
    fn test_get_range_empty_chunk() {
        for chunk_type in &CHUNK_TYPES {
            let chunk = TimeSeriesChunk::new(*chunk_type, 100);

            assert!(chunk.is_empty());

            let result = chunk.get_range(0, 100).unwrap();

            assert!(result.is_empty());
        }
    }

    #[test]
    fn test_get_range_single_sample() {
        let sample = Sample { timestamp: 10, value: 1.0 };

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);
            chunk.add_sample(&sample).unwrap();

            assert_eq!(chunk.num_samples(), 1);

            let result = chunk.get_range(0, 20).unwrap();
            assert_eq!(result.len(), 1, "{}: get_range_single_sample - expected 1 sample, got {}", chunk_type, result.len());
            assert_eq!(result[0], sample);

            let empty_result = chunk.get_range(20, 30).unwrap();
            assert!(empty_result.is_empty());
        }
    }

    #[test]
    fn test_get_range_start_equals_end() {
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 30, value: 3.0 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.num_samples(), 3);

            let result = chunk.get_range(20, 20).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0], Sample { timestamp: 20, value: 2.0 });

            let empty_result = chunk.get_range(15, 15).unwrap();
            assert!(empty_result.is_empty(), "{}: Expected empty result, got {:?}", chunk_type, empty_result);
        }
    }

    #[test]
    fn test_get_range_start_greater_than_end() {
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 30, value: 3.0 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.num_samples(), 3);

            let result = chunk.get_range(30, 10).unwrap();
            assert!(result.is_empty());
        }
    }

    #[test]
    fn test_get_range_full_range() {
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 30, value: 3.0 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.num_samples(), 3);

            let result = chunk.get_range(0, 40).unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(result, samples);
        }
    }

    #[test]
    fn test_get_range_between_samples() {
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 30, value: 3.0 },
            Sample { timestamp: 40, value: 4.0 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.num_samples(), 4);

            let result = chunk.get_range(15, 35).unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0], Sample { timestamp: 20, value: 2.0 });
            assert_eq!(result[1], Sample { timestamp: 30, value: 3.0 });
        }
    }

    #[test]
    fn test_remove_range_chunk() {
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 30, value: 3.0 },
            Sample { timestamp: 40, value: 4.0 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.num_samples(), 4);

            chunk.remove_range(20, 30).unwrap();

            assert_eq!(chunk.num_samples(), 2);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![
                Sample { timestamp: 10, value: 1.0 },
                Sample { timestamp: 40, value: 4.0 },
            ]);
        }
    }

    #[test]
    fn test_remove_range_single_sample() {
        let sample = Sample { timestamp: 10, value: 1.0 };

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);

            chunk.add_sample(&sample).unwrap();

            assert_eq!(chunk.num_samples(), 1);

            chunk.remove_range(10, 20).unwrap();

            assert_eq!(chunk.num_samples(), 0);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![]);
        }
    }

    #[test]
    fn test_remove_range_same_timestamp() {
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 10, value: 2.0 },
            Sample { timestamp: 20, value: 3.0 },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.num_samples(), 3);

            chunk.remove_range(10, 10).unwrap();

            assert_eq!(chunk.num_samples(), 1);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![
                Sample { timestamp: 20, value: 3.0 },
            ]);
        }
    }

    #[test]
    fn test_merge_samples_with_duplicate_timestamps_keep_first() {
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 20, value: 3.0 },
            Sample { timestamp: 30, value: 4.0 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);
            chunk.set_data(&[Sample { timestamp: 20, value: 5.0 }]).unwrap();

            let mut blocked = BTreeSet::new();
            let result = chunk.merge_samples(&samples, DuplicatePolicy::KeepFirst, &mut blocked);

            assert_eq!(result.unwrap(), 3);
            assert_eq!(chunk.num_samples(), 3);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![
                Sample { timestamp: 10, value: 1.0 },
                Sample { timestamp: 20, value: 5.0 },
                Sample { timestamp: 30, value: 4.0 },
            ]);
            assert_eq!(blocked.len(), 1);
            assert!(blocked.contains(&20));
        }
    }

    #[test]
    fn test_merge_samples_with_duplicate_timestamps_keep_last() {
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 20, value: 3.0 },
            Sample { timestamp: 30, value: 4.0 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);
            chunk.set_data(&[Sample { timestamp: 20, value: 5.0 }]).unwrap();

            let mut blocked = BTreeSet::new();
            let result = chunk.merge_samples(&samples, DuplicatePolicy::KeepLast, &mut blocked);

            assert_eq!(result.unwrap(), 3);
            assert_eq!(chunk.num_samples(), 3);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![
                Sample { timestamp: 10, value: 1.0 },
                Sample { timestamp: 20, value: 3.0 },
                Sample { timestamp: 30, value: 4.0 },
            ]);
            assert_eq!(blocked.len(), 1);
            assert!(blocked.contains(&20));
        }
    }

    #[test]
    fn test_merge_samples_exceed_capacity() {
        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);
            let initial_samples = vec![
                Sample { timestamp: 10, value: 1.0 },
                Sample { timestamp: 20, value: 2.0 },
            ];
            chunk.set_data(&initial_samples).unwrap();

            let samples_to_merge = vec![
                Sample { timestamp: 30, value: 3.0 },
                Sample { timestamp: 40, value: 4.0 },
                Sample { timestamp: 50, value: 5.0 },
            ];

            let mut blocked = BTreeSet::new();
            let result = chunk.merge_samples(&samples_to_merge, DuplicatePolicy::Block, &mut blocked);

            assert!(result.is_ok());
            let merged_count = result.unwrap();
            assert!(merged_count < samples_to_merge.len(),
                "{}: Expected fewer samples to be merged due to capacity limit", chunk_type);

            let all_samples = chunk.get_range(0, 100).unwrap();
            assert!(all_samples.len() > initial_samples.len(),
                "{}: Expected some samples to be merged", chunk_type);
            assert!(all_samples.len() < initial_samples.len() + samples_to_merge.len(),
                "{}: Expected not all samples to be merged due to capacity limit", chunk_type);
        }
    }

    #[test]
    fn test_merge_samples_with_duplicate_timestamps_sum() {
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 20, value: 3.0 },
            Sample { timestamp: 30, value: 4.0 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);
            chunk.add_sample(&Sample { timestamp: 20, value: 5.0 }).unwrap();

            let mut blocked = BTreeSet::new();
            let result = chunk.merge_samples(&samples, DuplicatePolicy::Sum, &mut blocked);

            assert_eq!(result.unwrap(), 3);
            assert_eq!(chunk.num_samples(), 3);

            let merged_samples = chunk.get_range(0, 40).unwrap();
            assert_eq!(merged_samples, vec![
                Sample { timestamp: 10, value: 1.0 },
                Sample { timestamp: 20, value: 10.0 }, // 5.0 + 2.0 + 3.0
                Sample { timestamp: 30, value: 4.0 },
            ]);

            assert!(blocked.is_empty());
        }
    }

    #[test]
    fn test_merge_samples_outside_range() {
        let samples = vec![
            Sample { timestamp: 5, value: 1.0 },
            Sample { timestamp: 15, value: 2.0 },
            Sample { timestamp: 25, value: 3.0 },
            Sample { timestamp: 35, value: 4.0 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);
            chunk.set_data(&[Sample { timestamp: 10, value: 0.0 }, Sample { timestamp: 20, value: 0.0 }]).unwrap();

            assert_eq!(chunk.num_samples(), 2);
            assert_eq!(chunk.first_timestamp(), 10);
            assert_eq!(chunk.last_timestamp(), 20);

            let mut blocked = BTreeSet::new();
            let result = chunk.merge_samples(&samples, DuplicatePolicy::Block, &mut blocked);

            assert_eq!(result.unwrap(), 2);
            assert_eq!(chunk.num_samples(), 4);
            assert_eq!(chunk.first_timestamp(), 5);
            assert_eq!(chunk.last_timestamp(), 35);

            let range = chunk.get_range(0, 40).unwrap();
            assert_eq!(range, vec![
                Sample { timestamp: 5, value: 1.0 },
                Sample { timestamp: 10, value: 0.0 },
                Sample { timestamp: 20, value: 0.0 },
                Sample { timestamp: 35, value: 4.0 },
            ]);
        }
    }

    #[test]
    fn test_merge_samples_with_mixed_timestamps() {
        let existing_samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 30, value: 3.0 },
        ];

        let new_samples = vec![
            Sample { timestamp: 15, value: 1.5 },
            Sample { timestamp: 20, value: 2.5 },
            Sample { timestamp: 25, value: 2.5 },
            Sample { timestamp: 35, value: 3.5 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);
            chunk.set_data(&existing_samples).unwrap();

            assert_eq!(chunk.num_samples(), 3);

            let mut blocked = BTreeSet::new();
            let result = chunk.merge_samples(&new_samples, DuplicatePolicy::Block, &mut blocked);

            assert_eq!(result.unwrap(), 3);
            assert_eq!(chunk.num_samples(), 6);
            assert_eq!(blocked.len(), 1);
            assert!(blocked.contains(&20));

            let all_samples = chunk.get_range(0, 40).unwrap();
            assert_eq!(all_samples, vec![
                Sample { timestamp: 10, value: 1.0 },
                Sample { timestamp: 15, value: 1.5 },
                Sample { timestamp: 20, value: 2.0 },
                Sample { timestamp: 25, value: 2.5 },
                Sample { timestamp: 30, value: 3.0 },
                Sample { timestamp: 35, value: 3.5 },
            ]);
        }
    }

    #[test]
    fn test_merge_samples_return_value() {
        let samples = vec![
            Sample { timestamp: 10, value: 1.0 },
            Sample { timestamp: 20, value: 2.0 },
            Sample { timestamp: 30, value: 3.0 },
        ];

        for chunk_type in &CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(*chunk_type, 100);
            let mut blocked = BTreeSet::new();

            // First merge should add all samples
            let result = chunk.merge_samples(&samples, DuplicatePolicy::Block, &mut blocked);
            assert_eq!(result.unwrap(), 3, "{}: Expected 3 samples to be merged", chunk_type);

            // Second merge with same samples should add no new samples
            let result = chunk.merge_samples(&samples, DuplicatePolicy::Block, &mut blocked);
            assert_eq!(result.unwrap(), 0, "{}: Expected 0 samples to be merged on second attempt", chunk_type);

            // Merge with new samples should add only the new ones
            let new_samples = vec![
                Sample { timestamp: 40, value: 4.0 },
                Sample { timestamp: 20, value: 5.0 }, // Duplicate timestamp
            ];
            let result = chunk.merge_samples(&new_samples, DuplicatePolicy::Block, &mut blocked);
            assert_eq!(result.unwrap(), 1, "{}: Expected 1 new sample to be merged", chunk_type);
        }
    }

}