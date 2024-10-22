#[cfg(test)]
mod tests {
    use crate::common::types::Sample;
    use crate::error::TsdbError;
    use crate::series::merge::merge_by_capacity;
    use crate::series::test_utils::generate_random_samples;
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 4);

            chunk.clear();

            assert_eq!(chunk.len(), 0);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![]);
        }
    }

    #[test]
    fn test_get_range_empty_chunk() {
        for chunk_type in CHUNK_TYPES {
            let chunk = TimeSeriesChunk::new(chunk_type, 100);

            assert!(chunk.is_empty());

            let result = chunk.get_range(0, 100).unwrap();

            assert!(result.is_empty());
        }
    }

    #[test]
    fn test_get_range_single_sample() {
        let sample = Sample { timestamp: 10, value: 1.0 };

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.add_sample(&sample).unwrap();

            assert_eq!(chunk.len(), 1);

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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 3);

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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 3);

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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 3);

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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 4);

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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 4);

            chunk.remove_range(20, 30).unwrap();

            assert_eq!(chunk.len(), 2);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![
                Sample { timestamp: 10, value: 1.0 },
                Sample { timestamp: 40, value: 4.0 },
            ]);
        }
    }

    #[test]
    fn test_remove_range_single_sample() {
        let sample = Sample { timestamp: 10, value: 1.0 };

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);

            chunk.add_sample(&sample).unwrap();

            assert_eq!(chunk.len(), 1);

            chunk.remove_range(10, 20).unwrap();

            assert_eq!(chunk.len(), 0);
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

            assert_eq!(chunk.len(), 3);

            chunk.remove_range(10, 10).unwrap();

            assert_eq!(chunk.len(), 1);
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&[Sample { timestamp: 20, value: 5.0 }]).unwrap();

            let result = chunk.merge_samples(&samples, Some(DuplicatePolicy::KeepFirst));

            assert_eq!(result.unwrap(), 3);
            assert_eq!(chunk.len(), 3);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![
                Sample { timestamp: 10, value: 1.0 },
                Sample { timestamp: 20, value: 5.0 },
                Sample { timestamp: 30, value: 4.0 },
            ]);
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&[Sample { timestamp: 20, value: 5.0 }]).unwrap();

            let result = chunk.merge_samples(&samples, Some(DuplicatePolicy::KeepLast));

            assert_eq!(result.unwrap(), 3);
            assert_eq!(chunk.len(), 3);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![
                Sample { timestamp: 10, value: 1.0 },
                Sample { timestamp: 20, value: 3.0 },
                Sample { timestamp: 30, value: 4.0 },
            ]);
            // assert_eq!(blocked.len(), 1);
        }
    }

    #[test]
    fn test_merge_samples_exceed_capacity() {
        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
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

            let result = chunk.merge_samples(&samples_to_merge, Some(DuplicatePolicy::Block));

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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.add_sample(&Sample { timestamp: 20, value: 5.0 }).unwrap();

            let result = chunk.merge_samples(&samples, Some(DuplicatePolicy::Sum));

            assert_eq!(result.unwrap(), 3);
            assert_eq!(chunk.len(), 3);

            let merged_samples = chunk.get_range(0, 40).unwrap();
            assert_eq!(merged_samples, vec![
                Sample { timestamp: 10, value: 1.0 },
                Sample { timestamp: 20, value: 10.0 }, // 5.0 + 2.0 + 3.0
                Sample { timestamp: 30, value: 4.0 },
            ]);
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&[Sample { timestamp: 10, value: 0.0 }, Sample { timestamp: 20, value: 0.0 }]).unwrap();

            assert_eq!(chunk.len(), 2);
            assert_eq!(chunk.first_timestamp(), 10);
            assert_eq!(chunk.last_timestamp(), 20);

            let result = chunk.merge_samples(&samples, Some(DuplicatePolicy::Block));

            assert_eq!(result.unwrap(), 2);
            assert_eq!(chunk.len(), 4);
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&existing_samples).unwrap();

            assert_eq!(chunk.len(), 3);

            let result = chunk.merge_samples(&new_samples, Some(DuplicatePolicy::Block));

            assert_eq!(result.unwrap(), 3);
            assert_eq!(chunk.len(), 6);
            // assert_eq!(blocked.len(), 1);
            // assert!(blocked.contains(&20));

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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);

            // First merge should add all samples
            let result = chunk.merge_samples(&samples, Some(DuplicatePolicy::Block));
            assert_eq!(result.unwrap(), 3, "{}: Expected 3 samples to be merged", chunk_type);

            // Second merge with same samples should add no new samples
            let result = chunk.merge_samples(&samples, Some(DuplicatePolicy::Block));
            assert_eq!(result.unwrap(), 0, "{}: Expected 0 samples to be merged on second attempt", chunk_type);

            // Merge with new samples should add only the new ones
            let new_samples = vec![
                Sample { timestamp: 40, value: 4.0 },
                Sample { timestamp: 20, value: 5.0 }, // Duplicate timestamp
            ];
            let result = chunk.merge_samples(&new_samples, Some(DuplicatePolicy::Block));
            assert_eq!(result.unwrap(), 1, "{}: Expected 1 new sample to be merged", chunk_type);
        }
    }

    const ELEMENTS_PER_CHUNK: usize = 60;

    fn saturate_chunk(chunk: &mut TimeSeriesChunk) {
        let estimated_capacity = chunk.estimate_remaining_sample_capacity();
        let samples = generate_random_samples(0, estimated_capacity * 2);
        let (normal, spillage) = samples.split_at(estimated_capacity);
        chunk.set_data(normal).unwrap();
        for sample in spillage {
            match chunk.add_sample(sample) {
                Ok(_) => (),
                Err(TsdbError::CapacityFull(_)) => {
                    break;
                },
                _ => {}
            };
        }
    }

    #[test]
    fn test_merge_by_capacity_with_empty_source_chunk() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);

        // Ensure the source chunk is empty
        assert!(src_chunk.is_empty());

        let result = merge_by_capacity(&mut dest_chunk, &mut src_chunk, 0, Some(DuplicatePolicy::KeepLast));

        assert_eq!(result, Ok(None));
    }

    #[test]
    fn test_merge_by_capacity_exact_capacity() {
        // possibly dont need
        let mut dest_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);

        // Fill the source chunk with samples
        let samples = generate_random_samples(0, ELEMENTS_PER_CHUNK);
        src_chunk.set_data(&samples).unwrap();

        // Ensure destination chunk has exactly the same remaining capacity as source chunk's size
        assert_eq!(dest_chunk.estimate_remaining_sample_capacity(), src_chunk.len());

        // Perform the merge
        let result = merge_by_capacity(&mut dest_chunk, &mut src_chunk, 0, Some(DuplicatePolicy::KeepLast));

        // Verify the result
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(samples.len()));

        // Ensure source chunk is cleared
        assert!(src_chunk.is_empty());

        // Ensure destination chunk contains all samples from source
        let dest_samples = dest_chunk.get_range(0, samples.last().unwrap().timestamp).unwrap();
        assert_eq!(dest_samples.len(), samples.len());
        assert_eq!(dest_samples, samples);
    }

    #[test]
    fn test_merge_by_capacity_partial_merge() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);

        let capacity = dest_chunk.estimate_remaining_sample_capacity();

        // Fill the destination chunk to have more than a quarter but less than full capacity of source
        let dest_samples = generate_random_samples(0, capacity / 2);
        dest_chunk.set_data(&dest_samples).unwrap();

        let dest_samples_count = dest_samples.len();

        // Fill the source chunk with samples
        saturate_chunk(&mut src_chunk);

        let remaining_capacity = dest_chunk.estimate_remaining_sample_capacity();

        // Perform the merge
        let result = merge_by_capacity(&mut dest_chunk, &mut src_chunk, 0, Some(DuplicatePolicy::KeepLast)).unwrap();

        // Check that a partial merge occurred
        assert!(result.is_some());
        let merged_samples_count = result.unwrap();

        let count_merged = dest_chunk.len() - dest_samples_count;
        // Verify that the source chunk still contains the remaining samples
        assert_eq!(src_chunk.len(), dest_chunk.len() - count_merged);
    }

    #[test]
    fn test_merge_by_capacity_dest_less_than_quarter_capacity() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 2048);
        let mut src_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 2048);

        // Fill the source chunk with samples
        let samples = generate_random_samples(0, 100);
        src_chunk.set_data(&samples).unwrap();

        // Ensure the destination chunk has less than a quarter of the source chunk's capacity
        let dest_samples = generate_random_samples(0, 10);
        dest_chunk.set_data(&dest_samples).unwrap();

        // Calculate remaining capacity in destination chunk
        let remaining_capacity = dest_chunk.estimate_remaining_sample_capacity();

        // Ensure remaining capacity is less than a quarter of the source chunk's sample count
        assert!(remaining_capacity < src_chunk.len() / 4);

        let result = merge_by_capacity(&mut dest_chunk, &mut src_chunk, 0, Some(DuplicatePolicy::KeepLast));

        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_merge_by_capacity_with_duplicate_timestamps_block_policy() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);

        // Add samples to the source chunk with duplicate timestamps
        let samples = vec![
            Sample { timestamp: 1000, value: 1.0 },
            Sample { timestamp: 1000, value: 2.0 },
            Sample { timestamp: 2000, value: 3.0 },
        ];
        src_chunk.set_data(&samples).unwrap();

        // Set the duplicate policy to Block
        let duplicate_policy = Some(DuplicatePolicy::Block);

        // Attempt to merge the source chunk into the destination chunk
        let result = merge_by_capacity(&mut dest_chunk, &mut src_chunk, 0, duplicate_policy);

        // Verify the merge result
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(2)); // Only two unique timestamps should be merged

        // Verify the destination chunk contains the correct samples
        let merged_samples = dest_chunk.get_range(0, 3000).unwrap();
        assert_eq!(merged_samples.len(), 2);
        assert_eq!(merged_samples[0], Sample { timestamp: 1000, value: 1.0 }); // First occurrence
        assert_eq!(merged_samples[1], Sample { timestamp: 2000, value: 3.0 });
    }

    #[test]
    fn test_merge_by_capacity_with_empty_destination() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);

        // Add some samples to the source chunk
        let samples = vec![
            Sample { timestamp: 100, value: 1.0 },
            Sample { timestamp: 200, value: 2.0 },
            Sample { timestamp: 300, value: 3.0 },
        ];
        src_chunk.set_data(&samples).unwrap();

        // Ensure destination chunk is empty
        assert!(dest_chunk.is_empty());

        // Perform the merge
        let result = merge_by_capacity(&mut dest_chunk, &mut src_chunk, 0, Some(DuplicatePolicy::KeepLast));

        // Verify the merge result
        assert_eq!(result.unwrap(), Some(samples.len()));
        assert!(src_chunk.is_empty());
        assert_eq!(dest_chunk.len(), samples.len());

        // Verify the samples in the destination chunk
        let merged_samples = dest_chunk.get_range(100, 300).unwrap();
        assert_eq!(merged_samples, samples);
    }

    #[test]
    fn test_merge_by_capacity_clears_source_after_full_merge() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkCompression::Uncompressed, 1024);

        // Populate source chunk with samples
        let samples = generate_random_samples(0, ELEMENTS_PER_CHUNK);
        src_chunk.set_data(&samples).unwrap();

        // Ensure destination chunk has enough capacity for a full merge
        let remaining_capacity = dest_chunk.estimate_remaining_sample_capacity();
        assert!(remaining_capacity >= ELEMENTS_PER_CHUNK);

        // Perform the merge
        let min_timestamp = samples[0].timestamp;
        let result = merge_by_capacity(&mut dest_chunk, &mut src_chunk, min_timestamp, Some(DuplicatePolicy::KeepLast));

        // Verify the result
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(ELEMENTS_PER_CHUNK));
        assert!(src_chunk.is_empty());
    }
}