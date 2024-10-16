#[cfg(test)]
mod tests {
    use crate::common::types::Sample;
    use crate::series::{Chunk, ChunkCompression, TimeSeriesChunk};

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
}