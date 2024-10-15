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
            assert_eq!(chunk.get_samples(0, 100).unwrap(), vec![]);
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
            assert_eq!(chunk.get_samples(0, 100).unwrap(), vec![
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
            assert_eq!(chunk.get_samples(0, 100).unwrap(), vec![]);
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
            assert_eq!(chunk.get_samples(0, 100).unwrap(), vec![
                Sample { timestamp: 20, value: 3.0 },
            ]);
        }
    }
}