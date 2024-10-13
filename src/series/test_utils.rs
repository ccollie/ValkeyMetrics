use crate::error::TsdbError;
use crate::series::{Chunk, TimeSeriesChunk};

use crate::common::types::Sample;
use rand::{Rng, SeedableRng};

pub fn generate_random_samples(seed: u64, vec_size: usize) -> Vec<Sample> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

    let mut timestamp: i64 = rng.gen_range(1234567890..1357908642);
    let mut vec = Vec::with_capacity(vec_size);

    let mut value: f64 = if rng.gen_bool(0.5) {
        rng.gen_range(-100000000.0..1000000.0)
    } else {
        rng.gen_range(-10000.0..10000.0)
    };
    vec.push(Sample { timestamp, value });

    for _ in 1..vec_size {
        timestamp += rng.gen_range(1..30);
        if rng.gen_bool(0.33) {
            value += 1.0;
        } else if rng.gen_bool(0.33) {
            value = rng.gen();
        }
        vec.push(Sample { timestamp, value });
    }

    vec
}

pub fn generate_random_test_data_ex(seed: u64, cases: usize) -> Vec<Vec<Sample>> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

    let mut test_cases = Vec::with_capacity(cases);
    for _ in 0..cases {
        let vec_size = rng.gen_range(1..129);

        let vec = generate_random_samples(seed, vec_size);
        test_cases.push(vec);
    }
    test_cases
}

pub fn generate_random_test_data(seed: u64) -> Vec<Sample> {
    let mut cases = generate_random_test_data_ex(seed, 1);
    cases.remove(0)
}

pub fn saturate_chunk(chunk: &mut TimeSeriesChunk) {
    loop {
        let samples = generate_random_samples(13, 1000);
        for sample in samples.iter() {
            match chunk.add_sample(sample) {
                Ok(_) => {}
                Err(TsdbError::CapacityFull(_)) => {
                    break
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        }
    }
}