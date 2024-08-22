use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::stream_aggregation::dedup::DedupAggr;
use crate::stream_aggregation::PushSample;

#[test]
fn test_dedup_aggr_serial() {
    let mut da = DedupAggr::new();

    const SERIES_COUNT: usize = 100_000;
    let mut expected_samples_map = HashMap::new();
    for i in 0..2 {
        let mut samples = Vec::with_capacity(SERIES_COUNT);
        for j in 0..SERIES_COUNT {
            let sample = PushSample {
                key: format!("key_{}", j),
                value: (i + j) as f64,
                timestamp: 0,
            };
            expected_samples_map.insert(sample.key.clone(), sample.clone());
            samples.push(sample);
        }
        da.push_samples(samples, 0, 0);
    }

    if da.size_bytes() > 5_000_000 {
        panic!("too big dedupAggr state before flush: {} bytes; it shouldn't exceed 5_000_000 bytes", da.size_bytes());
    }
    if da.items_count() != SERIES_COUNT as u64 {
        panic!("unexpected itemsCount; got {}; want {}", da.items_count(), SERIES_COUNT);
    }

    let flushed_samples_map = Arc::new(Mutex::new(HashMap::new()));
    let flush_samples = |samples: Vec<PushSample>, _: i64, _: usize| {
        let mut flushed_samples_map = flushed_samples_map.lock().unwrap();
        for sample in samples {
            flushed_samples_map.insert(sample.key.clone(), sample);
        }
    };

    let flush_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;
    da.flush(flush_samples, flush_timestamp, 0, 0);

    let flushed_samples_map = flushed_samples_map.lock().unwrap();
    if expected_samples_map != *flushed_samples_map {
        panic!("unexpected samples;\ngot\n{:?}\nwant\n{:?}", flushed_samples_map, expected_samples_map);
    }

    if da.size_bytes() > 17_000 {
        panic!("too big dedupAggr state after flush; {} bytes; it shouldn't exceed 17_000 bytes", da.size_bytes());
    }
    if da.items_count() != 0 {
        panic!("unexpected non-zero itemsCount after flush; got {}", da.items_count());
    }
}

#[test]
fn test_dedup_aggr_concurrent() {
    const CONCURRENCY: usize = 5;
    const SERIES_COUNT: usize = 10_000;
    let mut da = Arc::new(DedupAggr::new());

    let mut handles = Vec::new();
    for _ in 0..CONCURRENCY {
        let da = da.clone();
        let handle = thread::spawn(move || {
            for i in 0..10 {
                let mut samples = Vec::with_capacity(SERIES_COUNT);
                for j in 0..SERIES_COUNT {
                    let sample = PushSample {
                        key: format!("key_{}", j),
                        value: (i + j) as f64,
                        timestamp: 0,
                    };
                    samples.push(sample);
                }
                da.push_samples(samples, 0, 0);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}