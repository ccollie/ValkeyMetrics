use rand::Rng;
use crate::common::types::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::storage::{Chunk, TimeSeriesChunk};
use crate::tests::generators::create_rng;

pub fn saturate_chunk(start_ts: Timestamp, chunk: &mut TimeSeriesChunk) -> TsdbResult<()> {
    let mut rng = create_rng(None).unwrap();
    let mut ts: i64 = start_ts;
    loop {
        let sample = Sample {
            timestamp: ts,
            value: rng.gen_range(0.0..100.0),
        };
        ts += rng.gen_range(1000..20000);
        match chunk.add_sample(&sample) {
            Ok(_) => {}
            Err(TsdbError::CapacityFull(_)) => {
                break
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}