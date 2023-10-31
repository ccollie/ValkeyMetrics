use crate::error::TsdbResult;
use crate::storage::{Chunk, merge_by_capacity};
use crate::storage::time_series::TimeSeries;

pub fn defrag_series(series: &mut TimeSeries) -> TsdbResult<()> {
    series.trim()?;

    if series.chunks.len() < 2 {
        return Ok(());
    }

    let min_timestamp = series.get_min_timestamp();
    let duplicate_policy = series.duplicate_policy;
    let mut deleted_count = 0;

    let mut chunks_to_remove = Vec::new();
    let mut i = series.chunks.len() - 1;

    let mut iter = series.chunks.iter_mut().rev();
    while let Some(mut chunk) = iter.next() {
        if chunk.is_empty() {
            deleted_count += chunk.num_samples();
            chunks_to_remove.push(i);
            i -= 1;
            continue;
        }
        i -= 1;

        loop {
            // check if previous block has capacity, and if so merge into it
            if let Some(prev_chunk) = iter.next() {
                if let Some(deleted) = merge_by_capacity(
                    prev_chunk,
                    chunk,
                    min_timestamp,
                    duplicate_policy,
                )? {
                    deleted_count -= deleted;
                }
            } else {
                break
            }
            break;
        }
    }

    for chunk in chunks_to_remove {
        series.chunks.remove(chunk);
    }

    series.total_samples -= deleted_count;

    Ok(())
}