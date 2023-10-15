use std::error::Error;
use std::io::Write;
use q_compress::CompressorConfig;
use q_compress::data_types::NumberLike;
use q_compress::errors::QCompressError;
use q_compress::wrapped::{ChunkSpec, Compressor, Decompressor};
use crate::error::{TsdbError, TsdbResult};
use crate::ts::chunks::compressed::common::PageMetadata;
use crate::ts::chunks::compressed::common::serialize::{read_usize, write_usize};

pub(super) const DATA_PAGE_SIZE: usize = 256;

// todo: pass in opts. we can upfront determine if we need to use delta encoding
// and type of value (gauge, counter, etc)
pub(crate) fn compress_series(
    buf: &mut Vec<u8>,
    timestamps: &[i64],
    values: &[f64],
) -> TsdbResult<Vec<PageMetadata>> {
    let mut page_metas = Vec::with_capacity(timestamps.len() / DATA_PAGE_SIZE + 1);

    let mut count = timestamps.len();

    let mut data_page_sizes = Vec::with_capacity(count / DATA_PAGE_SIZE + 1);

    while count > 0 {
        let page_size = count.min(DATA_PAGE_SIZE);
        data_page_sizes.push(page_size);
        count -= page_size;
    }

    let chunk_spec = ChunkSpec::default().with_page_sizes(data_page_sizes.clone());

    let ts_compressor_config = CompressorConfig::default().with_delta_encoding_order(2);

    let mut ts_compressor = Compressor::<i64>::from_config(ts_compressor_config);

    let mut value_compressor = get_value_compressor(values);

    // timestamp metadata
    write_metadata(&mut ts_compressor, buf, timestamps, &chunk_spec)?;

    // write out value chunk metadata
    write_metadata(&mut value_compressor, buf, values, &chunk_spec)?;

    let mut idx = 0;
    for page_size in data_page_sizes.iter() {
        // Each page consists of
        // 1. count
        // 2. timestamp compressed body size
        // 3. timestamp page
        // 4. values compressed body size
        // 5. values page

        // 1.
        write_usize(buf, *page_size);

        // Update metadata
        let t_min = timestamps[idx];
        idx += page_size;
        let t_max = timestamps[idx - 1];

        page_metas.push(PageMetadata {
            offset: buf.len(),
            count: *page_size,
            t_min,
            t_max,
        });

        // 2.
        write_usize(buf, ts_compressor.byte_size());

        // 3.
        buf.extend(ts_compressor.drain_bytes());

        // 4.
        write_usize(buf, value_compressor.byte_size());

        // 5.
        buf.extend(value_compressor.drain_bytes());
    }

    Ok(page_metas)
}

fn get_value_compressor(values: &[f64]) -> Compressor<f64> {
    Compressor::<f64>::from_config(q_compress::auto_compressor_config(
        values,
        q_compress::DEFAULT_COMPRESSION_LEVEL,
    ))
}

fn write_metadata<T: NumberLike>(
    compressor: &mut Compressor<T>,
    dest: &mut Vec<u8>,
    values: &[T],
    chunk_spec: &ChunkSpec,
) -> TsdbResult<()> {
    // timestamp metadata
    compressor.header().map_err(map_compression_err)?;
    compressor
        .chunk_metadata(values, chunk_spec)
        .map_err(map_compression_err)?;
    write_usize(dest, compressor.byte_size());
    dest.extend(compressor.drain_bytes());
    Ok(())
}

pub(in crate::ts) fn read_metadata<'a, T: NumberLike>(
    decompressor: &mut Decompressor<T>,
    compressed: &'a [u8],
) -> TsdbResult<&'a [u8]> {
    let (mut tail, size) = read_usize(compressed, "compressed chunk metadata length")?;
    decompressor.write_all(&tail[..size]).unwrap();
    decompressor.header().map_err(map_compression_err)?;
    decompressor.chunk_metadata().map_err(map_compression_err)?;
    tail = &tail[size..];
    Ok(tail)
}

pub(crate) fn map_compression_err(e: QCompressError) -> TsdbError {
    TsdbError::CannotSerialize(e.to_string())
}

fn map_unmarshal_err(e: impl Error, what: &str) -> TsdbError {
    let msg = format!("error reading {what}: {:?}", e);
    TsdbError::CannotDeserialize(msg)
}

#[cfg(test)]
mod test {
    use std::time::{Duration, Instant, SystemTime};
    use q_compress::errors::QCompressResult;
    use rand::Rng;

    fn main() -> QCompressResult<()> {
        let mut rng = rand::thread_rng();

        let mut series = TimeSeries::default();
        let t0 = SystemTime::now();
        let mut t = t0;
        let mut v = 100.0;
        for _ in 0..100000 {
            t += Duration::from_nanos(1_000_000_000 + rng.gen_range(0..1_000_000));
            v += rng.gen_range(0.0..1.0);
            series.timestamps.push(t);
            series.values.push(v);
        }

        let compressed = compress_time_series(&series)?;
        println!("compressed to {} bytes", compressed.len());

        let filter_t0 = t0 + Duration::from_secs(10000);
        let filter_t1 = t0 + Duration::from_secs(20000);
        let benchmark_instant = Instant::now();
        let decompressed = decompress_time_series_between(&compressed, filter_t0, filter_t1)?;
        let benchmark_dt = Instant::now() - benchmark_instant;
        println!(
            "decompressed {} numbers matching filter in {:?}",
            decompressed.len(),
            benchmark_dt,
        );

        Ok(())
    }
}