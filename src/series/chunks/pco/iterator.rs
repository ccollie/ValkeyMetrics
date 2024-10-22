use crate::common::types::Sample;
use crate::error_consts;
use metricsql_runtime::types::Timestamp;
use pco::data_types::NumberLike;
use pco::errors::PcoError;
use pco::standalone::{FileDecompressor, MaybeChunkDecompressor};
use pco::FULL_BATCH_N;
use valkey_module::{ValkeyError, ValkeyResult};

const EMPTY_SLICE: [u8; 0] = [];
struct StreamState<'a, T: NumberLike> {
    decompressor: FileDecompressor,
    chunk_decompressor: MaybeChunkDecompressor<T, &'a [u8]>,
    cursor: &'a [u8],
    count: usize,
    is_finished: bool,
    finished_chunk: bool,
}

impl<'a, T: NumberLike> StreamState<'a, T> {
    fn new(src: &'a [u8]) -> ValkeyResult<Self> {
        let (decompressor, cursor) = FileDecompressor::new(src)
            .map_err(|_| ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION))?;

        let state = Self {
            decompressor,
            chunk_decompressor: MaybeChunkDecompressor::EndOfData(&EMPTY_SLICE),
            cursor,
            count: 0,
            is_finished: false,
            finished_chunk: false,
        };

        Ok(state)
    }

    // https://docs.rs/pco/0.3.1/src/pco/standalone/decompressor.rs.html

    fn next_chunk(&mut self, data: &mut [T; FULL_BATCH_N]) -> ValkeyResult<bool> {
        if self.is_finished {
            return Ok(false);
        }
        self.count = 0;
        if let MaybeChunkDecompressor::Some(chunk_decompressor) = &mut self.chunk_decompressor {
            let progress = chunk_decompressor.decompress(data).map_err(convert_error)?;
            self.finished_chunk = progress.finished;
            self.count = progress.n_processed;
            if self.finished_chunk {
                let mut replacement = MaybeChunkDecompressor::<T, &[u8]>::EndOfData(&EMPTY_SLICE);
                std::mem::swap(&mut self.chunk_decompressor, &mut replacement);
                match replacement {
                    MaybeChunkDecompressor::Some(replacement) => {
                        self.cursor = replacement.into_src();
                    }
                    MaybeChunkDecompressor::EndOfData(_) => {
                        self.is_finished = true;
                    }
                }
            }
            return Ok(true);
        }
        if self.next_chunk_decompressor()? {
            self.next_chunk(data)
        } else {
            Ok(false)
        }
    }

    fn next_chunk_decompressor(&mut self) -> ValkeyResult<bool> {
        match self.decompressor.chunk_decompressor::<T, _>(self.cursor) {
            Ok(MaybeChunkDecompressor::EndOfData(_)) => {
                self.is_finished = true;
                Ok(false)
            }
            Ok(decompressor) => {
                self.chunk_decompressor = decompressor;
                self.finished_chunk = false;
                Ok(true)
            }
            Err(err) => Err(convert_error(err)),
        }
    }
}

pub struct PcoSampleIterator<'a> {
    timestamp_state: StreamState<'a, Timestamp>,
    values_state: StreamState<'a, f64>,
    timestamps: [Timestamp; FULL_BATCH_N],
    values: [f64; FULL_BATCH_N],
    count: usize,
    idx: usize,
    chunks_finished: bool,
    first_ts: Timestamp,
    last_ts: Timestamp,
    filtered: bool,
}

impl<'a> PcoSampleIterator<'a> {
    pub fn new(timestamps: &'a [u8], values: &'a [u8]) -> ValkeyResult<Self> {
        let timestamp_state = StreamState::new(timestamps)?;
        let values_state = StreamState::new(values)?;

        Ok(Self {
            timestamp_state,
            values_state,
            timestamps: [0; FULL_BATCH_N],
            values: [f64::NAN; FULL_BATCH_N],
            count: 0,
            idx: 0,
            chunks_finished: false,
            first_ts: 0,
            last_ts: 0,
            filtered: false,
        })
    }

    pub fn new_range(timestamps: &'a [u8], values: &'a [u8], start_ts: Timestamp, end_ts: Timestamp) -> ValkeyResult<Self> {
        let mut iter = Self::new(timestamps, values)?;
        iter.filtered = true;
        iter.first_ts = start_ts;
        iter.last_ts = end_ts;
        Ok(iter)
    }

    fn next_chunk(&mut self) -> bool {
        self.idx = 0;
        if self.chunks_finished {
            return false;
        }
        self.chunks_finished = match (self.timestamp_state.next_chunk(&mut self.timestamps), self.values_state.next_chunk(&mut self.values)) {
            (Ok(true), Ok(true)) => self.timestamp_state.is_finished || self.values_state.is_finished,
            _ => true,
        };
        // these counts should be the same
        self.count = self.timestamp_state.count.min(self.values_state.count);
        true
    }
}

impl<'a> Iterator for PcoSampleIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.idx < self.count {
                // todo: use get_unchecked to avoid bounds check
                let ts = self.timestamps[self.idx];
                let val = self.values[self.idx];
                self.idx += 1;
                if self.filtered {
                    if ts < self.first_ts {
                        continue;
                    }
                    if ts > self.last_ts {
                        return None;
                    }
                }
                return Some(Sample {
                    timestamp: ts,
                    value: val,
                });
            }
            if !self.next_chunk() {
                return None;
            }
        }
    }
}

fn convert_error(_err: PcoError) -> ValkeyError {
    // todo: log error
    ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION)
}

#[cfg(test)]
mod tests {
    use crate::common::rounding::round_to_decimal_digits;
    use crate::common::types::{Sample, Timestamp};
    use crate::series::chunks::pco::pco_utils::{compress_timestamps, compress_values};
    use crate::series::chunks::pco::PcoSampleIterator;
    use crate::tests::generators::mackey_glass;

    #[test]
    fn test_pco_sample_iterator() {
        const SAMPLE_COUNT: usize = 1000;

        let epoch = 10000;
        let values = mackey_glass(SAMPLE_COUNT, Some(17), Some(42))
            .into_iter()
            .map(|x| round_to_decimal_digits(x, 6))
            .collect::<Vec<_>>();

        let timestamps = (0..SAMPLE_COUNT).map(|x| (epoch + x * 5000) as Timestamp)
            .collect::<Vec<_>>();

        let mut timestamp_buf: Vec<u8> = Vec::with_capacity(1024);
        let mut values_buf: Vec<u8> = Vec::with_capacity(1024);

        compress_timestamps(&mut timestamp_buf, &timestamps).expect("Unable to compress timestamps");
        compress_values(&mut values_buf, &values).expect("Unable to compress values");

        let expected: Vec<_> = timestamps.iter().zip(values.iter())
            .map(|(ts, val)| Sample {
                timestamp: *ts,
                value: *val,
            }).collect();

        let iterator = PcoSampleIterator::new(&timestamp_buf, &values_buf).expect("Unable to create iterator");
        let actual: Vec<Sample> = iterator.collect();

        assert_eq!(actual.len(), expected.len());
        for (i, sample) in actual.iter().enumerate() {
            assert_eq!(sample, &expected[i]);
        }
    }
}