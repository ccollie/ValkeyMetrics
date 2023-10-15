use std::io::Write;
use q_compress::wrapped::Decompressor;
use crate::error::TsdbResult;
use super::compress::{map_compression_err, read_metadata};
use crate::ts::chunks::compressed::common::serialize::read_usize;

pub struct DecompressorContext<'a> {
    data: &'a [u8],
    t_decompressor: Decompressor<i64>,
    v_decompressor: Decompressor<f64>,
}

impl<'a> DecompressorContext<'a> {
    pub(crate) fn new(data: &'a [u8]) -> TsdbResult<Self> {
        let mut t_decompressor = Decompressor::<i64>::default();

        // read timestamp metadata
        let compressed = read_metadata(&mut t_decompressor, data)?;
        let mut v_decompressor = Decompressor::<f64>::default();

        let _ = read_metadata(&mut v_decompressor, compressed)?;

        Ok(Self {
            data,
            t_decompressor,
            v_decompressor,
        })
    }

    pub(crate) fn get_page_data(
        &mut self,
        offset: usize,
        timestamps: &mut Vec<i64>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<&[u8]> {
        let buf = &self.data[offset..];
        // we need to filter and append this data
        let (mut compressed, mut size) = read_usize(buf, "timestamp data size")?;

        let n = size;
        self.t_decompressor.write_all(&compressed[..size]).unwrap();
        let page_t = self.t_decompressor.data_page(n, size)
            .map_err(map_compression_err)?;

        compressed = &compressed[size..];

        (compressed, size) = read_usize(compressed, "value data size")?;
        self.v_decompressor.write_all(&compressed[..size]).unwrap();
        let page_v = self.v_decompressor.data_page(n, size)
            .map_err(map_compression_err)?;

        timestamps.extend(page_t);
        values.extend(page_v);
        Ok(compressed)
    }
}
