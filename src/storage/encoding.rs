use crate::error::{TsdbError, TsdbResult};

pub fn encode_usize(x: usize) -> [u8; 4] {
    (x as u32).to_be_bytes()
}

pub fn write_usize(buf: &mut Vec<u8>, x: usize) {
    buf.extend_from_slice(&encode_usize(x));
}

pub fn read_usize<'a>(slice: &'a [u8], context: &str) -> TsdbResult<(&'a [u8], usize)> {
    let to_decode = slice
        .get(0..4)
        .ok_or_else(|| TsdbError::CannotDeserialize(context.to_string()))?;

    let bytes: [u8; std::mem::size_of::<u32>()] = to_decode.try_into()
        .map_err(|_| TsdbError::CannotDeserialize(context.to_string()))?;

    let byte_size = u32::from_be_bytes(bytes); // TODO: remove this unwrap
    Ok((&slice[4..], byte_size as usize))
}

#[cfg(test)]
mod tests {

}
