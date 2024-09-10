
use integer_encoding::VarInt;
use crate::error::{TsdbError, TsdbResult};

/// appends marshaled v to dst and returns the result.
pub fn write_var_int<T: VarInt>(dst: &mut Vec<u8>, v: T) {
    let len = dst.len();
    dst.resize(len + v.required_space(), 0);
    let _ = v.encode_var(&mut dst[len..]);
}

/// returns unmarshalled int from src.
pub fn read_var_int<T: VarInt>(src: &[u8]) -> TsdbResult<(T, &[u8])> {
    match T::decode_var(src) {
        Some((v, ofs)) => Ok((v, &src[ofs..])),
        _ => Err(TsdbError::CannotDeserialize(
            "Error decoding var int".to_string(),
        )),
    }
}
