mod varint;
mod uvarint;

pub type NomBitInput<'a> = (&'a [u8], usize);

pub use varint::*;
pub use uvarint::*;