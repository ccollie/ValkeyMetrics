mod uvarint;
mod varint;

pub type NomBitInput<'a> = (&'a [u8], usize);

pub use uvarint::*;
pub use varint::*;
