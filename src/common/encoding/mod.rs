mod varbit_xor;
mod varbit_ts;
mod varbit;
mod varint;
mod uvarint;

pub type NomBitInput<'a> = (&'a [u8], usize);

pub use varbit::*;
pub use varbit_ts::*;
pub use varbit_xor::*;
pub use varint::*;
pub use uvarint::*;