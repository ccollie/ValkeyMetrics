// Copyright 2017 Brian Langenberger
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Traits and implementations for writing bits to a stream.
//!

use std::io;
use std::marker::PhantomData;

use core::convert::From;

use bitstream_io::{
    huffman::WriteHuffmanTree,
    BitQueue,
    Endianness,
    Numeric,
    Primitive
};

pub use bitstream_io::BigEndian;
use get_size::GetSize;

/// For writing bit values to an underlying stream in a given endianness.
///
/// Because this only writes whole bytes to the underlying stream,
/// it is important that output is byte-aligned before the bitstream
/// writer's lifetime ends.
/// **Partial bytes will be lost** if the writer is disposed of
/// before they can be written.
#[derive(Debug)]
pub struct BitWriter<W: io::Write, E: Endianness> {
    pub writer: W,
    pub bit_queue: BitQueue<E, u8>,
}

impl<W: io::Write, E: Endianness> BitWriter<W, E> {
    /// Wraps a BitWriter around something that implements `Write`
    pub fn new(writer: W) -> BitWriter<W, E> {
        BitWriter {
            writer,
            bit_queue: BitQueue::new(),
        }
    }

    /// Wraps a BitWriter around something that implements `Write`
    /// with the given endianness.
    pub fn endian(writer: W, _endian: E) -> BitWriter<W, E> {
        BitWriter {
            writer,
            bit_queue: BitQueue::new(),
        }
    }

    /// Unwraps internal writer and disposes of BitWriter.
    ///
    /// # Warning
    ///
    /// Any unwritten partial bits are discarded.
    #[inline]
    pub fn into_writer(self) -> W {
        self.writer
    }

    /// If stream is byte-aligned, provides mutable reference
    /// to internal writer. Otherwise, returns `None`
    #[inline]
    pub fn writer(&mut self) -> Option<&mut W> {
        if self.byte_aligned() {
            Some(&mut self.writer)
        } else {
            None
        }
    }

    /// Converts `BitWriter` to `ByteWriter` in the same endianness.
    ///
    /// # Warning
    ///
    /// Any written partial bits are discarded.
    #[inline]
    pub fn into_bytewriter(self) -> ByteWriter<W, E> {
        ByteWriter::new(self.into_writer())
    }

    /// If stream is byte-aligned, provides temporary `ByteWriter`
    /// in the same endianness. Otherwise, returns `None`
    ///
    /// # Warning
    ///
    /// Any unwritten bits left over when `ByteWriter` is dropped are lost.
    #[inline]
    pub fn bytewriter(&mut self) -> Option<ByteWriter<&mut W, E>> {
        self.writer().map(ByteWriter::new)
    }

    /// Consumes writer and returns any un-written partial byte
    /// as a `(bits, value)` tuple.
    ///
    /// # Examples
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut data = Vec::new();
    /// let (bits, value) = {
    ///     let mut writer = BitWriter::endian(&mut data, BigEndian);
    ///     writer.write(15, 0b1010_0101_0101_101).unwrap();
    ///     writer.into_unwritten()
    /// };
    /// assert_eq!(data, [0b1010_0101]);
    /// assert_eq!(bits, 7);
    /// assert_eq!(value, 0b0101_101);
    /// ```
    ///
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut data = Vec::new();
    /// let (bits, value) = {
    ///     let mut writer = BitWriter::endian(&mut data, BigEndian);
    ///     writer.write(8, 0b1010_0101).unwrap();
    ///     writer.into_unwritten()
    /// };
    /// assert_eq!(data, [0b1010_0101]);
    /// assert_eq!(bits, 0);
    /// assert_eq!(value, 0);
    /// ```
    #[inline(always)]
    pub fn into_unwritten(self) -> (u32, u8) {
        (self.bit_queue.len(), self.bit_queue.value())
    }

    /// Flushes output stream to disk, if necessary.
    /// Any partial bytes are not flushed.
    ///
    /// # Errors
    ///
    /// Passes along any errors from the underlying stream.
    #[inline(always)]
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// A trait for anything that can write a variable number of
/// potentially un-aligned values to an output stream
pub trait BitWrite {
    /// Writes a single bit to the stream.
    /// `true` indicates 1, `false` indicates 0
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    fn write_bit(&mut self, bit: bool) -> io::Result<()>;

    /// Writes an unsigned value to the stream using the given
    /// number of bits.
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    /// Returns an error if the input type is too small
    /// to hold the given number of bits.
    /// Returns an error if the value is too large
    /// to fit the given number of bits.
    fn write<U>(&mut self, bits: u32, value: U) -> io::Result<()>
    where
        U: Numeric;

    /// Writes an unsigned value to the stream using the given
    /// const number of bits.
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    /// Returns an error if the value is too large
    /// to fit the given number of bits.
    /// A compile-time error occurs if the given number of bits
    /// is larger than the output type.
    fn write_out<const BITS: u32, U>(&mut self, value: U) -> io::Result<()>
    where
        U: Numeric,
    {
        self.write(BITS, value)
    }

    /// Writes the entirety of a byte buffer to the stream.
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    ///
    /// # Example
    ///
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), BigEndian);
    /// writer.write(8, 0x66).unwrap();
    /// writer.write(8, 0x6F).unwrap();
    /// writer.write(8, 0x6F).unwrap();
    /// writer.write_bytes(b"bar").unwrap();
    /// assert_eq!(writer.into_writer(), b"foobar");
    /// ```
    #[inline]
    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<()> {
        buf.iter().try_for_each(|b| self.write_out::<8, _>(*b))
    }

    /// Writes `value` number of 1 bits to the stream
    /// and then writes a 0 bit.  This field is variably-sized.
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    ///
    /// # Examples
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), BigEndian);
    /// writer.write_unary0(0).unwrap();
    /// writer.write_unary0(3).unwrap();
    /// writer.write_unary0(10).unwrap();
    /// assert_eq!(writer.into_writer(), [0b01110111, 0b11111110]);
    /// ```
    ///
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{LittleEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), LittleEndian);
    /// writer.write_unary0(0).unwrap();
    /// writer.write_unary0(3).unwrap();
    /// writer.write_unary0(10).unwrap();
    /// assert_eq!(writer.into_writer(), [0b11101110, 0b01111111]);
    /// ```
    fn write_unary0(&mut self, value: u32) -> io::Result<()> {
        match value {
            0 => self.write_bit(false),
            bits @ 1..=31 => self
                .write(value, (1u32 << bits) - 1)
                .and_then(|()| self.write_bit(false)),
            32 => self
                .write(value, 0xFFFF_FFFFu32)
                .and_then(|()| self.write_bit(false)),
            bits @ 33..=63 => self
                .write(value, (1u64 << bits) - 1)
                .and_then(|()| self.write_bit(false)),
            64 => self
                .write(value, 0xFFFF_FFFF_FFFF_FFFFu64)
                .and_then(|()| self.write_bit(false)),
            mut bits => {
                while bits > 64 {
                    self.write(64, 0xFFFF_FFFF_FFFF_FFFFu64)?;
                    bits -= 64;
                }
                self.write_unary0(bits)
            }
        }
    }

    /// Writes `value` number of 0 bits to the stream
    /// and then writes a 1 bit.  This field is variably-sized.
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    ///
    /// # Example
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), BigEndian);
    /// writer.write_unary1(0).unwrap();
    /// writer.write_unary1(3).unwrap();
    /// writer.write_unary1(10).unwrap();
    /// assert_eq!(writer.into_writer(), [0b10001000, 0b00000001]);
    /// ```
    ///
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{LittleEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), LittleEndian);
    /// writer.write_unary1(0).unwrap();
    /// writer.write_unary1(3).unwrap();
    /// writer.write_unary1(10).unwrap();
    /// assert_eq!(writer.into_writer(), [0b00010001, 0b10000000]);
    /// ```
    fn write_unary1(&mut self, value: u32) -> io::Result<()> {
        match value {
            0 => self.write_bit(true),
            1..=32 => self.write(value, 0u32).and_then(|()| self.write_bit(true)),
            33..=64 => self.write(value, 0u64).and_then(|()| self.write_bit(true)),
            mut bits => {
                while bits > 64 {
                    self.write(64, 0u64)?;
                    bits -= 64;
                }
                self.write_unary1(bits)
            }
        }
    }

    /// Returns true if the stream is aligned at a whole byte.
    fn byte_aligned(&self) -> bool;

    /// Pads the stream with 0 bits until it is aligned at a whole byte.
    /// Does nothing if the stream is already aligned.
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    ///
    /// # Example
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), BigEndian);
    /// writer.write(1, 0).unwrap();
    /// writer.byte_align().unwrap();
    /// writer.write(8, 0xFF).unwrap();
    /// assert_eq!(writer.into_writer(), [0x00, 0xFF]);
    /// ```
    fn byte_align(&mut self) -> io::Result<()> {
        while !self.byte_aligned() {
            self.write_bit(false)?;
        }
        Ok(())
    }
}

/// A trait for anything that can write Huffman codes
/// of a given endianness to an output stream
pub trait HuffmanWrite<E: Endianness> {
    /// Writes Huffman code for the given symbol to the stream.
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    fn write_huffman<T>(&mut self, tree: &WriteHuffmanTree<E, T>, symbol: T) -> io::Result<()>
    where
        T: Ord + Copy;
}

impl<W: io::Write, E: Endianness> BitWrite for BitWriter<W, E> {
    /// # Examples
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), BigEndian);
    /// writer.write_bit(true).unwrap();
    /// writer.write_bit(false).unwrap();
    /// writer.write_bit(true).unwrap();
    /// writer.write_bit(true).unwrap();
    /// writer.write_bit(false).unwrap();
    /// writer.write_bit(true).unwrap();
    /// writer.write_bit(true).unwrap();
    /// writer.write_bit(true).unwrap();
    /// assert_eq!(writer.into_writer(), [0b10110111]);
    /// ```
    ///
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{LittleEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), LittleEndian);
    /// writer.write_bit(true).unwrap();
    /// writer.write_bit(true).unwrap();
    /// writer.write_bit(true).unwrap();
    /// writer.write_bit(false).unwrap();
    /// writer.write_bit(true).unwrap();
    /// writer.write_bit(true).unwrap();
    /// writer.write_bit(false).unwrap();
    /// writer.write_bit(true).unwrap();
    /// assert_eq!(writer.into_writer(), [0b10110111]);
    /// ```
    fn write_bit(&mut self, bit: bool) -> io::Result<()> {
        self.bit_queue.push(1, u8::from(bit));
        if self.bit_queue.is_full() {
            write_byte(&mut self.writer, self.bit_queue.pop(8))
        } else {
            Ok(())
        }
    }

    /// # Examples
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), BigEndian);
    /// writer.write(1, 0b1).unwrap();
    /// writer.write(2, 0b01).unwrap();
    /// writer.write(5, 0b10111).unwrap();
    /// assert_eq!(writer.into_writer(), [0b10110111]);
    /// ```
    ///
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{LittleEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), LittleEndian);
    /// writer.write(1, 0b1).unwrap();
    /// writer.write(2, 0b11).unwrap();
    /// writer.write(5, 0b10110).unwrap();
    /// assert_eq!(writer.into_writer(), [0b10110111]);
    /// ```
    ///
    /// ```
    /// use std::io::{Write, sink};
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut w = BitWriter::endian(sink(), BigEndian);
    /// assert!(w.write(9, 0u8).is_err());    // can't write  u8 in 9 bits
    /// assert!(w.write(17, 0u16).is_err());  // can't write u16 in 17 bits
    /// assert!(w.write(33, 0u32).is_err());  // can't write u32 in 33 bits
    /// assert!(w.write(65, 0u64).is_err());  // can't write u64 in 65 bits
    /// assert!(w.write(1, 2).is_err());      // can't write   2 in 1 bit
    /// assert!(w.write(2, 4).is_err());      // can't write   4 in 2 bits
    /// assert!(w.write(3, 8).is_err());      // can't write   8 in 3 bits
    /// assert!(w.write(4, 16).is_err());     // can't write  16 in 4 bits
    /// ```
    fn write<U>(&mut self, bits: u32, value: U) -> io::Result<()>
    where
        U: Numeric,
    {
        if bits > U::BITS_SIZE {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "excessive bits for type written",
            ))
        } else if (bits < U::BITS_SIZE) && (value >= (U::ONE << bits)) {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "excessive value for bits written",
            ))
        } else if bits < self.bit_queue.remaining_len() {
            self.bit_queue.push(bits, value.to_u8());
            Ok(())
        } else {
            let mut acc = BitQueue::from_value(value, bits);
            write_unaligned(&mut self.writer, &mut acc, &mut self.bit_queue)?;
            write_aligned(&mut self.writer, &mut acc)?;
            self.bit_queue.push(acc.len(), acc.value().to_u8());
            Ok(())
        }
    }

    /// # Examples
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), BigEndian);
    /// writer.write_out::<1, _>(0b1).unwrap();
    /// writer.write_out::<2, _>(0b01).unwrap();
    /// writer.write_out::<5, _>(0b10111).unwrap();
    /// assert_eq!(writer.into_writer(), [0b10110111]);
    /// ```
    ///
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{LittleEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(Vec::new(), LittleEndian);
    /// writer.write_out::<1, _>(0b1).unwrap();
    /// writer.write_out::<2, _>(0b11).unwrap();
    /// writer.write_out::<5, _>(0b10110).unwrap();
    /// assert_eq!(writer.into_writer(), [0b10110111]);
    /// ```
    ///
    /// ```
    /// use std::io::{Write, sink};
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut w = BitWriter::endian(sink(), BigEndian);
    /// assert!(w.write_out::<1, _>(2).is_err());      // can't write   2 in 1 bit
    /// assert!(w.write_out::<2, _>(4).is_err());      // can't write   4 in 2 bits
    /// assert!(w.write_out::<3, _>(8).is_err());      // can't write   8 in 3 bits
    /// assert!(w.write_out::<4, _>(16).is_err());     // can't write  16 in 4 bits
    /// ```
    fn write_out<const BITS: u32, U>(&mut self, value: U) -> io::Result<()>
    where
        U: Numeric,
    {
        const {
            assert!(BITS <= U::BITS_SIZE, "excessive bits for type written");
        }

        if (BITS < U::BITS_SIZE) && (value >= (U::ONE << BITS)) {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "excessive value for bits written",
            ))
        } else if BITS < self.bit_queue.remaining_len() {
            self.bit_queue.push_fixed::<BITS>(value.to_u8());
            Ok(())
        } else {
            let mut acc = BitQueue::from_value(value, BITS);
            write_unaligned(&mut self.writer, &mut acc, &mut self.bit_queue)?;
            write_aligned(&mut self.writer, &mut acc)?;
            self.bit_queue.push(acc.len(), acc.value().to_u8());
            Ok(())
        }
    }

    #[inline]
    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<()> {
        if self.byte_aligned() {
            self.writer.write_all(buf)
        } else {
            buf.iter().try_for_each(|b| self.write_out::<8, _>(*b))
        }
    }

    /// # Example
    /// ```
    /// use std::io::{Write, sink};
    /// use bitstream_io::{BigEndian, BitWriter, BitWrite};
    /// let mut writer = BitWriter::endian(sink(), BigEndian);
    /// assert_eq!(writer.byte_aligned(), true);
    /// writer.write(1, 0).unwrap();
    /// assert_eq!(writer.byte_aligned(), false);
    /// writer.write(7, 0).unwrap();
    /// assert_eq!(writer.byte_aligned(), true);
    /// ```
    #[inline(always)]
    fn byte_aligned(&self) -> bool {
        self.bit_queue.is_empty()
    }
}

impl<W: io::Write, E: Endianness> HuffmanWrite<E> for BitWriter<W, E> {
    /// # Example
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, BitWriter, HuffmanWrite};
    /// use bitstream_io::huffman::compile_write_tree;
    /// let tree = compile_write_tree(
    ///     vec![('a', vec![0]),
    ///          ('b', vec![1, 0]),
    ///          ('c', vec![1, 1, 0]),
    ///          ('d', vec![1, 1, 1])]).unwrap();
    /// let mut writer = BitWriter::endian(Vec::new(), BigEndian);
    /// writer.write_huffman(&tree, 'b').unwrap();
    /// writer.write_huffman(&tree, 'c').unwrap();
    /// writer.write_huffman(&tree, 'd').unwrap();
    /// assert_eq!(writer.into_writer(), [0b10110111]);
    /// ```
    #[inline]
    fn write_huffman<T>(&mut self, tree: &WriteHuffmanTree<E, T>, symbol: T) -> io::Result<()>
    where
        T: Ord + Copy,
    {
        tree.get(&symbol)
            .try_for_each(|(bits, value)| self.write(*bits, *value))
    }
}

#[inline]
fn write_byte<W>(mut writer: W, byte: u8) -> io::Result<()>
where
    W: io::Write,
{
    writer.write_all(core::slice::from_ref(&byte))
}

fn write_unaligned<W, E, N>(
    writer: W,
    acc: &mut BitQueue<E, N>,
    rem: &mut BitQueue<E, u8>,
) -> io::Result<()>
where
    W: io::Write,
    E: Endianness,
    N: Numeric,
{
    if rem.is_empty() {
        Ok(())
    } else {
        use core::cmp::min;
        let bits_to_transfer = min(8 - rem.len(), acc.len());
        rem.push(bits_to_transfer, acc.pop(bits_to_transfer).to_u8());
        if rem.len() == 8 {
            write_byte(writer, rem.pop(8))
        } else {
            Ok(())
        }
    }
}

fn write_aligned<W, E, N>(mut writer: W, acc: &mut BitQueue<E, N>) -> io::Result<()>
where
    W: io::Write,
    E: Endianness,
    N: Numeric,
{
    let to_write = (acc.len() / 8) as usize;
    if to_write > 0 {
        let mut buf = N::buffer();
        let buf_ref: &mut [u8] = buf.as_mut();
        for b in buf_ref[0..to_write].iter_mut() {
            *b = acc.pop_fixed::<8>().to_u8();
        }
        writer.write_all(&buf_ref[0..to_write])
    } else {
        Ok(())
    }
}

/// For writing aligned bytes to a stream of bytes in a given endianness.
///
/// This only writes aligned values and maintains no internal state.
pub struct ByteWriter<W: io::Write, E: Endianness> {
    phantom: PhantomData<E>,
    writer: W,
}

impl<W: io::Write, E: Endianness> ByteWriter<W, E> {
    /// Wraps a ByteWriter around something that implements `Write`
    pub fn new(writer: W) -> ByteWriter<W, E> {
        ByteWriter {
            phantom: PhantomData,
            writer,
        }
    }

    /// Wraps a BitWriter around something that implements `Write`
    /// with the given endianness.
    pub fn endian(writer: W, _endian: E) -> ByteWriter<W, E> {
        ByteWriter {
            phantom: PhantomData,
            writer,
        }
    }

    /// Unwraps internal writer and disposes of `ByteWriter`.
    /// Any unwritten partial bits are discarded.
    #[inline]
    pub fn into_writer(self) -> W {
        self.writer
    }

    /// Provides mutable reference to internal writer.
    #[inline]
    pub fn writer(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Converts `ByteWriter` to `BitWriter` in the same endianness.
    #[inline]
    pub fn into_bitwriter(self) -> BitWriter<W, E> {
        BitWriter::new(self.into_writer())
    }

    /// Provides temporary `BitWriter` in the same endianness.
    ///
    /// # Warning
    ///
    /// Any unwritten bits left over when `BitWriter` is dropped are lost.
    #[inline]
    pub fn bitwriter(&mut self) -> BitWriter<&mut W, E> {
        BitWriter::new(self.writer())
    }
}

/// A trait for anything that can write aligned values to an output stream
pub trait ByteWrite {
    /// Writes whole numeric value to stream
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    /// # Examples
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, ByteWriter, ByteWrite};
    /// let mut writer = ByteWriter::endian(Vec::new(), BigEndian);
    /// writer.write(0b0000000011111111u16).unwrap();
    /// assert_eq!(writer.into_writer(), [0b00000000, 0b11111111]);
    /// ```
    ///
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{LittleEndian, ByteWriter, ByteWrite};
    /// let mut writer = ByteWriter::endian(Vec::new(), LittleEndian);
    /// writer.write(0b0000000011111111u16).unwrap();
    /// assert_eq!(writer.into_writer(), [0b11111111, 0b00000000]);
    /// ```
    fn write<V>(&mut self, value: V) -> io::Result<()>
    where
        V: Primitive;

    /// Writes whole numeric value to stream in a potentially different endianness
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    ///
    /// # Examples
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, ByteWriter, ByteWrite, LittleEndian};
    /// let mut writer = ByteWriter::endian(Vec::new(), BigEndian);
    /// writer.write_as::<LittleEndian, u16>(0b0000000011111111).unwrap();
    /// assert_eq!(writer.into_writer(), [0b11111111, 0b00000000]);
    /// ```
    ///
    /// ```
    /// use std::io::Write;
    /// use bitstream_io::{BigEndian, ByteWriter, ByteWrite, LittleEndian};
    /// let mut writer = ByteWriter::endian(Vec::new(), LittleEndian);
    /// writer.write_as::<BigEndian, u16>(0b0000000011111111).unwrap();
    /// assert_eq!(writer.into_writer(), [0b00000000, 0b11111111]);
    /// ```
    fn write_as<F, V>(&mut self, value: V) -> io::Result<()>
    where
        F: Endianness,
        V: Primitive;

    /// Writes the entirety of a byte buffer to the stream.
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<()>;

    /// Returns mutable reference to underlying writer
    fn writer_ref(&mut self) -> &mut dyn io::Write;
}

impl<W: io::Write, E: Endianness> ByteWrite for ByteWriter<W, E> {
    #[inline]
    fn write<V>(&mut self, value: V) -> io::Result<()>
    where
        V: Primitive,
    {
        E::write_numeric(&mut self.writer, value)
    }

    #[inline]
    fn write_as<F, V>(&mut self, value: V) -> io::Result<()>
    where
        F: Endianness,
        V: Primitive,
    {
        F::write_numeric(&mut self.writer, value)
    }

    #[inline]
    fn write_bytes(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer.write_all(buf)
    }

    #[inline]
    fn writer_ref(&mut self) -> &mut dyn io::Write {
        &mut self.writer
    }
}

