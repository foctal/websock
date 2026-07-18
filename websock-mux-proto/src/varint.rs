//! QUIC variable-length integer encoding and decoding.

// Based on Quinn: https://github.com/quinn-rs/quinn/tree/main/quinn-proto/src
// Licensed under Apache-2.0 OR MIT

use std::{convert::TryInto, fmt};

#[cfg(not(target_arch = "wasm32"))]
use std::io::Cursor;

use bytes::{Buf, BufMut};
use thiserror::Error;

#[cfg(not(target_arch = "wasm32"))]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// An integer less than 2^62.
///
/// Values of this type are suitable for encoding as QUIC variable-length integer.
// Rust does not currently model that the top two bits are reserved for the length tag.
#[derive(Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VarInt(pub(crate) u64);

impl VarInt {
    const MAX_1BYTE: u64 = (1 << 6) - 1;
    const MAX_2BYTE: u64 = (1 << 14) - 1;
    const MAX_4BYTE: u64 = (1 << 30) - 1;

    /// The largest representable value.
    pub const MAX: Self = Self((1 << 62) - 1);
    /// The largest encoded value length.
    pub const MAX_SIZE: usize = 8;

    /// Construct a `VarInt` infallibly.
    pub const fn from_u32(x: u32) -> Self {
        Self(x as u64)
    }

    /// Succeeds if `x` < 2^62.
    pub fn from_u64(x: u64) -> Result<Self, VarIntBoundsExceeded> {
        if x <= Self::MAX.0 {
            Ok(Self(x))
        } else {
            Err(VarIntBoundsExceeded)
        }
    }

    /// Create a `VarInt` without checking the bounds.
    ///
    /// # Safety
    ///
    /// `x` must be less than 2^62.
    pub const unsafe fn from_u64_unchecked(x: u64) -> Self {
        Self(x)
    }

    /// Extract the integer value.
    pub const fn into_inner(self) -> u64 {
        self.0
    }

    /// Compute the number of bytes needed to encode this value.
    pub fn size(self) -> usize {
        let x = self.0;
        if x <= Self::MAX_1BYTE {
            1
        } else if x <= Self::MAX_2BYTE {
            2
        } else if x <= Self::MAX_4BYTE {
            4
        } else if x <= Self::MAX.0 {
            8
        } else {
            unreachable!("malformed VarInt");
        }
    }
}

impl From<VarInt> for u64 {
    fn from(x: VarInt) -> Self {
        x.0
    }
}

impl From<u8> for VarInt {
    fn from(x: u8) -> Self {
        Self(x.into())
    }
}

impl From<u16> for VarInt {
    fn from(x: u16) -> Self {
        Self(x.into())
    }
}

impl From<u32> for VarInt {
    fn from(x: u32) -> Self {
        Self(x.into())
    }
}

impl std::convert::TryFrom<u64> for VarInt {
    type Error = VarIntBoundsExceeded;
    /// Succeeds if `x` < 2^62.
    fn try_from(x: u64) -> Result<Self, VarIntBoundsExceeded> {
        Self::from_u64(x)
    }
}

impl std::convert::TryFrom<u128> for VarInt {
    type Error = VarIntBoundsExceeded;
    /// Succeeds if `x` < 2^62.
    fn try_from(x: u128) -> Result<Self, VarIntBoundsExceeded> {
        Self::from_u64(x.try_into().map_err(|_| VarIntBoundsExceeded)?)
    }
}

impl std::convert::TryFrom<usize> for VarInt {
    type Error = VarIntBoundsExceeded;
    /// Succeeds if `x` < 2^62.
    fn try_from(x: usize) -> Result<Self, VarIntBoundsExceeded> {
        Self::try_from(x as u64)
    }
}

impl fmt::Debug for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl VarInt {
    /// Decode one QUIC variable-length integer from a byte buffer.
    pub fn decode<B: Buf>(r: &mut B) -> Result<Self, VarIntUnexpectedEnd> {
        if !r.has_remaining() {
            return Err(VarIntUnexpectedEnd);
        }
        let first = r.get_u8();
        let tag = first >> 6;
        let body = (first & 0b0011_1111) as u64;
        let x = match tag {
            0b00 => body,
            0b01 => {
                if r.remaining() < 1 {
                    return Err(VarIntUnexpectedEnd);
                }
                (body << 8) | u64::from(r.get_u8())
            }
            0b10 => {
                if r.remaining() < 3 {
                    return Err(VarIntUnexpectedEnd);
                }
                (body << 24)
                    | (u64::from(r.get_u8()) << 16)
                    | (u64::from(r.get_u8()) << 8)
                    | u64::from(r.get_u8())
            }
            0b11 => {
                if r.remaining() < 7 {
                    return Err(VarIntUnexpectedEnd);
                }
                (body << 56)
                    | (u64::from(r.get_u8()) << 48)
                    | (u64::from(r.get_u8()) << 40)
                    | (u64::from(r.get_u8()) << 32)
                    | (u64::from(r.get_u8()) << 24)
                    | (u64::from(r.get_u8()) << 16)
                    | (u64::from(r.get_u8()) << 8)
                    | u64::from(r.get_u8())
            }
            _ => unreachable!(),
        };
        Ok(Self(x))
    }

    /// Read one QUIC variable-length integer from an asynchronous stream.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Self, VarIntUnexpectedEnd> {
        // Eight bytes is the maximum encoded length.
        let mut buf = [0; 8];

        // Read the first byte because it encodes the length tag.
        stream
            .read_exact(&mut buf[0..1])
            .await
            .map_err(|_| VarIntUnexpectedEnd)?;

        // 0b00 = 1 byte, 0b01 = 2 bytes, 0b10 = 4 bytes, 0b11 = 8 bytes.
        let size = 1 << (buf[0] >> 6);
        stream
            .read_exact(&mut buf[1..size])
            .await
            .map_err(|_| VarIntUnexpectedEnd)?;

        // Use a cursor to decode from the stack buffer.
        let mut cursor = Cursor::new(&buf[..size]);
        let v = VarInt::decode(&mut cursor).unwrap();

        Ok(v)
    }

    /// Encode this value into a byte buffer.
    pub fn encode<B: BufMut>(&self, w: &mut B) {
        let x = self.0;
        if x <= Self::MAX_1BYTE {
            w.put_u8(x as u8);
        } else if x <= Self::MAX_2BYTE {
            w.put_u16((0b01 << 14) | x as u16);
        } else if x <= Self::MAX_4BYTE {
            w.put_u32((0b10 << 30) | x as u32);
        } else if x <= Self::MAX.0 {
            w.put_u64((0b11 << 62) | x);
        } else {
            unreachable!("malformed VarInt")
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Write this value to an asynchronous stream.
    pub async fn write<S: AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
    ) -> Result<(), VarIntUnexpectedEnd> {
        // Keep the temporary buffer on the stack to avoid allocation.
        let mut buf = [0u8; 8];
        let mut cursor: &mut [u8] = &mut buf;
        self.encode(&mut cursor);
        let size = 8 - cursor.len();

        let mut cursor = &buf[..size];
        stream
            .write_all_buf(&mut cursor)
            .await
            .map_err(|_| VarIntUnexpectedEnd)?;

        Ok(())
    }
}

/// Error returned when constructing a `VarInt` from a value >= 2^62.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
#[error("value too large for varint encoding")]
pub struct VarIntBoundsExceeded;

/// Error returned when a buffer or stream ends before a varint is complete.
#[derive(Error, Debug, Copy, Clone, Eq, PartialEq)]
#[error("unexpected end of buffer")]
pub struct VarIntUnexpectedEnd;
