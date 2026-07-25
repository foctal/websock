use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::varint::{VarInt, VarIntBoundsExceeded, VarIntUnexpectedEnd};

/// Directionality of a multiplexed stream.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StreamDir {
    /// Bidirectional stream.
    Bi,
    /// Unidirectional stream.
    Uni,
}

/// Encoded stream identifier containing counter, initiator, and direction bits.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct StreamId(pub u64);

impl StreamId {
    /// Construct a stream identifier from its component fields.
    pub fn new(
        counter: u64,
        is_server: bool,
        dir: StreamDir,
    ) -> Result<Self, VarIntBoundsExceeded> {
        let initiator = if is_server { 1 } else { 0 };
        let dir_bit = match dir {
            StreamDir::Bi => 0,
            StreamDir::Uni => 1,
        };
        let value = counter.checked_mul(4).ok_or(VarIntBoundsExceeded)?
            | ((dir_bit as u64) << 1)
            | (initiator as u64);
        VarInt::from_u64(value)?;
        Ok(Self(value))
    }

    /// Return the stream direction encoded in the identifier.
    pub fn dir(self) -> StreamDir {
        if (self.0 >> 1) & 1 == 1 {
            StreamDir::Uni
        } else {
            StreamDir::Bi
        }
    }

    /// Return whether the server initiated the stream.
    pub fn initiator_is_server(self) -> bool {
        self.0 & 1 == 1
    }

    /// Return the stream sequence number encoded in this ID.
    pub fn counter(self) -> u64 {
        self.0 >> 2
    }
}

/// A single frame in the version 1 mux wire protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Open a peer-receive-only stream.
    OpenUni {
        /// Identifier of the new stream.
        id: StreamId,
    },
    /// Open a bidirectional stream.
    OpenBi {
        /// Identifier of the new stream.
        id: StreamId,
    },
    /// Carry stream payload data and optional end-of-stream state.
    Stream {
        /// Target stream identifier.
        id: StreamId,
        /// Payload bytes.
        data: Bytes,
        /// Whether this frame finishes the sender direction.
        fin: bool,
    },
    /// Abruptly terminate the sender direction.
    ResetStream {
        /// Target stream identifier.
        id: StreamId,
        /// Application-defined reset code.
        code: u64,
    },
    /// Ask the peer to stop its sender direction.
    StopSending {
        /// Target stream identifier.
        id: StreamId,
        /// Application-defined stop code.
        code: u64,
    },
    /// Increase the cumulative stream-data allowance.
    MaxStreamData {
        /// Target stream identifier.
        id: StreamId,
        /// New cumulative byte limit.
        max: u64,
    },
    /// Close the entire mux connection.
    ConnectionClose {
        /// Connection-level error code.
        code: u64,
        /// Human-readable UTF-8 reason.
        reason: String,
    },
}

impl Frame {
    /// Return the exact encoded frame length in bytes.
    ///
    /// This is primarily useful for pre-allocating encode buffers.
    pub fn encoded_len(&self) -> usize {
        match self {
            Frame::OpenUni { id } | Frame::OpenBi { id } => 1 + VarInt(id.0).size(),
            Frame::Stream { id, data, fin } => {
                1 + VarInt(id.0).size()
                    + VarInt(u64::from(*fin)).size()
                    + VarInt(data.len() as u64).size()
                    + data.len()
            }
            Frame::ResetStream { id, code } | Frame::StopSending { id, code } => {
                1 + VarInt(id.0).size() + VarInt(*code).size()
            }
            Frame::MaxStreamData { id, max } => 1 + VarInt(id.0).size() + VarInt(*max).size(),
            Frame::ConnectionClose { code, reason } => {
                1 + VarInt(*code).size() + VarInt(reason.len() as u64).size() + reason.len()
            }
        }
    }

    /// Encode this frame into its canonical wire representation.
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.encoded_len());
        match self {
            Frame::OpenUni { id } => {
                VarInt(0).encode(&mut buf);
                VarInt(id.0).encode(&mut buf);
            }
            Frame::OpenBi { id } => {
                VarInt(1).encode(&mut buf);
                VarInt(id.0).encode(&mut buf);
            }
            Frame::Stream { id, data, fin } => {
                VarInt(2).encode(&mut buf);
                VarInt(id.0).encode(&mut buf);
                VarInt(u64::from(*fin)).encode(&mut buf);
                VarInt(data.len() as u64).encode(&mut buf);
                buf.put_slice(data);
            }
            Frame::ResetStream { id, code } => {
                VarInt(3).encode(&mut buf);
                VarInt(id.0).encode(&mut buf);
                VarInt(*code).encode(&mut buf);
            }
            Frame::StopSending { id, code } => {
                VarInt(4).encode(&mut buf);
                VarInt(id.0).encode(&mut buf);
                VarInt(*code).encode(&mut buf);
            }
            Frame::MaxStreamData { id, max } => {
                VarInt(6).encode(&mut buf);
                VarInt(id.0).encode(&mut buf);
                VarInt(*max).encode(&mut buf);
            }
            Frame::ConnectionClose { code, reason } => {
                VarInt(5).encode(&mut buf);
                VarInt(*code).encode(&mut buf);
                VarInt(reason.len() as u64).encode(&mut buf);
                buf.put_slice(reason.as_bytes());
            }
        }
        buf
    }

    /// Decode one complete frame from the front of a byte buffer.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, FrameDecodeError> {
        let tag = VarInt::decode(buf)?.into_inner();
        match tag {
            0 => Ok(Frame::OpenUni {
                id: StreamId(VarInt::decode(buf)?.into_inner()),
            }),
            1 => Ok(Frame::OpenBi {
                id: StreamId(VarInt::decode(buf)?.into_inner()),
            }),
            2 => {
                let id = StreamId(VarInt::decode(buf)?.into_inner());
                let fin = match VarInt::decode(buf)?.into_inner() {
                    0 => false,
                    1 => true,
                    value => return Err(FrameDecodeError::InvalidFin(value)),
                };
                let len = VarInt::decode(buf)?.into_inner() as usize;
                if buf.remaining() < len {
                    return Err(FrameDecodeError::UnexpectedEnd);
                }
                let data = buf.copy_to_bytes(len);
                Ok(Frame::Stream { id, data, fin })
            }
            3 => Ok(Frame::ResetStream {
                id: StreamId(VarInt::decode(buf)?.into_inner()),
                code: VarInt::decode(buf)?.into_inner(),
            }),
            4 => Ok(Frame::StopSending {
                id: StreamId(VarInt::decode(buf)?.into_inner()),
                code: VarInt::decode(buf)?.into_inner(),
            }),
            5 => {
                let code = VarInt::decode(buf)?.into_inner();
                let len = VarInt::decode(buf)?.into_inner() as usize;
                if buf.remaining() < len {
                    return Err(FrameDecodeError::UnexpectedEnd);
                }
                let data = buf.copy_to_bytes(len);
                let reason = std::str::from_utf8(data.as_ref())
                    .map_err(|_| FrameDecodeError::InvalidUtf8)?
                    .to_owned();
                Ok(Frame::ConnectionClose { code, reason })
            }
            6 => Ok(Frame::MaxStreamData {
                id: StreamId(VarInt::decode(buf)?.into_inner()),
                max: VarInt::decode(buf)?.into_inner(),
            }),
            _ => Err(FrameDecodeError::UnknownTag(tag)),
        }
    }
}

/// Errors produced while decoding a mux frame.
#[derive(Debug, thiserror::Error)]
pub enum FrameDecodeError {
    /// The buffer ended before the frame was complete.
    #[error("unexpected end of buffer")]
    UnexpectedEnd,
    /// The frame tag is not defined by this protocol version.
    #[error("unknown frame tag {0}")]
    UnknownTag(u64),
    /// A connection-close reason was not valid UTF-8.
    #[error("invalid utf-8 in reason")]
    InvalidUtf8,
    /// A stream FIN field contained a value other than zero or one.
    #[error("invalid stream FIN value {0}")]
    InvalidFin(u64),
}

impl From<VarIntUnexpectedEnd> for FrameDecodeError {
    fn from(_: VarIntUnexpectedEnd) -> Self {
        FrameDecodeError::UnexpectedEnd
    }
}
