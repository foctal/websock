use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::varint::{VarInt, VarIntBoundsExceeded, VarIntUnexpectedEnd};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StreamDir {
    Bi,
    Uni,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct StreamId(pub u64);

impl StreamId {
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
        let value = counter.checked_shl(2).ok_or(VarIntBoundsExceeded)?
            | ((dir_bit as u64) << 1)
            | (initiator as u64);
        VarInt::from_u64(value)?;
        Ok(Self(value))
    }

    pub fn dir(self) -> StreamDir {
        if (self.0 >> 1) & 1 == 1 {
            StreamDir::Uni
        } else {
            StreamDir::Bi
        }
    }

    pub fn initiator_is_server(self) -> bool {
        self.0 & 1 == 1
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    OpenUni {
        id: StreamId,
    },
    OpenBi {
        id: StreamId,
    },
    Stream {
        id: StreamId,
        data: Bytes,
        fin: bool,
    },
    ResetStream {
        id: StreamId,
        code: u64,
    },
    StopSending {
        id: StreamId,
        code: u64,
    },
    MaxStreamData {
        id: StreamId,
        max: u64,
    },
    ConnectionClose {
        code: u64,
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
                let fin = VarInt::decode(buf)?.into_inner() != 0;
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

#[derive(Debug, thiserror::Error)]
pub enum FrameDecodeError {
    #[error("unexpected end of buffer")]
    UnexpectedEnd,
    #[error("unknown frame tag {0}")]
    UnknownTag(u64),
    #[error("invalid utf-8 in reason")]
    InvalidUtf8,
}

impl From<VarIntUnexpectedEnd> for FrameDecodeError {
    fn from(_: VarIntUnexpectedEnd) -> Self {
        FrameDecodeError::UnexpectedEnd
    }
}
