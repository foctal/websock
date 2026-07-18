//! Multiplexing protocol primitives and core trait contracts shared by
//! native and WebAssembly mux transports.

#![warn(missing_docs)]

/// Mux stream identifiers and wire frames.
pub mod stream;
pub mod transport;
pub mod varint;

pub use stream::{Frame, FrameDecodeError, StreamDir, StreamId};
pub use transport::{MuxRecvStream, MuxSendStream, MuxSession};
pub use varint::{VarInt, VarIntBoundsExceeded, VarIntUnexpectedEnd};

/// WebSocket subprotocol identifier for mux wire protocol version 1.
pub const SUBPROTOCOL: &str = "websock-mux-1";
