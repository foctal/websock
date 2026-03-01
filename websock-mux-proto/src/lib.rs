//! Multiplexing protocol primitives and core trait contracts shared by
//! native and WebAssembly mux transports.

pub mod stream;
pub mod transport;
pub mod varint;

pub use stream::{Frame, FrameDecodeError, StreamDir, StreamId};
pub use transport::{MuxRecvStream, MuxSendStream, MuxSession};
pub use varint::{VarInt, VarIntBoundsExceeded, VarIntUnexpectedEnd};

pub const SUBPROTOCOL: &str = "websock-mux-1";
