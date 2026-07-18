//! Core multiplexing trait contracts shared by native and WebAssembly backends.

use bytes::Bytes;
use websock_proto::{LocalBoxFuture, Result};

/// Cross-platform write-side stream contract.
pub trait MuxSendStream {
    /// Write one chunk.
    fn write_buf<'a>(&'a self, data: Bytes) -> LocalBoxFuture<'a, Result<()>>;

    /// Finish the stream.
    fn finish<'a>(&'a self) -> LocalBoxFuture<'a, Result<()>>;

    /// Reset the stream with an application-defined code.
    fn reset<'a>(&'a self, code: u64) -> LocalBoxFuture<'a, Result<()>>;

    /// Return true if the stream is closed for sending.
    fn closed(&self) -> bool;
}

/// Cross-platform read-side stream contract.
pub trait MuxRecvStream {
    /// Read at most `max` bytes from the next chunk.
    fn read_chunk<'a>(&'a mut self, max: usize) -> LocalBoxFuture<'a, Result<Option<Bytes>>>;

    /// Ask peer to stop sending with an application-defined code.
    fn stop<'a>(&'a self, code: u64) -> LocalBoxFuture<'a, Result<()>>;

    /// Return true if the stream is closed for receiving.
    fn closed(&self) -> bool;
}

/// Cross-platform mux session contract.
pub trait MuxSession {
    /// Concrete send-stream type.
    type SendStream: MuxSendStream;
    /// Concrete receive-stream type.
    type RecvStream: MuxRecvStream;

    /// Open a unidirectional stream.
    fn open_uni<'a>(&'a self) -> LocalBoxFuture<'a, Result<Self::SendStream>>;

    /// Open a bidirectional stream.
    fn open_bi<'a>(&'a self) -> LocalBoxFuture<'a, Result<(Self::SendStream, Self::RecvStream)>>;

    /// Accept a peer-initiated unidirectional stream.
    fn accept_uni<'a>(&'a self) -> LocalBoxFuture<'a, Result<Self::RecvStream>>;

    /// Accept a peer-initiated bidirectional stream.
    fn accept_bi<'a>(&'a self) -> LocalBoxFuture<'a, Result<(Self::SendStream, Self::RecvStream)>>;

    /// Close the underlying WebSocket and wait for session tasks to finish.
    fn shutdown<'a>(&'a self) -> LocalBoxFuture<'a, Result<()>>;

    /// Return true if the session has closed.
    fn closed(&self) -> bool;
}
