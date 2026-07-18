use thiserror::Error;

/// Result type for protocol and transport operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur when sending or receiving WebSocket messages.
#[derive(Debug, Error)]
pub enum Error {
    /// The connection has been closed.
    #[error("connection closed")]
    Closed,

    /// The provided URL was not valid.
    #[error("invalid url: {0}")]
    InvalidUrl(String),

    /// A protocol violation or malformed message was encountered.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// An IO failure occurred while reading or writing.
    #[error("io error: {0}")]
    Io(String),

    /// A TLS handshake or validation error occurred.
    #[error("tls error: {0}")]
    Tls(String),

    /// The operation is not supported on the current platform or configuration.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// A multiplexing frame could not be decoded.
    #[error("frame decode error: {0}")]
    FrameDecode(String),

    /// A multiplexing stream identifier was invalid.
    #[error("stream id error: {0}")]
    StreamId(String),

    /// A catch-all error for unexpected failures.
    #[error("other error: {0}")]
    Other(String),
}

impl Error {
    /// Create an `Error::Other` from any displayable error value.
    pub fn other<E: std::fmt::Display>(e: E) -> Self {
        Self::Other(e.to_string())
    }
}
