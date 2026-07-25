use std::error::Error as StdError;
use std::io;

use thiserror::Error;

/// Result type for protocol and transport operations.
pub type Result<T> = std::result::Result<T, Error>;

/// A thread-safe, type-erased error retained as the source of a transport error.
pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

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
    Io(#[from] io::Error),

    /// A TLS handshake or validation error occurred.
    #[error("tls error: {0}")]
    Tls(#[source] BoxError),

    /// An error reported by the underlying WebSocket transport.
    #[error("transport error: {0}")]
    Transport(#[source] BoxError),

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
    /// Create an `Error::Tls` while retaining the concrete source error.
    pub fn tls<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Tls(Box::new(error))
    }

    /// Create an `Error::Transport` while retaining the concrete source error.
    pub fn transport<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Transport(Box::new(error))
    }

    /// Create an `Error::Other` from any displayable error value.
    pub fn other<E: std::fmt::Display>(e: E) -> Self {
        Self::Other(e.to_string())
    }

    /// Return the underlying I/O error kind, when this is an I/O error.
    pub fn io_kind(&self) -> Option<io::ErrorKind> {
        match self {
            Self::Io(error) => Some(error.kind()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::Error;

    #[test]
    fn io_error_preserves_kind_and_source() {
        let error = Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "peer reset",
        ));

        assert_eq!(error.io_kind(), Some(std::io::ErrorKind::ConnectionReset));
        assert_eq!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::ConnectionReset)
        );
    }

    #[test]
    fn transport_error_preserves_concrete_source() {
        let error = Error::transport(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "transport timeout",
        ));

        assert_eq!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::TimedOut)
        );
    }
}
