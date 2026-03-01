//! Browser (wasm32) WebSocket multiplexing transport.
//!
//! This crate provides a QUIC/WebTransport-like *logical stream* interface over a single
//! browser WebSocket connection.

mod builder;
mod client;
mod session;

pub use builder::ClientBuilder;
pub use client::Client;
pub use session::{Limits, RecvStream, SendStream, Session};

#[cfg(test)]
mod tests {
    use super::{RecvStream, SendStream};

    #[test]
    fn async_traits_are_implemented() {
        fn assert_async_write<T: futures_io::AsyncWrite + Unpin>() {}
        fn assert_async_read<T: futures_io::AsyncRead + Unpin>() {}
        assert_async_write::<SendStream>();
        assert_async_read::<RecvStream>();
    }
}
