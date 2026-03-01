//! Tokio + tokio-tungstenite based WebSocket multiplexing transport.
//!
//! This crate provides a QUIC/WebTransport-like logical stream interface over a single WebSocket.

mod builder;
mod client;
mod server;
mod session;
pub mod tls;

pub use builder::{ClientBuilder, ServerBuilder};
pub use client::Client;
pub use server::{Server, bind};
pub use session::Limits;
pub use session::{RecvStream, SendStream, Session};
pub use tls::{
    TlsClientConfig, TlsClientConfigBuilder, TlsConfig, TlsServerConfig, TlsServerConfigBuilder,
};

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use tokio::sync::mpsc;
    use websock_mux_proto::{Frame, StreamDir, StreamId};

    use crate::session::Limits;
    use crate::session::SessionInner;

    #[test]
    fn frame_roundtrip() {
        let id = StreamId(4);
        let frame = Frame::Stream {
            id,
            data: Bytes::from_static(b"hello"),
            fin: true,
        };
        let mut buf = frame.encode();
        let decoded = Frame::decode(&mut buf).expect("decode");
        assert_eq!(frame, decoded);
    }

    #[tokio::test]
    async fn stream_open_data_fin() {
        let (outbound_tx, _outbound_rx) = mpsc::channel(4);
        let (accept_uni_tx, mut accept_uni_rx) = mpsc::channel(4);
        let (accept_bi_tx, _accept_bi_rx) = mpsc::channel(4);
        let inner = std::sync::Arc::new(SessionInner::new(
            false,
            Limits::default(),
            outbound_tx,
            accept_uni_tx,
            accept_bi_tx,
        ));
        let id = StreamId::new(0, true, StreamDir::Uni).expect("stream id");
        inner
            .clone()
            .handle_frame(Frame::OpenUni { id })
            .await
            .expect("open");
        let mut recv = accept_uni_rx.recv().await.expect("recv stream");
        let data = Bytes::from_static(b"ping");
        inner
            .clone()
            .handle_frame(Frame::Stream {
                id,
                data: data.clone(),
                fin: true,
            })
            .await
            .expect("stream");
        let mut buf = BytesMut::new();
        let n = recv.read_buf::<BytesMut>(&mut buf).await.expect("read");
        assert_eq!(n, Some(4));
        assert_eq!(buf.as_ref(), data.as_ref());
        let max_size: usize = 1024;
        let end = recv.read_chunk(max_size).await.expect("fin");
        assert!(end.is_none());
    }
}
