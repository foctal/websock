//! Sink/Stream split helpers for Tokio Tungstenite connections.

use crate::Connection;
use crate::connection::map_tungstenite_err;
use futures_core::Stream;
use futures_sink::Sink;
use futures_util::SinkExt;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite as tt;
use tokio_util::sync::PollSender;
use websock_proto::{Bytes, Error, Message, Result};

#[derive(Debug)]
enum WriterCmd {
    /// Send a text or binary message.
    Msg(Message),
    /// Send a pong in response to a ping.
    Pong(Bytes),
    /// Close the underlying WebSocket.
    Close,
}

/// Sender side (Sink).
pub struct ConnectionSink {
    tx: PollSender<WriterCmd>,
    /// Track closure to keep close idempotent.
    closed: bool,
}

impl ConnectionSink {
    /// Create a sink backed by the writer command channel.
    fn new(tx: mpsc::Sender<WriterCmd>) -> Self {
        Self {
            tx: PollSender::new(tx),
            closed: false,
        }
    }
}

impl Sink<Message> for ConnectionSink {
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Err(Error::Closed));
        }
        this.tx.poll_reserve(cx).map_err(|_| Error::Closed)
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> std::result::Result<(), Self::Error> {
        let this = self.get_mut();
        if this.closed {
            return Err(Error::Closed);
        }
        this.tx
            .send_item(WriterCmd::Msg(item))
            .map_err(|_| Error::Closed)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        this.tx.poll_flush_unpin(cx).map_err(|_| Error::Closed)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        if !this.closed {
            // Request a graceful close.
            match this.tx.poll_reserve(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => {
                    this.closed = true;
                    return Poll::Ready(Err(Error::Closed));
                }
                Poll::Ready(Ok(())) => {
                    let _ = this.tx.send_item(WriterCmd::Close);
                    this.closed = true;
                }
            }
        }
        this.tx.poll_flush_unpin(cx).map_err(|_| Error::Closed)
    }
}

impl Drop for ConnectionSink {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        if let Some(tx) = self.tx.get_ref() {
            let _ = tx.try_send(WriterCmd::Close);
        }
        self.closed = true;
    }
}

/// Receiver side (Stream).
pub struct ConnectionStream {
    rx: mpsc::Receiver<Result<Message>>,
    terminated: bool,
}

impl ConnectionStream {
    /// Create a stream backed by the reader channel.
    fn new(rx: mpsc::Receiver<Result<Message>>) -> Self {
        Self {
            rx,
            terminated: false,
        }
    }
}

impl Stream for ConnectionStream {
    type Item = Result<Message>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }

        match Pin::new(&mut this.rx).poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.terminated = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
        }
    }
}

/// Split a connection into sink and stream halves.
pub fn split<S>(conn: Connection<S>) -> (ConnectionSink, ConnectionStream)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    use futures_util::{SinkExt, StreamExt};
    // Command queue for the writer task.
    let (tx_cmd, mut rx_cmd) = mpsc::channel::<WriterCmd>(64);
    // Queue from the reader task back to the application.
    let (tx_msg, rx_msg) = mpsc::channel::<Result<Message>>(64);

    // Split the WebSocket stream using futures_util's split helper.
    let (mut ws_writer, mut ws_reader) = conn.ws.split();

    // Writer task.
    tokio::spawn(async move {
        while let Some(cmd) = rx_cmd.recv().await {
            match cmd {
                WriterCmd::Msg(m) => {
                    let tmsg = match m {
                        Message::Text(s) => tt::Message::Text(s.into()),
                        Message::Binary(b) => tt::Message::Binary(b),
                    };
                    if ws_writer.send(tmsg).await.is_err() {
                        break;
                    }
                }
                WriterCmd::Pong(p) => {
                    // Best-effort pong response.
                    let _ = ws_writer.send(tt::Message::Pong(p)).await;
                }
                WriterCmd::Close => {
                    let _ = ws_writer.close().await;
                    break;
                }
            }
        }
    });

    // Reader task.
    let tx_cmd_for_pong = tx_cmd.clone();
    tokio::spawn(async move {
        use futures_util::StreamExt;

        loop {
            let item = ws_reader.next().await;
            let item = match item {
                None => break,
                Some(Err(e)) => {
                    let _ = tx_msg.send(Err(map_tungstenite_err(e))).await;
                    break;
                }
                Some(Ok(m)) => m,
            };

            match item {
                tt::Message::Ping(p) => {
                    // Respond to Ping with Pong.
                    let _ = tx_cmd_for_pong.send(WriterCmd::Pong(p)).await;
                    continue;
                }
                tt::Message::Pong(_) => continue,

                tt::Message::Text(s) => {
                    if tx_msg.send(Ok(Message::Text(s.to_string()))).await.is_err() {
                        break;
                    }
                }
                tt::Message::Binary(b) => {
                    if tx_msg.send(Ok(Message::Binary(b))).await.is_err() {
                        break;
                    }
                }
                tt::Message::Close(_) => {
                    let _ = tx_msg.send(Err(Error::Closed)).await;
                    // Signal the writer task to close.
                    let _ = tx_cmd_for_pong.send(WriterCmd::Close).await;
                    break;
                }
                _ => {
                    let _ = tx_msg
                        .send(Err(Error::Protocol("unsupported ws message".into())))
                        .await;
                    let _ = tx_cmd_for_pong.send(WriterCmd::Close).await;
                    break;
                }
            }
        }
    });

    (ConnectionSink::new(tx_cmd), ConnectionStream::new(rx_msg))
}
