//! Connection management for the Tokio Tungstenite transport.

use rustls::ClientConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{Connector, WebSocketStream, tungstenite};
use tungstenite::client::IntoClientRequest;
use websock_proto::{CloseFrame, ConnectOptions, Error, Message, Result};

#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Remote peer address for the connection.
    pub peer: std::net::SocketAddr,
    /// Local socket address for the connection.
    pub local: std::net::SocketAddr,
    /// True when the connection is established over TLS.
    pub is_tls: bool,
    /// WebSocket subprotocol selected during the opening handshake.
    pub subprotocol: Option<String>,
}

/// Establish a WebSocket connection using Tokio Tungstenite.
pub async fn connect(url: &str, opts: ConnectOptions) -> Result<Connection> {
    connect_with_tls(url, opts, None).await
}

/// Establish a WebSocket connection using a custom TLS configuration.
pub async fn connect_with_tls(
    url: &str,
    opts: ConnectOptions,
    tls: Option<Arc<ClientConfig>>,
) -> Result<Connection> {
    opts.limits.validate()?;
    let write_buffer_size = (128 * 1024).min(opts.limits.max_write_buffer_size.saturating_sub(1));
    let config = tungstenite::protocol::WebSocketConfig::default()
        .read_buffer_size((128 * 1024).min(opts.limits.max_frame_size))
        .write_buffer_size(write_buffer_size)
        .max_write_buffer_size(opts.limits.max_write_buffer_size)
        .max_message_size(Some(opts.limits.max_message_size))
        .max_frame_size(Some(opts.limits.max_frame_size));
    let mut req = url
        .into_client_request()
        .map_err(|e| Error::InvalidUrl(e.to_string()))?;

    // Apply configured headers and subprotocols.
    {
        let headers = req.headers_mut();
        for (k, v) in opts.headers {
            let name = tungstenite::http::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| Error::Protocol(format!("invalid header name: {e}")))?;
            let value = tungstenite::http::header::HeaderValue::from_str(&v)
                .map_err(|e| Error::Protocol(format!("invalid header value: {e}")))?;
            headers.append(name, value);
        }

        // Apply subprotocols.
        if !opts.protocols.is_empty() {
            let joined = opts.protocols.join(",");
            let value = tungstenite::http::header::HeaderValue::from_str(&joined)
                .map_err(|e| Error::Protocol(format!("invalid protocol value: {e}")))?;
            headers.insert(tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL, value);
        }
    }

    let connector = tls.map(Connector::Rustls);
    let (ws, resp) =
        tokio_tungstenite::connect_async_tls_with_config(req, Some(config), false, connector)
            .await
            .map_err(map_tungstenite_err)?;

    let info = ConnectionInfo {
        peer: ws
            .get_ref()
            .get_ref()
            .peer_addr()
            .map_err(|e| Error::Io(e.to_string()))?,
        local: ws
            .get_ref()
            .get_ref()
            .local_addr()
            .map_err(|e| Error::Io(e.to_string()))?,
        is_tls: matches!(ws.get_ref(), tokio_tungstenite::MaybeTlsStream::Rustls(_)),
        subprotocol: resp
            .headers()
            .get(tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
    };

    Ok(Connection {
        ws,
        info,
        close_frame: None,
    })
}

/// WebSocket connection wrapper around a Tokio Tungstenite stream.
pub struct Connection<S = tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    pub(crate) ws: WebSocketStream<S>,
    pub(crate) info: ConnectionInfo,
    pub(crate) close_frame: Option<CloseFrame>,
}

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Send a text or binary message.
    pub async fn send(&mut self, msg: Message) -> Result<()> {
        use futures_util::SinkExt;

        let tmsg = match msg {
            Message::Text(s) => tungstenite::Message::Text(s.into()),
            Message::Binary(b) => tungstenite::Message::Binary(b),
        };

        self.ws.send(tmsg).await.map_err(map_tungstenite_err)?;
        Ok(())
    }

    /// Receive the next text or binary message, responding to pings as needed.
    pub async fn recv(&mut self) -> Result<Message> {
        use futures_util::{SinkExt, StreamExt};

        loop {
            let item = self.ws.next().await.ok_or(Error::Closed)?;
            let msg = item.map_err(map_tungstenite_err)?;

            match msg {
                tungstenite::Message::Ping(p) => {
                    self.ws
                        .send(tungstenite::Message::Pong(p))
                        .await
                        .map_err(map_tungstenite_err)?;
                    continue;
                }
                tungstenite::Message::Pong(_) => continue,
                tungstenite::Message::Text(s) => return Ok(Message::Text(s.to_string())),
                tungstenite::Message::Binary(b) => return Ok(Message::Binary(b)),
                tungstenite::Message::Close(frame) => {
                    self.close_frame = frame.map(|frame| CloseFrame {
                        code: frame.code.into(),
                        reason: frame.reason.to_string(),
                    });
                    let _ = self.ws.close(None).await;
                    return Err(Error::Closed);
                }
                _ => return Err(Error::Protocol("unsupported ws message".into())),
            }
        }
    }

    /// Close the WebSocket connection gracefully.
    pub async fn close(&mut self) -> Result<()> {
        self.ws.close(None).await.map_err(map_tungstenite_err)?;
        Ok(())
    }

    /// Borrow the underlying transport stream.
    pub fn get_ref(&self) -> &S {
        self.ws.get_ref()
    }

    /// Mutably borrow the underlying transport stream.
    pub fn get_mut(&mut self) -> &mut S {
        self.ws.get_mut()
    }
}

impl<S> websock_proto::WebSocketConnection for Connection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn send<'a>(&'a mut self, msg: Message) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { Connection::send(self, msg).await })
    }

    fn recv<'a>(&'a mut self) -> websock_proto::LocalBoxFuture<'a, Result<Message>> {
        Box::pin(async move { Connection::recv(self).await })
    }

    fn close<'a>(&'a mut self) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { Connection::close(self).await })
    }
}

impl<S> Connection<S> {
    /// Return the peer address.
    pub fn peer_addr(&self) -> SocketAddr {
        self.info.peer
    }
    /// Return the local address.
    pub fn local_addr(&self) -> SocketAddr {
        self.info.local
    }
    /// Report whether TLS is in use.
    pub fn is_tls(&self) -> bool {
        self.info.is_tls
    }
    /// Return the full connection metadata snapshot.
    pub fn info(&self) -> ConnectionInfo {
        self.info.clone()
    }

    /// Return the negotiated WebSocket subprotocol, if any.
    pub fn negotiated_subprotocol(&self) -> Option<&str> {
        self.info.subprotocol.as_deref()
    }

    /// Return the most recently received close-frame metadata, if any.
    pub fn close_frame(&self) -> Option<CloseFrame> {
        self.close_frame.clone()
    }
}

/// Map tungstenite errors into the shared error type.
pub(crate) fn map_tungstenite_err(e: tungstenite::Error) -> Error {
    use tungstenite::Error as E;
    match e {
        E::ConnectionClosed | E::AlreadyClosed => Error::Closed,
        E::Io(io) => Error::Io(io.to_string()),
        E::Tls(tls) => Error::Tls(tls.to_string()),
        E::Url(url) => Error::InvalidUrl(url.to_string()),
        E::Protocol(err) => Error::Protocol(err.to_string()),
        E::Utf8(err) => Error::Protocol(err),
        E::Capacity(err) => Error::Protocol(err.to_string()),
        E::HttpFormat(err) => Error::Protocol(err.to_string()),
        other => Error::Other(other.to_string()),
    }
}
