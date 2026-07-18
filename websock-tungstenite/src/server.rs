//! Server-side WebSocket acceptor for the Tokio Tungstenite transport.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{accept_hdr_async_with_config, tungstenite};
use tungstenite::handshake::server::{Request, Response};
use tungstenite::http::header::{HeaderName, HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use websock_proto::{Error, Result, ServerOptions};

use crate::Connection;
use crate::connection::{ConnectionInfo, map_tungstenite_err};

/// Bind a WebSocket server listener.
pub async fn bind<A>(
    addr: A,
    opts: ServerOptions,
    tls: Option<rustls::ServerConfig>,
) -> Result<Server>
where
    A: ToSocketAddrs,
{
    opts.limits.validate()?;
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Io(e.to_string()))?;
    let headers = prepare_headers(&opts)?;
    validate_protocols(&opts)?;

    let acceptor = tls.map(|cfg| TlsAcceptor::from(Arc::new(cfg)));
    let write_buffer_size = (128 * 1024).min(opts.limits.max_write_buffer_size.saturating_sub(1));
    let config = tungstenite::protocol::WebSocketConfig::default()
        .read_buffer_size((128 * 1024).min(opts.limits.max_frame_size))
        .write_buffer_size(write_buffer_size)
        .max_write_buffer_size(opts.limits.max_write_buffer_size)
        .max_message_size(Some(opts.limits.max_message_size))
        .max_frame_size(Some(opts.limits.max_frame_size));

    Ok(Server {
        listener,
        protocols: Arc::new(opts.protocols.into_iter().collect()),
        headers: Arc::new(headers),
        acceptor,
        config,
    })
}

/// Marker trait for IO types usable by the server.
pub trait ServerIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> ServerIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Boxed stream type used for server connections.
pub type ServerStream = Box<dyn ServerIo>;

/// WebSocket server listener.
pub struct Server {
    listener: TcpListener,
    protocols: Arc<HashSet<String>>,
    headers: Arc<Vec<(HeaderName, HeaderValue)>>,
    acceptor: Option<TlsAcceptor>,
    config: tungstenite::protocol::WebSocketConfig,
}

impl Server {
    /// Accept an incoming WebSocket connection.
    #[allow(clippy::result_large_err)]
    pub async fn accept(&self) -> Result<Connection<ServerStream>> {
        let (stream, _addr) = self
            .listener
            .accept()
            .await
            .map_err(|e| Error::Io(e.to_string()))?;

        let peer = stream.peer_addr().map_err(|e| Error::Io(e.to_string()))?;
        let local = stream.local_addr().map_err(|e| Error::Io(e.to_string()))?;

        let (stream, is_tls): (ServerStream, bool) = if let Some(acceptor) = &self.acceptor {
            let tls_stream = acceptor
                .accept(stream)
                .await
                .map_err(|e| Error::Tls(e.to_string()))?;
            (Box::new(tls_stream), true)
        } else {
            (Box::new(stream), false)
        };

        let mut info = ConnectionInfo {
            peer,
            local,
            is_tls,
            subprotocol: None,
        };

        let headers = Arc::clone(&self.headers);
        let protocols = Arc::clone(&self.protocols);
        let selected_protocol = Arc::new(Mutex::new(None));
        let selected_protocol_callback = Arc::clone(&selected_protocol);

        let ws = accept_hdr_async_with_config(
            stream,
            move |req: &Request, mut resp: Response| {
                for (name, value) in headers.iter() {
                    resp.headers_mut().append(name, value.clone());
                }

                if let Some(protocol) = select_protocol(req, protocols.as_ref()) {
                    let value =
                        HeaderValue::from_str(protocol).expect("protocol value validated on bind");
                    resp.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, value);
                    *selected_protocol_callback
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(protocol.to_owned());
                }

                Ok(resp)
            },
            Some(self.config),
        )
        .await
        .map_err(map_tungstenite_err)?;

        info.subprotocol = selected_protocol
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok(Connection {
            ws,
            info,
            close_frame: None,
        })
    }

    /// Accept an incoming WebSocket connection, returning the TLS stream type.
    #[allow(clippy::result_large_err)]
    pub async fn accept_tls(
        &self,
    ) -> Result<Connection<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>> {
        let (stream, _addr) = self
            .listener
            .accept()
            .await
            .map_err(|e| Error::Io(e.to_string()))?;

        let tls_stream = self
            .acceptor
            .as_ref()
            .ok_or_else(|| Error::Tls("missing tls acceptor".into()))?
            .accept(stream)
            .await
            .map_err(|e| Error::Tls(e.to_string()))?;

        let headers = Arc::clone(&self.headers);
        let protocols = Arc::clone(&self.protocols);

        let mut info = ConnectionInfo {
            peer: tls_stream
                .get_ref()
                .0
                .peer_addr()
                .map_err(|e| Error::Io(e.to_string()))?,
            local: tls_stream
                .get_ref()
                .0
                .local_addr()
                .map_err(|e| Error::Io(e.to_string()))?,
            is_tls: true,
            subprotocol: None,
        };
        let selected_protocol = Arc::new(Mutex::new(None));
        let selected_protocol_callback = Arc::clone(&selected_protocol);

        let ws = accept_hdr_async_with_config(
            tls_stream,
            move |req: &Request, mut resp: Response| {
                for (name, value) in headers.iter() {
                    resp.headers_mut().append(name, value.clone());
                }

                if let Some(protocol) = select_protocol(req, protocols.as_ref()) {
                    let value =
                        HeaderValue::from_str(protocol).expect("protocol value validated on bind");
                    resp.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, value);
                    *selected_protocol_callback
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(protocol.to_owned());
                }

                Ok(resp)
            },
            Some(self.config),
        )
        .await
        .map_err(map_tungstenite_err)?;

        info.subprotocol = selected_protocol
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok(Connection {
            ws,
            info,
            close_frame: None,
        })
    }

    /// Return the local address of the listener.
    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|e| Error::Io(e.to_string()))
    }
}

/// Convert configured headers into tungstenite types.
fn prepare_headers(opts: &ServerOptions) -> Result<Vec<(HeaderName, HeaderValue)>> {
    let mut out = Vec::new();
    for (k, v) in &opts.headers {
        let name = HeaderName::from_bytes(k.as_bytes())
            .map_err(|e| Error::Protocol(format!("invalid header name: {e}")))?;
        let value = HeaderValue::from_str(v)
            .map_err(|e| Error::Protocol(format!("invalid header value: {e}")))?;
        out.push((name, value));
    }
    Ok(out)
}

/// Validate configured subprotocols before binding.
fn validate_protocols(opts: &ServerOptions) -> Result<()> {
    for protocol in &opts.protocols {
        HeaderValue::from_str(protocol)
            .map_err(|e| Error::Protocol(format!("invalid protocol value: {e}")))?;
    }
    Ok(())
}

/// Select the first requested subprotocol that appears in the allowed list.
fn select_protocol<'a>(req: &'a Request, allowed: &HashSet<String>) -> Option<&'a str> {
    if allowed.is_empty() {
        return None;
    }
    let header = req.headers().get(SEC_WEBSOCKET_PROTOCOL)?;
    let header = header.to_str().ok()?;
    header
        .split(',')
        .map(str::trim)
        .find(|candidate| allowed.contains(*candidate))
}
