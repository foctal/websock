use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::handshake::server;
use tungstenite::http;
use tungstenite::http::header::{HeaderName, HeaderValue, SEC_WEBSOCKET_PROTOCOL};

use websock_proto::{Error, Result};

use crate::Session;
use crate::session::Limits;
use crate::session::map_tungstenite_err;
use websock_mux_proto::SUBPROTOCOL;

/// Marker trait for IO types usable by the server.
pub trait ServerIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> ServerIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Boxed stream type used for server connections.
pub type ServerStream = Box<dyn ServerIo>;

/// Convert configured headers into tungstenite types.
fn prepare_headers(opts: &websock_proto::ServerOptions) -> Result<Vec<(HeaderName, HeaderValue)>> {
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
fn validate_protocols(opts: &websock_proto::ServerOptions) -> Result<()> {
    for protocol in &opts.protocols {
        HeaderValue::from_str(protocol)
            .map_err(|e| Error::Protocol(format!("invalid protocol value: {e}")))?;
    }
    Ok(())
}

/// Select the first requested subprotocol that appears in the allowed list.
fn select_protocol<'a>(req: &'a server::Request, allowed: &HashSet<String>) -> Option<&'a str> {
    if allowed.is_empty() {
        return None;
    }
    let header = req.headers().get(SEC_WEBSOCKET_PROTOCOL)?;
    let header = header.to_str().ok()?;
    header
        .split(',')
        .map(|s| s.trim())
        .find(|candidate| allowed.contains(*candidate))
}

/// Bind a WebSocket server listener.
pub async fn bind<A>(
    addr: A,
    opts: websock_proto::ServerOptions,
    tls: Option<rustls::ServerConfig>,
    limits: Limits,
) -> Result<Server>
where
    A: ToSocketAddrs,
{
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Io(e.to_string()))?;
    let headers = prepare_headers(&opts)?;
    validate_protocols(&opts)?;

    if !opts.protocols.iter().any(|p| p == SUBPROTOCOL) {
        return Err(Error::Protocol(
            "SUBPROTOCOL missing in ServerOptions".into(),
        ));
    }

    let allowed: HashSet<String> = opts.protocols.into_iter().collect();
    let acceptor = tls.map(|cfg| TlsAcceptor::from(Arc::new(cfg)));

    Ok(Server {
        listener,
        allowed: Arc::new(allowed),
        headers: Arc::new(headers),
        acceptor,
        limits,
    })
}

pub struct Server {
    listener: TcpListener,
    allowed: Arc<HashSet<String>>,
    headers: Arc<Vec<(HeaderName, HeaderValue)>>,
    acceptor: Option<TlsAcceptor>,
    limits: Limits,
}

impl Server {
    pub async fn accept(&self) -> Result<Session> {
        let (stream, _) = self
            .listener
            .accept()
            .await
            .map_err(|e| Error::Io(e.to_string()))?;

        let (stream, _is_tls): (ServerStream, bool) = if let Some(acceptor) = &self.acceptor {
            let tls_stream = acceptor
                .accept(stream)
                .await
                .map_err(|e| Error::Tls(e.to_string()))?;
            (Box::new(tls_stream), true)
        } else {
            (Box::new(stream), false)
        };

        let headers = Arc::clone(&self.headers);
        let allowed = Arc::clone(&self.allowed);

        let ws = accept_hdr_async(
            stream,
            move |req: &server::Request, mut resp: server::Response| {
                // Additional headers from configuration
                for (name, value) in headers.iter() {
                    resp.headers_mut().append(name, value.clone());
                }

                // websock-mux is required protocol
                let Some(protocol) = select_protocol(req, allowed.as_ref()) else {
                    return Err(http::Response::builder()
                        .status(http::StatusCode::BAD_REQUEST)
                        .body(Some(format!("'{SUBPROTOCOL}' protocol required")))
                        .unwrap());
                };

                // Confirm that the required protocol is present
                if !protocol.eq_ignore_ascii_case(SUBPROTOCOL) {
                    return Err(http::Response::builder()
                        .status(http::StatusCode::BAD_REQUEST)
                        .body(Some(format!("'{SUBPROTOCOL}' protocol required")))
                        .unwrap());
                }

                resp.headers_mut().insert(
                    http::header::SEC_WEBSOCKET_PROTOCOL,
                    http::HeaderValue::from_str(protocol).expect("validated"),
                );

                Ok(resp)
            },
        )
        .await
        .map_err(map_tungstenite_err)?;

        Session::new(ws, true, self.limits.clone())
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|e| Error::Io(e.to_string()))
    }
}
