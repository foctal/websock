use tokio_tungstenite::tungstenite;
use tungstenite::client::IntoClientRequest;
use tungstenite::http;
use tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;

use std::sync::Arc;

use rustls::ClientConfig;
use tokio_tungstenite::Connector;
use websock_proto::{Error, Result};

use crate::Session;
use crate::session::Limits;
use crate::session::map_tungstenite_err;
use websock_mux_proto::SUBPROTOCOL;

fn negotiated_protocol(resp: &http::Response<Option<Vec<u8>>>) -> Option<&str> {
    resp.headers()
        .get(SEC_WEBSOCKET_PROTOCOL)?
        .to_str()
        .ok()
        .map(|s| s.trim())
}

fn is_protocol_ok(p: &str) -> bool {
    p.eq_ignore_ascii_case(SUBPROTOCOL)
}

fn validate_client_protocols(opts: &websock_proto::ConnectOptions) -> Result<()> {
    for p in &opts.protocols {
        tungstenite::http::HeaderValue::from_str(p)
            .map_err(|e| Error::Protocol(format!("invalid protocol value: {e}")))?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) opts: websock_proto::ConnectOptions,
    pub(crate) tls: Option<Arc<ClientConfig>>,
    pub(crate) limits: Limits,
}

impl Client {
    pub fn options(&self) -> &websock_proto::ConnectOptions {
        &self.opts
    }

    /// Return the configured TLS client config (if any).
    pub fn tls_config(&self) -> Option<&Arc<ClientConfig>> {
        self.tls.as_ref()
    }

    pub async fn connect(&self, url: &str) -> Result<Session> {
        self.connect_with_tls(url, self.tls.clone()).await
    }

    /// Connect with an explicit TLS configuration.
    ///
    /// When `tls` is `None`, the default Tokio Tungstenite TLS settings are used
    /// (and plain `ws://` works as-is).
    pub async fn connect_with_tls(
        &self,
        url: &str,
        tls: Option<Arc<ClientConfig>>,
    ) -> Result<Session> {
        self.limits.validate()?;
        validate_client_protocols(&self.opts)?;

        let mut request = url.into_client_request().map_err(Error::transport)?;

        let headers = request.headers_mut();
        for (k, v) in self.opts.headers.iter() {
            let name = tungstenite::http::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| Error::Protocol(format!("invalid header name: {e}")))?;
            let value = tungstenite::http::header::HeaderValue::from_str(v)
                .map_err(|e| Error::Protocol(format!("invalid header value: {e}")))?;
            headers.append(name, value);
        }

        // Apply subprotocols.
        if !self.opts.protocols.is_empty() {
            let joined = self.opts.protocols.join(",");
            let value = tungstenite::http::header::HeaderValue::from_str(&joined)
                .map_err(|e| Error::Protocol(format!("invalid protocol value: {e}")))?;
            headers.insert(SEC_WEBSOCKET_PROTOCOL, value);
        }

        let connector = tls.map(Connector::Rustls);
        let config = self.limits.websocket_config();
        let (stream, response) = tokio_tungstenite::connect_async_tls_with_config(
            request,
            Some(config),
            false,
            connector,
        )
        .await
        .map_err(map_tungstenite_err)?;

        let proto = negotiated_protocol(&response)
            .ok_or_else(|| Error::Protocol("missing SEC_WEBSOCKET_PROTOCOL in response".into()))?;
        if !is_protocol_ok(proto) {
            return Err(Error::Protocol(format!("subprotocol mismatch: {proto}")));
        }

        Session::new(stream, false, self.limits.clone())
    }
}
