//! Builders for clients and servers using the Tokio Tungstenite transport.

use crate::{Connection, Server};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use websock_proto::default_ws_alpn;
use websock_proto::{ConnectOptions, Error, Result, ServerOptions, WebSocketLimits};

/// Builder for creating a WebSocket client.
///
/// The resulting client can be reused for multiple `connect()` calls.
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    opts: ConnectOptions,
    tls: Option<ClientConfig>,
    alpn: Option<Vec<Vec<u8>>>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    /// Create a new client builder with default options.
    pub fn new() -> Self {
        Self {
            opts: ConnectOptions::default(),
            tls: None,
            alpn: None,
        }
    }

    /// Replace the builder options wholesale.
    pub fn with_options(mut self, opts: ConnectOptions) -> Self {
        self.opts = opts;
        self
    }

    /// Return a reference to the current options.
    pub fn options(&self) -> &ConnectOptions {
        &self.opts
    }

    /// Add a single header to the connection request.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.opts.headers.push((name.into(), value.into()));
        self
    }

    /// Add multiple headers to the connection request.
    pub fn with_headers<I, K, V>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (k, v) in headers {
            self.opts.headers.push((k.into(), v.into()));
        }
        self
    }

    /// Configure WebSocket message, frame, and write-buffer limits.
    pub fn with_limits(mut self, limits: WebSocketLimits) -> Self {
        self.opts.limits = limits;
        self
    }

    /// Add a single subprotocol.
    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.opts.protocols.push(protocol.into());
        self
    }

    /// Add multiple subprotocols.
    pub fn with_protocols<I, P>(mut self, protocols: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<String>,
    {
        for p in protocols {
            self.opts.protocols.push(p.into());
        }
        self
    }

    /// Configure a custom rustls client config (for `wss://`).
    pub fn with_tls_config(mut self, tls: ClientConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Build a client configured with the system trust store.
    pub fn with_system_roots(self) -> Result<Client> {
        let config = crate::tls::TlsClientConfigBuilder::new_with_native_certs()?.build();
        Ok(Client {
            opts: self.opts,
            tls: Some(Arc::new(config)),
        })
    }

    /// Build a client configured with a custom certificate chain.
    pub fn with_server_certificates<I>(self, chain: I) -> Result<Client>
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        let mut roots = RootCertStore::empty();
        for cert in chain {
            roots.add(CertificateDer::from(cert)).map_err(Error::tls)?;
        }
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        Ok(Client {
            opts: self.opts,
            tls: Some(Arc::new(config)),
        })
    }

    /// Enter the "dangerous" builder that can disable certificate verification.
    pub fn dangerous(self) -> DangerousClientBuilder {
        DangerousClientBuilder { opts: self.opts }
    }

    /// Configure ALPN with the default WebSocket protocol identifiers.
    pub fn with_default_alpn(mut self) -> Self {
        self.alpn = Some(default_ws_alpn());
        self
    }

    /// Configure ALPN with custom protocol identifiers.
    pub fn with_alpn_protocols(mut self, alpn: Vec<Vec<u8>>) -> Self {
        self.alpn = Some(alpn);
        self
    }

    fn build_tls_config(&self) -> Option<Arc<ClientConfig>> {
        let mut cfg = self.tls.clone()?;

        if let Some(alpn) = &self.alpn {
            cfg.alpn_protocols = alpn.clone();
        }

        Some(Arc::new(cfg))
    }

    /// Build a client.
    pub fn build(&self) -> Client {
        Client {
            opts: self.opts.clone(),
            tls: self.build_tls_config(),
        }
    }
}

/// Reusable WebSocket client created by [`ClientBuilder`].
#[derive(Debug, Clone)]
pub struct Client {
    opts: ConnectOptions,
    tls: Option<Arc<ClientConfig>>,
}

impl Client {
    /// Return a reference to the configured connection options.
    pub fn options(&self) -> &ConnectOptions {
        &self.opts
    }

    /// Establish a WebSocket connection using the configured TLS settings.
    pub async fn connect(&self, url: &str) -> Result<Connection> {
        crate::connection::connect_with_tls(url, self.opts.clone(), self.tls.clone()).await
    }
}

/// Builder that can create clients with disabled certificate verification.
pub struct DangerousClientBuilder {
    opts: ConnectOptions,
}

impl DangerousClientBuilder {
    /// Build a client that does not verify certificates (testing only).
    pub fn with_no_certificate_verification(self) -> Result<Client> {
        let config = crate::tls::TlsClientConfigBuilder::new_insecure()?.build();
        Ok(Client {
            opts: self.opts,
            tls: Some(Arc::new(config)),
        })
    }
}

/// Builder for creating a WebSocket server.
#[derive(Debug, Clone)]
pub struct ServerBuilder {
    addr: SocketAddr,
    opts: ServerOptions,
    tls: Option<ServerConfig>,
    alpn: Option<Vec<Vec<u8>>>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerBuilder {
    /// Create a new server builder bound to localhost on an ephemeral port.
    pub fn new() -> Self {
        Self {
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            opts: ServerOptions::default(),
            alpn: None,
            tls: None,
        }
    }

    /// Set the bind address.
    pub fn with_addr(mut self, addr: impl Into<SocketAddr>) -> Self {
        self.addr = addr.into();
        self
    }

    /// Replace the server options wholesale.
    pub fn with_options(mut self, opts: ServerOptions) -> Self {
        self.opts = opts;
        self
    }

    /// Return a reference to the configured server options.
    pub fn options(&self) -> &ServerOptions {
        &self.opts
    }

    /// Add a response header to the server handshake.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.opts.headers.push((name.into(), value.into()));
        self
    }

    /// Configure accepted WebSocket message, frame, and write-buffer limits.
    pub fn with_limits(mut self, limits: WebSocketLimits) -> Self {
        self.opts.limits = limits;
        self
    }

    /// Add a single subprotocol to the allowed set.
    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.opts.protocols.push(protocol.into());
        self
    }

    /// Configure TLS using a certificate chain and private key.
    pub fn with_certificate(
        mut self,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Self> {
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .map_err(Error::tls)?;
        self.tls = Some(config);
        Ok(self)
    }

    /// Provide an already-built rustls server configuration.
    pub fn with_rustls_config(mut self, config: ServerConfig) -> Self {
        self.tls = Some(config);
        self
    }

    /// Configure ALPN with the default WebSocket protocol identifiers.
    pub fn with_default_alpn(mut self) -> Self {
        self.alpn = Some(default_ws_alpn());
        self
    }

    /// Configure ALPN with custom protocol identifiers.
    pub fn with_alpn_protocols(mut self, alpn: Vec<Vec<u8>>) -> Self {
        self.alpn = Some(alpn);
        self
    }

    fn build_tls_config(&self) -> Option<ServerConfig> {
        let mut cfg = self.tls.clone()?;

        if let Some(alpn) = &self.alpn {
            cfg.alpn_protocols = alpn.clone();
        }

        Some(cfg)
    }

    /// Bind the listener and return a server instance.
    pub async fn build(&self) -> Result<Server> {
        crate::server::bind(self.addr, self.opts.clone(), self.build_tls_config()).await
    }
}
