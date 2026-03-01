use rustls::{ClientConfig, ServerConfig};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use websock_proto::{Result, default_ws_alpn};

use crate::session::Limits;
use crate::{Client, Server, bind};
use websock_mux_proto::SUBPROTOCOL;

/// Builder for a mux WebSocket client.
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    pub(crate) opts: websock_proto::ConnectOptions,
    pub(crate) tls: Option<ClientConfig>,
    pub(crate) alpn: Option<Vec<Vec<u8>>>,
    pub(crate) limits: Limits,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    /// Create a new client builder with default mux options.
    pub fn new() -> Self {
        let mut opts = websock_proto::ConnectOptions::default();
        opts.protocols.insert(0, SUBPROTOCOL.to_string());
        Self {
            opts,
            tls: None,
            alpn: None,
            limits: Limits::default(),
        }
    }

    /// Replace the builder options.
    ///
    /// The mux subprotocol is inserted if it is not already present.
    pub fn with_options(mut self, opts: websock_proto::ConnectOptions) -> Self {
        self.opts = opts;
        if !self.opts.protocols.iter().any(|p| p == SUBPROTOCOL) {
            self.opts.protocols.insert(0, SUBPROTOCOL.to_string());
        }
        self
    }

    /// Configure a custom rustls client config (for `wss://`).
    pub fn with_tls_config(mut self, tls: ClientConfig) -> Self {
        self.tls = Some(tls);
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

    /// Configure session limits.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Add a request header.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.opts.headers.push((name.into(), value.into()));
        self
    }

    /// Add a requested subprotocol.
    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.opts.protocols.push(protocol.into());
        self
    }

    fn build_tls_config(&self) -> Option<Arc<ClientConfig>> {
        let mut cfg = self.tls.clone()?;

        if let Some(alpn) = &self.alpn {
            cfg.alpn_protocols = alpn.clone();
        }

        Some(Arc::new(cfg))
    }

    /// Build a reusable client.
    pub fn build(&self) -> Client {
        Client {
            opts: self.opts.clone(),
            tls: self.build_tls_config(),
            limits: self.limits.clone(),
        }
    }
}

/// Builder for a mux WebSocket server.
#[derive(Debug, Clone)]
pub struct ServerBuilder {
    pub(crate) addr: SocketAddr,
    pub(crate) opts: websock_proto::ServerOptions,
    pub(crate) tls: Option<ServerConfig>,
    pub(crate) alpn: Option<Vec<Vec<u8>>>,
    pub(crate) limits: Limits,
}

impl ServerBuilder {
    /// Create a new server builder with default mux options.
    pub fn new() -> Self {
        let mut opts = websock_proto::ServerOptions::default();
        opts.protocols.push(SUBPROTOCOL.to_string());
        Self {
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            opts,
            tls: None,
            alpn: None,
            limits: Limits::default(),
        }
    }

    /// Set the bind address.
    pub fn with_addr(mut self, addr: impl Into<SocketAddr>) -> Self {
        self.addr = addr.into();
        self
    }

    /// Replace the server options.
    ///
    /// The mux subprotocol is inserted if it is not already present.
    pub fn with_options(mut self, opts: websock_proto::ServerOptions) -> Self {
        self.opts = opts;
        if !self.opts.protocols.iter().any(|p| p == SUBPROTOCOL) {
            self.opts.protocols.insert(0, SUBPROTOCOL.to_string());
        }
        self
    }

    /// Configure TLS for incoming connections (accept `wss://`).
    pub fn with_tls_config(mut self, tls: ServerConfig) -> Self {
        self.tls = Some(tls);
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

    /// Configure session limits.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Add a response header.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.opts.headers.push((name.into(), value.into()));
        self
    }

    /// Add an accepted subprotocol.
    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.opts.protocols.push(protocol.into());
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
        bind(
            self.addr,
            self.opts.clone(),
            self.build_tls_config(),
            self.limits.clone(),
        )
        .await
    }
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}
