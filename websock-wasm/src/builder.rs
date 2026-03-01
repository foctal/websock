//! Builders for browser WebSocket clients.

use websock_proto::{ConnectOptions, Error, Result};

use crate::Connection;
use crate::connection::connect;

/// Builder for creating a WebSocket client.
///
/// The resulting client can be reused for multiple `connect()` calls.
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    opts: ConnectOptions,
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

    /// Build a reusable client.
    pub fn build(self) -> Client {
        Client { opts: self.opts }
    }

    /// Build a client using system roots (no-op in the browser).
    pub fn with_system_roots(self) -> Result<Client> {
        Ok(self.build())
    }

    /// Attempt to configure custom certificates (not supported in the browser).
    pub fn with_server_certificates<I>(self, _chain: I) -> Result<Client>
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        Err(Error::Unsupported(
            "custom certificates are not supported in browser wasm".into(),
        ))
    }

    /// Enter the "dangerous" builder that can disable certificate verification.
    pub fn dangerous(self) -> DangerousClientBuilder {
        DangerousClientBuilder { opts: self.opts }
    }
}

/// Reusable WebSocket client created by [`ClientBuilder`].
#[derive(Debug, Clone)]
pub struct Client {
    opts: ConnectOptions,
}

impl Client {
    /// Return a reference to the configured connection options.
    pub fn options(&self) -> &ConnectOptions {
        &self.opts
    }

    /// Establish a browser WebSocket connection.
    pub async fn connect(&self, url: &str) -> Result<Connection> {
        connect(url, self.opts.clone()).await
    }
}

/// Builder that can attempt to disable certificate verification.
pub struct DangerousClientBuilder {
    #[allow(dead_code)]
    opts: ConnectOptions,
}

impl DangerousClientBuilder {
    /// Return an unsupported error, since browsers cannot disable verification.
    pub fn with_no_certificate_verification(self) -> Result<Client> {
        Err(Error::Unsupported(
            "certificate verification cannot be disabled in browser wasm".into(),
        ))
    }
}
