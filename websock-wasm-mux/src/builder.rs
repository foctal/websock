//! Builder for browser mux clients.

use crate::{Client, Limits};
use websock_mux_proto::SUBPROTOCOL;
use websock_proto::{ConnectOptions, Result};

/// Builder for creating a browser WebSocket mux client.
///
/// This wraps `websock-wasm` and enforces the mux `SUBPROTOCOL`.
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    opts: ConnectOptions,
    limits: Limits,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    /// Create a new builder with default options.
    pub fn new() -> Self {
        let mut opts = ConnectOptions::default();
        opts.protocols.push(SUBPROTOCOL.to_string());
        Self {
            opts,
            limits: Limits::default(),
        }
    }

    /// Replace the builder options wholesale.
    ///
    /// Note: this will re-append the mux subprotocol if missing.
    pub fn with_options(mut self, mut opts: ConnectOptions) -> Self {
        if !opts.protocols.iter().any(|p| p == SUBPROTOCOL) {
            opts.protocols.push(SUBPROTOCOL.to_string());
        }
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

    /// Configure session limits.
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Build a reusable client.
    pub fn build(self) -> Client {
        Client {
            opts: self.opts,
            limits: self.limits,
        }
    }

    /// Build a client (no-op for browser cert roots).
    pub fn with_system_roots(self) -> Result<Client> {
        Ok(self.build())
    }
}
