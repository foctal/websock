//! Browser mux client.

use crate::{Limits, Session};
use websock_proto::{ConnectOptions, Result};

/// Reusable browser WebSocket mux client created by [`crate::ClientBuilder`].
#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) opts: ConnectOptions,
    pub(crate) limits: Limits,
}

impl Client {
    /// Return a reference to the configured connection options.
    pub fn options(&self) -> &ConnectOptions {
        &self.opts
    }

    /// Establish a browser WebSocket connection and create a mux [`Session`].
    pub async fn connect(&self, url: &str) -> Result<Session> {
        self.limits.validate()?;
        let conn = websock_wasm::connect(url, self.opts.clone()).await?;
        Session::new(conn, self.limits.clone())
    }
}
