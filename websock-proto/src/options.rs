pub const ALPN_HTTP_1_1: &[u8] = b"http/1.1";
pub const ALPN_H2: &[u8] = b"h2";

/// Connection configuration shared by native and WebAssembly transports.
#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    /// Requested subprotocols (e.g. "json", "cbor").
    pub protocols: Vec<String>,

    /// Additional headers (native only). Browser WebSockets typically ignore these by design.
    pub headers: Vec<(String, String)>,
}

/// Server configuration shared by native transports.
#[derive(Debug, Clone, Default)]
pub struct ServerOptions {
    /// Accepted subprotocols (e.g. "json", "cbor").
    pub protocols: Vec<String>,

    /// Additional response headers (native only).
    pub headers: Vec<(String, String)>,
}

pub fn default_ws_alpn() -> Vec<Vec<u8>> {
    vec![ALPN_HTTP_1_1.to_vec(), ALPN_H2.to_vec()]
}
