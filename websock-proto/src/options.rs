pub const ALPN_HTTP_1_1: &[u8] = b"http/1.1";

/// Resource limits applied by WebSocket transports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebSocketLimits {
    /// Maximum accepted WebSocket message size in bytes.
    pub max_message_size: usize,
    /// Maximum accepted WebSocket frame payload size in bytes.
    ///
    /// Browsers do not expose individual frames, so this limit is native-only.
    pub max_frame_size: usize,
    /// Maximum native transport write-buffer size in bytes.
    ///
    /// Browsers manage their own write buffers and ignore this value.
    pub max_write_buffer_size: usize,
}

impl Default for WebSocketLimits {
    fn default() -> Self {
        Self {
            max_message_size: 16 * 1024 * 1024,
            max_frame_size: 4 * 1024 * 1024,
            max_write_buffer_size: 16 * 1024 * 1024,
        }
    }
}

impl WebSocketLimits {
    /// Validate that all limits are non-zero and internally consistent.
    pub fn validate(&self) -> crate::Result<()> {
        if self.max_message_size == 0 {
            return Err(crate::Error::Protocol(
                "max_message_size must be greater than zero".into(),
            ));
        }
        if self.max_frame_size == 0 {
            return Err(crate::Error::Protocol(
                "max_frame_size must be greater than zero".into(),
            ));
        }
        if self.max_frame_size > self.max_message_size {
            return Err(crate::Error::Protocol(
                "max_frame_size must not exceed max_message_size".into(),
            ));
        }
        if self.max_write_buffer_size == 0 {
            return Err(crate::Error::Protocol(
                "max_write_buffer_size must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

/// Connection configuration shared by native and WebAssembly transports.
#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    /// Requested subprotocols (e.g. "json", "cbor").
    pub protocols: Vec<String>,

    /// Additional headers (native only). Browser WebSockets typically ignore these by design.
    pub headers: Vec<(String, String)>,

    /// Resource limits for the WebSocket transport.
    pub limits: WebSocketLimits,
}

/// Server configuration shared by native transports.
#[derive(Debug, Clone, Default)]
pub struct ServerOptions {
    /// Accepted subprotocols (e.g. "json", "cbor").
    pub protocols: Vec<String>,

    /// Additional response headers (native only).
    pub headers: Vec<(String, String)>,

    /// Resource limits for accepted WebSocket connections.
    pub limits: WebSocketLimits,
}

pub fn default_ws_alpn() -> Vec<Vec<u8>> {
    vec![ALPN_HTTP_1_1.to_vec()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_alpn_only_advertises_supported_http_version() {
        assert_eq!(default_ws_alpn(), vec![ALPN_HTTP_1_1.to_vec()]);
    }
}
