use bytes::Bytes;

/// WebSocket message representation used by websock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// UTF-8 text payload.
    Text(String),
    /// Binary payload.
    Binary(Bytes),
}

/// Close frame metadata associated with WebSocket close events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseFrame {
    /// Close status code.
    pub code: u16,
    /// Optional human-readable reason.
    pub reason: String,
}

impl CloseFrame {
    /// Construct a normal close frame with code 1000 and an empty reason.
    pub fn normal() -> Self {
        Self {
            code: 1000,
            reason: String::new(),
        }
    }
}
