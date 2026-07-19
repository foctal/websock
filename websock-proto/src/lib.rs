//! Protocol-level primitives shared across websock transports.
//!
//! This crate remains intentionally small and transport-agnostic so the public
//! API stays consistent across native and WebAssembly targets.
//! It also defines the core trait contracts shared by native and WASM transports.

#![warn(missing_docs)]

mod error;
mod message;
mod options;
mod transport;

pub use bytes::Bytes;
pub use error::{BoxError, Error, Result};
pub use message::{CloseFrame, Message};
pub use options::{ConnectOptions, ServerOptions, WebSocketLimits, default_ws_alpn};
pub use transport::{LocalBoxFuture, WebSocketConnection};
