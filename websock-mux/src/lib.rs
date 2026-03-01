//! Multiplexed WebSocket facade for native and WebAssembly targets.
//!
//! This crate selects the native Tokio Tungstenite transport on non-wasm
//! targets and the browser WebSocket transport on `wasm32`.

pub use websock_proto::*;

#[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
#[path = "tungstenite.rs"]
mod websocket;

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
#[path = "wasm.rs"]
mod websocket;

pub use websocket::*;
