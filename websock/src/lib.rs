//! WebSocket library for native and WebAssembly targets.
//!
//! ## Features
//! - Native (tokio-tungstenite) support on non-wasm targets
//! - Browser WebSocket support on wasm32 targets

pub use websock_proto::*;

#[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
#[path = "tungstenite.rs"]
mod websocket;

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
#[path = "wasm.rs"]
mod websocket;

pub use websocket::*;
