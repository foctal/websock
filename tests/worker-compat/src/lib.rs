//! Compile-only check that the public facades and workers-rs share one WASM graph.
//! This does not claim that the browser connector is a Workers socket backend.
pub use websock::{Message, WebSocketConnection};
pub use websock_mux::ClientBuilder;
pub use worker::{HttpRequest, HttpResponse, WebSocket};
