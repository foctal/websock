//! WebAssembly transport implementation for browser-based WebSockets.

mod builder;
mod connection;

pub use builder::{Client, ClientBuilder, DangerousClientBuilder};
pub use connection::{Connection, connect};
pub mod stream;
