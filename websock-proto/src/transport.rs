//! Core transport trait contracts shared by native and WebAssembly backends.

use core::future::Future;
use core::pin::Pin;

use crate::{Message, Result};

/// Local boxed future used by transport traits.
pub type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Cross-platform WebSocket connection contract.
///
/// Native and WebAssembly implementations can provide this trait to expose
/// a shared async shape without changing their concrete public APIs.
pub trait WebSocketConnection {
    /// Send one message.
    fn send<'a>(&'a mut self, msg: Message) -> LocalBoxFuture<'a, Result<()>>;

    /// Receive one message.
    fn recv<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<Message>>;

    /// Close the connection gracefully.
    fn close<'a>(&'a mut self) -> LocalBoxFuture<'a, Result<()>>;
}
