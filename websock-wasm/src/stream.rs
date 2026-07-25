//! Sink/Stream split helpers for browser WebSocket connections.

use crate::Connection;
use crate::connection::{check_send_capacity, js_err};
use futures_channel::mpsc;
use futures_core::Stream;
use futures_sink::Sink;
use std::cell::Cell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use wasm_bindgen::prelude::Closure;
use websock_proto::{Error, Message, Result};

/// Sink wrapper for sending messages through a browser WebSocket.
pub struct ConnectionSink {
    ws: Rc<web_sys::WebSocket>,
    closed: bool,
    ref_count: Rc<Cell<usize>>,
    max_write_buffer_size: usize,
}

impl ConnectionSink {
    /// Create a sink backed by the provided WebSocket instance.
    fn new(
        ws: Rc<web_sys::WebSocket>,
        ref_count: Rc<Cell<usize>>,
        max_write_buffer_size: usize,
    ) -> Self {
        Self {
            ws,
            closed: false,
            ref_count,
            max_write_buffer_size,
        }
    }
}

impl Sink<Message> for ConnectionSink {
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Err(Error::Closed));
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> std::result::Result<(), Self::Error> {
        let this = self.get_mut();
        if this.closed {
            return Err(Error::Closed);
        }
        match item {
            Message::Text(s) => {
                check_send_capacity(&this.ws, s.len(), this.max_write_buffer_size)?;
                this.ws.send_with_str(&s).map_err(js_err)?;
            }
            Message::Binary(b) => {
                check_send_capacity(&this.ws, b.len(), this.max_write_buffer_size)?;
                this.ws.send_with_u8_array(b.as_ref()).map_err(js_err)?;
            }
        }
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        if !this.closed {
            let _ = this.ws.close();
            this.closed = true;
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for ConnectionSink {
    fn drop(&mut self) {
        let remaining = self.ref_count.get().saturating_sub(1);
        self.ref_count.set(remaining);
        if remaining == 0 && !self.closed {
            let _ = self.ws.close();
            self.closed = true;
        }
    }
}

/// Stream wrapper for receiving messages from a browser WebSocket.
pub struct ConnectionStream {
    ws: Rc<web_sys::WebSocket>,
    rx: mpsc::Receiver<Result<Message>>,
    terminated: bool,
    ref_count: Rc<Cell<usize>>,
    _onmessage: Closure<dyn FnMut(web_sys::MessageEvent)>,
    _onerror: Closure<dyn FnMut(web_sys::Event)>,
    _onclose: Closure<dyn FnMut(web_sys::CloseEvent)>,
}

impl ConnectionStream {
    /// Create a stream backed by the provided WebSocket and receiver.
    fn new(
        ws: Rc<web_sys::WebSocket>,
        rx: mpsc::Receiver<Result<Message>>,
        ref_count: Rc<Cell<usize>>,
        onmessage: Closure<dyn FnMut(web_sys::MessageEvent)>,
        onerror: Closure<dyn FnMut(web_sys::Event)>,
        onclose: Closure<dyn FnMut(web_sys::CloseEvent)>,
    ) -> Self {
        Self {
            ws,
            rx,
            terminated: false,
            ref_count,
            _onmessage: onmessage,
            _onerror: onerror,
            _onclose: onclose,
        }
    }
}

impl Stream for ConnectionStream {
    type Item = Result<Message>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }

        match Pin::new(&mut this.rx).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.terminated = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
        }
    }
}

impl Drop for ConnectionStream {
    fn drop(&mut self) {
        self.ws.set_onmessage(None);
        self.ws.set_onerror(None);
        self.ws.set_onclose(None);
        self.ws.set_onopen(None);
        let remaining = self.ref_count.get().saturating_sub(1);
        self.ref_count.set(remaining);
        if remaining == 0 {
            let _ = self.ws.close();
        }
    }
}

/// Split a connection into sink and stream halves.
pub fn split(mut conn: Connection) -> (ConnectionSink, ConnectionStream) {
    let rx = conn.rx.take().expect("connection already split");

    let onmessage = conn._onmessage.take().expect("missing onmessage");
    let onerror = conn._onerror.take().expect("missing onerror");
    let onclose = conn._onclose.take().expect("missing onclose");

    let ws_for_sink = Rc::clone(&conn.ws);
    let ws_for_stream = Rc::clone(&conn.ws);
    let max_write_buffer_size = conn.max_write_buffer_size;
    let ref_count = Rc::new(Cell::new(2));

    (
        ConnectionSink::new(ws_for_sink, Rc::clone(&ref_count), max_write_buffer_size),
        ConnectionStream::new(ws_for_stream, rx, ref_count, onmessage, onerror, onclose),
    )
}
