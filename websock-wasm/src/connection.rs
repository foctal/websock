//! Browser WebSocket connection management.

use std::cell::RefCell;
use std::rc::Rc;
use websock_proto::Bytes;
use websock_proto::{CloseFrame, ConnectOptions, Error, Message, Result};

use futures_channel::{mpsc, oneshot};
use futures_util::StreamExt;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

/// Establish a browser WebSocket connection.
pub async fn connect(url: &str, opts: ConnectOptions) -> Result<Connection> {
    opts.limits.validate()?;
    let max_message_size = opts.limits.max_message_size;
    let ws = if opts.protocols.is_empty() {
        web_sys::WebSocket::new(url).map_err(js_err)?
    } else {
        let arr = js_sys::Array::new();
        for p in &opts.protocols {
            arr.push(&JsValue::from_str(p));
        }
        web_sys::WebSocket::new_with_str_sequence(url, &arr).map_err(js_err)?
    };

    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

    // Channel used to deliver messages to the consumer.
    let (tx, rx) = mpsc::channel::<Result<Message>>(64);

    // Handle the connection process.
    let (open_tx, open_rx) = oneshot::channel::<Result<()>>();
    let open_tx_cell: Rc<RefCell<Option<oneshot::Sender<Result<()>>>>> =
        Rc::new(RefCell::new(Some(open_tx)));

    let open_tx_cell_onopen = Rc::clone(&open_tx_cell);
    let wait_onopen = Closure::<dyn FnMut()>::new(move || {
        if let Some(tx) = open_tx_cell_onopen.borrow_mut().take() {
            let _ = tx.send(Ok(()));
        }
    });
    ws.set_onopen(Some(wait_onopen.as_ref().unchecked_ref()));

    let open_tx_cell_onerror = Rc::clone(&open_tx_cell);
    let wait_onerror = Closure::<dyn FnMut(web_sys::Event)>::new(move |_e: web_sys::Event| {
        if let Some(tx) = open_tx_cell_onerror.borrow_mut().take() {
            let _ = tx.send(Err(Error::Other("websocket error (before open)".into())));
        }
    });
    ws.set_onerror(Some(wait_onerror.as_ref().unchecked_ref()));

    let open_tx_cell_onclose = Rc::clone(&open_tx_cell);
    let wait_onclose =
        Closure::<dyn FnMut(web_sys::CloseEvent)>::new(move |_e: web_sys::CloseEvent| {
            if let Some(tx) = open_tx_cell_onclose.borrow_mut().take() {
                let _ = tx.send(Err(Error::Closed));
            }
        });
    ws.set_onclose(Some(wait_onclose.as_ref().unchecked_ref()));

    // Wait until the connection is opened or fails.
    let open_res = open_rx.await;

    // Always unset the connection process handlers.
    ws.set_onopen(None);
    ws.set_onerror(None);
    ws.set_onclose(None);

    // Drop closures AFTER unsetting.
    drop(wait_onopen);
    drop(wait_onerror);
    drop(wait_onclose);

    match open_res {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(Error::Other("onopen waiter dropped".into())),
    }

    // Set up message/error/close handlers.
    let mut tx_msg = tx.clone();
    let ws_onmessage = ws.clone();
    let onmessage =
        Closure::<dyn FnMut(web_sys::MessageEvent)>::new(move |e: web_sys::MessageEvent| {
            let data = e.data();

            if let Some(s) = data.as_string() {
                if s.len() > max_message_size {
                    let _ = tx_msg.try_send(Err(Error::Protocol(
                        "websocket message exceeds max_message_size".into(),
                    )));
                    tx_msg.close_channel();
                    let _ = ws_onmessage.close();
                    return;
                }
                if tx_msg.try_send(Ok(Message::Text(s))).is_err() {
                    tx_msg.close_channel();
                    let _ = ws_onmessage.close();
                }
                return;
            }

            if data.is_instance_of::<js_sys::ArrayBuffer>() {
                let ab: js_sys::ArrayBuffer = data.unchecked_into();
                let u8arr = js_sys::Uint8Array::new(&ab);
                if u8arr.length() as usize > max_message_size {
                    let _ = tx_msg.try_send(Err(Error::Protocol(
                        "websocket message exceeds max_message_size".into(),
                    )));
                    tx_msg.close_channel();
                    let _ = ws_onmessage.close();
                    return;
                }
                let mut buf = vec![0u8; u8arr.length() as usize];
                u8arr.copy_to(&mut buf);
                if tx_msg
                    .try_send(Ok(Message::Binary(Bytes::from(buf))))
                    .is_err()
                {
                    tx_msg.close_channel();
                    let _ = ws_onmessage.close();
                }
                return;
            }

            let _ = tx_msg.try_send(Err(Error::Protocol("unsupported message type".into())));
            tx_msg.close_channel();
            let _ = ws_onmessage.close();
        });
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

    let mut tx_err = tx.clone();
    let onerror = Closure::<dyn FnMut(web_sys::Event)>::new(move |_e: web_sys::Event| {
        if tx_err
            .try_send(Err(Error::Other("websocket error".into())))
            .is_err()
        {
            tx_err.close_channel();
        }
    });
    ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));

    let close_frame = Rc::new(RefCell::new(None));
    let close_frame_handler = Rc::clone(&close_frame);
    let mut tx_close = tx;
    let onclose = Closure::<dyn FnMut(web_sys::CloseEvent)>::new(move |e: web_sys::CloseEvent| {
        *close_frame_handler.borrow_mut() = Some(CloseFrame {
            code: e.code(),
            reason: e.reason(),
        });
        let _ = tx_close.try_send(Err(Error::Closed));
        tx_close.close_channel();
    });
    ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));
    let negotiated_subprotocol = ws.protocol();

    Ok(Connection {
        ws: Rc::new(ws),
        rx: Some(rx),
        max_write_buffer_size: opts.limits.max_write_buffer_size,
        negotiated_subprotocol,
        close_frame,
        _onmessage: Some(onmessage),
        _onerror: Some(onerror),
        _onclose: Some(onclose),
    })
}

/// WebSocket connection wrapper for browser WebSockets.
pub struct Connection {
    pub(crate) ws: Rc<web_sys::WebSocket>,
    pub(crate) rx: Option<mpsc::Receiver<Result<Message>>>,
    pub(crate) max_write_buffer_size: usize,
    pub(crate) negotiated_subprotocol: String,
    pub(crate) close_frame: Rc<RefCell<Option<CloseFrame>>>,

    pub(crate) _onmessage: Option<Closure<dyn FnMut(web_sys::MessageEvent)>>,
    pub(crate) _onerror: Option<Closure<dyn FnMut(web_sys::Event)>>,
    pub(crate) _onclose: Option<Closure<dyn FnMut(web_sys::CloseEvent)>>,
}

impl Connection {
    /// Send a text or binary message.
    pub async fn send(&mut self, msg: Message) -> Result<()> {
        match msg {
            Message::Text(s) => {
                check_send_capacity(&self.ws, s.len(), self.max_write_buffer_size)?;
                self.ws.send_with_str(&s).map_err(js_err)?;
            }
            Message::Binary(b) => {
                check_send_capacity(&self.ws, b.len(), self.max_write_buffer_size)?;
                self.ws.send_with_u8_array(b.as_ref()).map_err(js_err)?;
            }
        }
        Ok(())
    }

    /// Receive the next text or binary message.
    pub async fn recv(&mut self) -> Result<Message> {
        let rx = self.rx.as_mut().ok_or(Error::Closed)?;
        rx.next().await.ok_or(Error::Closed)?
    }

    /// Close the WebSocket connection and wait for the browser close event.
    pub async fn close(&mut self) -> Result<()> {
        if self.ws.ready_state() == web_sys::WebSocket::CLOSED {
            self.rx = None;
            return Ok(());
        }
        if self.ws.ready_state() != web_sys::WebSocket::CLOSING {
            self.ws.close().map_err(js_err)?;
        }

        let result = loop {
            let next = match self.rx.as_mut() {
                Some(rx) => rx.next().await,
                None => return Ok(()),
            };
            match next {
                Some(Ok(_)) => continue,
                Some(Err(Error::Closed)) | None => break Ok(()),
                Some(Err(error)) => break Err(error),
            }
        };
        self.rx = None;
        result
    }

    /// Return the WebSocket subprotocol selected by the server, if any.
    pub fn negotiated_subprotocol(&self) -> Option<&str> {
        (!self.negotiated_subprotocol.is_empty()).then_some(self.negotiated_subprotocol.as_str())
    }

    /// Return the most recently received close-frame metadata, if any.
    pub fn close_frame(&self) -> Option<CloseFrame> {
        self.close_frame.borrow().clone()
    }
}

impl websock_proto::WebSocketConnection for Connection {
    fn send<'a>(&'a mut self, msg: Message) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { Connection::send(self, msg).await })
    }

    fn recv<'a>(&'a mut self) -> websock_proto::LocalBoxFuture<'a, Result<Message>> {
        Box::pin(async move { Connection::recv(self).await })
    }

    fn close<'a>(&'a mut self) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { Connection::close(self).await })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // If there are other Rc references, do not close.
        if Rc::strong_count(&self.ws) != 1 {
            return;
        }

        if self._onmessage.is_some() {
            self.ws.set_onmessage(None);
        }
        if self._onerror.is_some() {
            self.ws.set_onerror(None);
        }
        if self._onclose.is_some() {
            self.ws.set_onclose(None);
        }
        self.ws.set_onopen(None);

        let _ = self.ws.close();
    }
}

/// Convert a JavaScript error into the shared error type.
pub(crate) fn js_err(e: JsValue) -> Error {
    Error::Other(format!("{e:?}"))
}

pub(crate) fn check_send_capacity(
    ws: &web_sys::WebSocket,
    message_len: usize,
    max_write_buffer_size: usize,
) -> Result<()> {
    let buffered = usize::try_from(ws.buffered_amount()).unwrap_or(usize::MAX);
    if buffered.saturating_add(message_len) > max_write_buffer_size {
        return Err(Error::Other("websocket write buffer limit exceeded".into()));
    }
    Ok(())
}
