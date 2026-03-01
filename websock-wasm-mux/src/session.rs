use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_channel::mpsc;
use futures_io::{AsyncRead as FuturesAsyncRead, AsyncWrite as FuturesAsyncWrite};
use futures_util::lock::Mutex;
use futures_util::stream::Stream;
use futures_util::task::AtomicWaker;
use futures_util::{FutureExt, StreamExt};
use wasm_bindgen_futures::spawn_local;
use websock_proto::{Error, Message, Result};

use websock_mux_proto::{Frame, StreamDir, StreamId};

const MAX_WRITE_CHUNK: usize = 16 * 1024;

/// Session limits to prevent unbounded buffering / DoS.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Maximum size of a single WebSocket binary message accepted by the inbound loop.
    pub max_ws_message_size: usize,
    /// Maximum `Stream` frame payload size.
    pub max_stream_data_per_frame: usize,
    /// Maximum number of concurrently open receive streams.
    pub max_open_streams: usize,
    /// Per-stream receive event queue length.
    pub recv_event_queue_len: usize,
    /// Session outbound queue length.
    pub outbound_queue_len: usize,
    /// Maximum number of mux frames packed into one WebSocket binary message.
    pub max_batch_frames: usize,
    /// Maximum encoded bytes packed into one WebSocket binary message.
    pub max_batch_bytes: usize,
    /// Initial per-stream flow-control window in bytes.
    pub initial_stream_window: usize,
    /// Window update threshold in bytes.
    pub stream_window_update_threshold: usize,
    /// Queue length for accepting inbound uni streams.
    pub accept_uni_queue_len: usize,
    /// Queue length for accepting inbound bi streams.
    pub accept_bi_queue_len: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_ws_message_size: 1 * 1024 * 1024,
            max_stream_data_per_frame: 256 * 1024,
            max_open_streams: 1024,
            recv_event_queue_len: 128,
            outbound_queue_len: 256,
            max_batch_frames: 64,
            max_batch_bytes: 256 * 1024,
            initial_stream_window: 512 * 1024,
            stream_window_update_threshold: 256 * 1024,
            accept_uni_queue_len: 128,
            accept_bi_queue_len: 128,
        }
    }
}

#[derive(Clone)]
pub struct Session {
    inner: Rc<SessionInner>,
    accept_uni: Rc<Mutex<mpsc::Receiver<RecvStream>>>,
    accept_bi: Rc<Mutex<mpsc::Receiver<(SendStream, RecvStream)>>>,
}

impl Session {
    pub(crate) fn new(conn: websock_wasm::Connection, limits: Limits) -> Self {
        let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundCmd>(limits.outbound_queue_len);
        let (accept_uni_tx, accept_uni_rx) =
            mpsc::channel::<RecvStream>(limits.accept_uni_queue_len);
        let (accept_bi_tx, accept_bi_rx) =
            mpsc::channel::<(SendStream, RecvStream)>(limits.accept_bi_queue_len);

        let inner = Rc::new(SessionInner::new(
            limits,
            outbound_tx,
            accept_uni_tx,
            accept_bi_tx,
        ));

        let session = Self {
            inner: inner.clone(),
            accept_uni: Rc::new(Mutex::new(accept_uni_rx)),
            accept_bi: Rc::new(Mutex::new(accept_bi_rx)),
        };

        inner.spawn_task(conn, outbound_rx);
        session
    }

    pub fn open_uni(&self) -> Result<SendStream> {
        let id = self.inner.next_stream_id(StreamDir::Uni)?;
        let flow = self
            .inner
            .register_send_flow(id, self.inner.limits.initial_stream_window as u64);
        self.inner.send_frame(Frame::OpenUni { id })?;
        Ok(SendStream::new(id, self.inner.clone(), flow))
    }

    pub fn open_bi(&self) -> Result<(SendStream, RecvStream)> {
        let id = self.inner.next_stream_id(StreamDir::Bi)?;
        let flow = self
            .inner
            .register_send_flow(id, self.inner.limits.initial_stream_window as u64);
        let recv = self.inner.clone().register_recv_stream(id);
        self.inner.send_frame(Frame::OpenBi { id })?;
        self.inner.send_frame(Frame::MaxStreamData {
            id,
            max: self.inner.limits.initial_stream_window as u64,
        })?;
        Ok((SendStream::new(id, self.inner.clone(), flow), recv))
    }

    pub async fn accept_uni(&self) -> Result<RecvStream> {
        let mut rx = self.accept_uni.lock().await;
        rx.next().await.ok_or(Error::Closed)
    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream)> {
        let mut rx = self.accept_bi.lock().await;
        rx.next().await.ok_or(Error::Closed)
    }
}

struct SendFlowState {
    max_data: AtomicU64,
    sent_data: AtomicU64,
    waker: AtomicWaker,
}

impl SendFlowState {
    fn new(initial_max: u64) -> Self {
        Self {
            max_data: AtomicU64::new(initial_max),
            sent_data: AtomicU64::new(0),
            waker: AtomicWaker::new(),
        }
    }

    fn try_reserve(&self, requested: usize) -> usize {
        if requested == 0 {
            return 0;
        }
        let requested_u64 = requested as u64;
        loop {
            let sent = self.sent_data.load(Ordering::Acquire);
            let max = self.max_data.load(Ordering::Acquire);
            if max <= sent {
                return 0;
            }
            let available = max - sent;
            let grant = available.min(requested_u64);
            if self
                .sent_data
                .compare_exchange(sent, sent + grant, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return grant as usize;
            }
        }
    }

    fn release(&self, n: usize) {
        if n == 0 {
            return;
        }
        self.sent_data.fetch_sub(n as u64, Ordering::AcqRel);
    }

    fn update_max(&self, max: u64) {
        let mut current = self.max_data.load(Ordering::Acquire);
        while max > current {
            match self
                .max_data
                .compare_exchange(current, max, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    self.waker.wake();
                    return;
                }
                Err(v) => current = v,
            }
        }
    }
}

pub struct SendStream {
    id: StreamId,
    session: Rc<SessionInner>,
    finished: Rc<AtomicBool>,
    flow: Rc<SendFlowState>,
    outbound: mpsc::Sender<OutboundCmd>,
    write_in_flight: Option<usize>,
    close_in_flight: bool,
}

impl SendStream {
    fn new(id: StreamId, session: Rc<SessionInner>, flow: Rc<SendFlowState>) -> Self {
        let outbound = session.outbound_tx.borrow().clone();
        Self {
            id,
            session,
            finished: Rc::new(AtomicBool::new(false)),
            flow,
            outbound,
            write_in_flight: None,
            close_in_flight: false,
        }
    }

    pub fn write(&self, data: &[u8]) -> Result<()> {
        self.write_buf(Bytes::copy_from_slice(data))
    }

    pub fn write_buf(&self, data: Bytes) -> Result<()> {
        if self.finished.load(Ordering::SeqCst) || self.session.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        let mut offset = 0usize;
        while offset < data.len() {
            let wanted = (data.len() - offset)
                .min(MAX_WRITE_CHUNK)
                .min(self.session.limits.max_stream_data_per_frame);
            if wanted == 0 {
                return Err(Error::Protocol("stream frame payload limit is zero".into()));
            }
            let grant = self.flow.try_reserve(wanted);
            if grant == 0 {
                return Err(Error::Other("flow control blocked".into()));
            }
            let chunk = data.slice(offset..offset + grant);
            if let Err(err) = self.session.send_frame(Frame::Stream {
                id: self.id,
                data: chunk,
                fin: false,
            }) {
                self.flow.release(grant);
                return Err(err);
            }
            offset += grant;
        }
        Ok(())
    }

    pub fn write_all(&self, data: &[u8]) -> Result<()> {
        self.write(data)
    }

    pub fn finish(&self) -> Result<()> {
        if self
            .finished
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.session.send_frame(Frame::Stream {
                id: self.id,
                data: Bytes::new(),
                fin: true,
            })?;
            self.session.remove_send_flow(self.id);
        }
        Ok(())
    }

    pub fn reset(&self, code: u64) -> Result<()> {
        self.finished.store(true, Ordering::SeqCst);
        self.session.remove_send_flow(self.id);
        self.session
            .send_frame(Frame::ResetStream { id: self.id, code })
    }

    pub fn closed(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }
}

impl Clone for SendStream {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            session: self.session.clone(),
            finished: self.finished.clone(),
            flow: self.flow.clone(),
            outbound: self.outbound.clone(),
            write_in_flight: None,
            close_in_flight: false,
        }
    }
}

impl FuturesAsyncWrite for SendStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.finished.load(Ordering::SeqCst) || this.session.closed.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io_closed()));
        }

        if this.write_in_flight.is_none() {
            this.flow.waker.register(cx.waker());
            let wanted = buf
                .len()
                .min(MAX_WRITE_CHUNK)
                .min(this.session.limits.max_stream_data_per_frame);
            if wanted == 0 {
                return Poll::Ready(Err(io_invalid_input("stream frame payload limit is zero")));
            }
            let chunk_len = this.flow.try_reserve(wanted);
            if chunk_len == 0 {
                return Poll::Pending;
            }

            match this.outbound.poll_ready(cx) {
                Poll::Pending => {
                    this.flow.release(chunk_len);
                    return Poll::Pending;
                }
                Poll::Ready(Err(_)) => {
                    this.flow.release(chunk_len);
                    return Poll::Ready(Err(io_closed()));
                }
                Poll::Ready(Ok(())) => {}
            }

            let frame = Frame::Stream {
                id: this.id,
                data: Bytes::copy_from_slice(&buf[..chunk_len]),
                fin: false,
            };
            if this.outbound.start_send(OutboundCmd::Frame(frame)).is_err() {
                this.flow.release(chunk_len);
                return Poll::Ready(Err(io_closed()));
            }
            this.write_in_flight = Some(chunk_len);
        }

        match this.outbound.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => {
                this.write_in_flight = None;
                Poll::Ready(Err(io_closed()))
            }
            Poll::Ready(Ok(())) => {
                let written = this.write_in_flight.take().unwrap_or(0);
                Poll::Ready(Ok(written))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match this.outbound.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => Poll::Ready(Err(io_closed())),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        if !this.finished.load(Ordering::SeqCst) && !this.close_in_flight {
            match this.outbound.poll_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Ready(Err(io_closed())),
                Poll::Ready(Ok(())) => {}
            }

            let frame = Frame::Stream {
                id: this.id,
                data: Bytes::new(),
                fin: true,
            };
            if this.outbound.start_send(OutboundCmd::Frame(frame)).is_err() {
                return Poll::Ready(Err(io_closed()));
            }
            this.finished.store(true, Ordering::SeqCst);
            this.close_in_flight = true;
            this.session.remove_send_flow(this.id);
        }

        match this.outbound.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => Poll::Ready(Err(io_closed())),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        }
    }
}

impl Drop for SendStream {
    fn drop(&mut self) {
        self.session.remove_send_flow(self.id);
        if !self.finished.load(Ordering::SeqCst) {
            let _ = self.session.try_send_frame(Frame::ResetStream {
                id: self.id,
                code: 0,
            });
        }
    }
}

#[derive(Debug)]
struct RecvEvent {
    data: Bytes,
    fin: bool,
}

pub struct RecvStream {
    id: StreamId,
    session: Rc<SessionInner>,
    receiver: mpsc::Receiver<RecvEvent>,
    finished: bool,
    pending: Bytes,
    consumed: u64,
    granted: u64,
    initial_window: u64,
    update_threshold: u64,
}

impl RecvStream {
    fn new(
        id: StreamId,
        session: Rc<SessionInner>,
        receiver: mpsc::Receiver<RecvEvent>,
        initial_window: u64,
        update_threshold: u64,
    ) -> Self {
        Self {
            id,
            session,
            receiver,
            finished: false,
            pending: Bytes::new(),
            consumed: 0,
            granted: initial_window,
            initial_window,
            update_threshold,
        }
    }

    fn on_bytes_consumed(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        self.consumed = self.consumed.saturating_add(n as u64);
        let target = self.consumed.saturating_add(self.initial_window);
        if target <= self.granted {
            return;
        }
        if target - self.granted < self.update_threshold {
            return;
        }
        if self
            .session
            .try_send_frame(Frame::MaxStreamData {
                id: self.id,
                max: target,
            })
            .is_ok()
        {
            self.granted = target;
        }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>> {
        if self.finished {
            return Ok(None);
        }
        if self.pending.is_empty() {
            if let Some(chunk) = self.read_chunk_internal().await? {
                self.pending = chunk;
            } else {
                return Ok(None);
            }
        }
        let amt = buf.len().min(self.pending.len());
        buf[..amt].copy_from_slice(&self.pending[..amt]);
        self.pending = self.pending.slice(amt..);
        self.on_bytes_consumed(amt);
        Ok(Some(amt))
    }

    pub async fn read_buf<B: BufMut>(&mut self, buf: &mut B) -> Result<Option<usize>> {
        if self.finished {
            return Ok(None);
        }
        if buf.remaining_mut() == 0 {
            return Ok(Some(0));
        }
        if self.pending.is_empty() {
            if let Some(chunk) = self.read_chunk_internal().await? {
                self.pending = chunk;
            } else {
                return Ok(None);
            }
        }

        let amt = self.pending.len().min(buf.remaining_mut());
        buf.put_slice(&self.pending[..amt]);
        self.pending = self.pending.slice(amt..);
        self.on_bytes_consumed(amt);
        Ok(Some(amt))
    }

    async fn read_chunk_internal(&mut self) -> Result<Option<Bytes>> {
        match self.receiver.next().await {
            Some(event) => {
                if event.fin {
                    self.finished = true;
                }
                if event.data.is_empty() && event.fin {
                    Ok(None)
                } else {
                    Ok(Some(event.data))
                }
            }
            None => {
                self.finished = true;
                Ok(None)
            }
        }
    }

    pub async fn read_chunk(&mut self, max: usize) -> Result<Option<Bytes>> {
        match self.receiver.next().await {
            Some(mut event) => {
                if event.fin {
                    self.finished = true;
                }
                if event.data.is_empty() && event.fin {
                    Ok(None)
                } else if event.data.len() > max {
                    let chunk = event.data.split_to(max);
                    self.pending = event.data;
                    self.on_bytes_consumed(chunk.len());
                    Ok(Some(chunk))
                } else {
                    self.on_bytes_consumed(event.data.len());
                    Ok(Some(event.data))
                }
            }
            None => {
                self.finished = true;
                Ok(None)
            }
        }
    }

    pub fn stop(&self, code: u64) -> Result<()> {
        self.session
            .send_frame(Frame::StopSending { id: self.id, code })
    }

    pub fn closed(&self) -> bool {
        self.finished
    }
}

impl Drop for RecvStream {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.session.try_send_frame(Frame::StopSending {
                id: self.id,
                code: 0,
            });
        }
    }
}

impl FuturesAsyncRead for RecvStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        if !this.pending.is_empty() {
            let amt = this.pending.len().min(buf.len());
            buf[..amt].copy_from_slice(&this.pending[..amt]);
            this.pending = this.pending.slice(amt..);
            this.on_bytes_consumed(amt);
            return Poll::Ready(Ok(amt));
        }

        if this.finished {
            return Poll::Ready(Ok(0));
        }

        loop {
            match Pin::new(&mut this.receiver).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.finished = true;
                    return Poll::Ready(Ok(0));
                }
                Poll::Ready(Some(event)) => {
                    if event.fin {
                        this.finished = true;
                    }

                    if event.data.is_empty() {
                        if event.fin {
                            return Poll::Ready(Ok(0));
                        }
                        continue;
                    }

                    let amt = event.data.len().min(buf.len());
                    buf[..amt].copy_from_slice(&event.data[..amt]);
                    if amt < event.data.len() {
                        this.pending = event.data.slice(amt..);
                    }
                    this.on_bytes_consumed(amt);
                    return Poll::Ready(Ok(amt));
                }
            }
        }
    }
}

enum OutboundCmd {
    Frame(Frame),
}

struct SessionInner {
    limits: Limits,
    outbound_tx: RefCell<mpsc::Sender<OutboundCmd>>,
    accept_uni_tx: Mutex<Option<mpsc::Sender<RecvStream>>>,
    accept_bi_tx: Mutex<Option<mpsc::Sender<(SendStream, RecvStream)>>>,
    streams: RefCell<HashMap<StreamId, mpsc::Sender<RecvEvent>>>,
    send_flows: RefCell<HashMap<StreamId, Rc<SendFlowState>>>,
    next_uni: AtomicU64,
    next_bi: AtomicU64,
    closed: AtomicBool,
}

impl SessionInner {
    fn new(
        limits: Limits,
        outbound_tx: mpsc::Sender<OutboundCmd>,
        accept_uni_tx: mpsc::Sender<RecvStream>,
        accept_bi_tx: mpsc::Sender<(SendStream, RecvStream)>,
    ) -> Self {
        Self {
            limits,
            outbound_tx: RefCell::new(outbound_tx),
            accept_uni_tx: Mutex::new(Some(accept_uni_tx)),
            accept_bi_tx: Mutex::new(Some(accept_bi_tx)),
            streams: RefCell::new(HashMap::new()),
            send_flows: RefCell::new(HashMap::new()),
            next_uni: AtomicU64::new(0),
            next_bi: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }
    }

    fn spawn_task(
        self: Rc<Self>,
        mut conn: websock_wasm::Connection,
        mut outbound_rx: mpsc::Receiver<OutboundCmd>,
    ) {
        let inner = self.clone();
        spawn_local(async move {
            loop {
                futures_util::select! {
                    msg = conn.recv().fuse() => {
                        match msg {
                            Ok(Message::Binary(data)) => {
                                if data.len() > inner.limits.max_ws_message_size {
                                    let _ = inner.protocol_error(2, "ws message too large").await;
                                    break;
                                }
                                let mut cursor = &data[..];
                                let mut frame_error = false;
                                while cursor.has_remaining() {
                                    let frame = match Frame::decode(&mut cursor) {
                                        Ok(f) => f,
                                        Err(_) => {
                                            let _ = inner.protocol_error(1, "invalid frame").await;
                                            frame_error = true;
                                            break;
                                        }
                                    };
                                    if inner.handle_frame(frame).await.is_err() {
                                        frame_error = true;
                                        break;
                                    }
                                }
                                if frame_error {
                                    break;
                                }
                            }
                            Ok(Message::Text(_)) => {
                                let _ = inner.protocol_error(1, "text message not supported").await;
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    out = outbound_rx.next().fuse() => {
                        match out {
                            Some(OutboundCmd::Frame(frame)) => {
                                let mut batch = BytesMut::new();
                                let mut batch_frames = 0usize;
                                let max_bytes = inner
                                    .limits
                                    .max_batch_bytes
                                    .min(inner.limits.max_ws_message_size);

                                let encoded = frame.encode().freeze();
                                batch.extend_from_slice(&encoded);
                                batch_frames += 1;

                                loop {
                                    if batch_frames >= inner.limits.max_batch_frames || batch.len() >= max_bytes {
                                        break;
                                    }
                                    match outbound_rx.try_recv() {
                                        Ok(OutboundCmd::Frame(next_frame)) => {
                                            let next = next_frame.encode().freeze();
                                            if !batch.is_empty() && batch.len() + next.len() > max_bytes {
                                                break;
                                            }
                                            batch.extend_from_slice(&next);
                                            batch_frames += 1;
                                        }
                                        Err(_) => break,
                                    }
                                }
                                if conn.send(Message::Binary(batch.freeze())).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }

            inner.close_all().await;
            let _ = conn.close().await;
        });
    }

    fn next_stream_id(&self, dir: StreamDir) -> Result<StreamId> {
        let is_server = false; // browser is always client
        let n = match dir {
            StreamDir::Uni => self.next_uni.fetch_add(1, Ordering::SeqCst),
            StreamDir::Bi => self.next_bi.fetch_add(1, Ordering::SeqCst),
        };
        StreamId::new(n, is_server, dir)
            .map_err(|e| Error::Protocol(format!("stream id overflow: {}", e)))
    }

    async fn handle_frame(self: &Rc<Self>, frame: Frame) -> Result<()> {
        match frame {
            Frame::OpenUni { id } => {
                if id.dir() != StreamDir::Uni {
                    return self
                        .protocol_error(1, "OpenUni with non-uni StreamId")
                        .await;
                }
                // In browser wasm, we are always the client, so the peer is the server.
                // Therefore, inbound streams must be server-initiated.
                if !id.initiator_is_server() {
                    return self.protocol_error(1, "OpenUni with wrong initiator").await;
                }

                let mut map = self.streams.borrow_mut();
                if map.len() >= self.limits.max_open_streams {
                    drop(map);
                    return self.protocol_error(3, "too many open streams").await;
                }
                if map.contains_key(&id) {
                    drop(map);
                    return self.protocol_error(1, "duplicate stream open").await;
                }

                let recv = Self::register_recv_stream_locked(&self, &mut map, id);
                drop(map);
                let _ = self.try_send_frame(Frame::MaxStreamData {
                    id,
                    max: self.limits.initial_stream_window as u64,
                });

                let tx = self.accept_uni_tx.lock().await.clone();
                if let Some(mut tx) = tx {
                    match tx.try_send(recv) {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            if e.is_full() {
                                self.streams.borrow_mut().remove(&id);
                                let _ = self.try_send_frame(Frame::ResetStream { id, code: 3 });
                                Ok(())
                            } else {
                                Err(Error::Closed)
                            }
                        }
                    }
                } else {
                    Err(Error::Closed)
                }
            }
            Frame::OpenBi { id } => {
                if id.dir() != StreamDir::Bi {
                    return self.protocol_error(1, "OpenBi with non-bi StreamId").await;
                }
                if !id.initiator_is_server() {
                    return self.protocol_error(1, "OpenBi with wrong initiator").await;
                }

                let mut map = self.streams.borrow_mut();
                if map.len() >= self.limits.max_open_streams {
                    drop(map);
                    return self.protocol_error(3, "too many open streams").await;
                }
                if map.contains_key(&id) {
                    drop(map);
                    return self.protocol_error(1, "duplicate stream open").await;
                }

                let recv = Self::register_recv_stream_locked(&self, &mut map, id);
                drop(map);
                let _ = self.try_send_frame(Frame::MaxStreamData {
                    id,
                    max: self.limits.initial_stream_window as u64,
                });

                let flow = self.register_send_flow(id, self.limits.initial_stream_window as u64);
                let send = SendStream::new(id, self.clone(), flow);
                let tx = self.accept_bi_tx.lock().await.clone();
                if let Some(mut tx) = tx {
                    match tx.try_send((send, recv)) {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            if e.is_full() {
                                self.streams.borrow_mut().remove(&id);
                                let _ = self.try_send_frame(Frame::ResetStream { id, code: 3 });
                                Ok(())
                            } else {
                                Err(Error::Closed)
                            }
                        }
                    }
                } else {
                    Err(Error::Closed)
                }
            }
            Frame::Stream { id, data, fin } => {
                if data.len() > self.limits.max_stream_data_per_frame {
                    return self.protocol_error(2, "stream data too large").await;
                }

                let mut map = self.streams.borrow_mut();
                let Some(tx) = map.get_mut(&id) else {
                    drop(map);
                    return self
                        .protocol_error(1, "Stream data on unknown stream")
                        .await;
                };

                match tx.try_send(RecvEvent { data, fin }) {
                    Ok(()) => {}
                    Err(e) => {
                        if e.is_full() {
                            map.remove(&id);
                            drop(map);
                            let _ = self.try_send_frame(Frame::ResetStream { id, code: 3 });
                            return Ok(());
                        } else {
                            map.remove(&id);
                            return Ok(());
                        }
                    }
                }

                if fin {
                    self.remove_send_flow(id);
                    map.remove(&id);
                }
                Ok(())
            }
            Frame::ResetStream { id, .. } | Frame::StopSending { id, .. } => {
                let removed_recv = self.streams.borrow_mut().remove(&id).is_some();
                let removed_send = self.send_flows.borrow_mut().remove(&id).is_some();
                if !removed_recv && !removed_send {
                    return self.protocol_error(1, "reset/stop on unknown stream").await;
                }
                Ok(())
            }
            Frame::MaxStreamData { id, max } => {
                if let Some(flow) = self.send_flows.borrow().get(&id) {
                    flow.update_max(max);
                }
                Ok(())
            }
            Frame::ConnectionClose { .. } => {
                self.close_all().await;
                Err(Error::Closed)
            }
        }
    }

    fn register_recv_stream(self: Rc<Self>, id: StreamId) -> RecvStream {
        let mut map = self.streams.borrow_mut();
        Self::register_recv_stream_locked(&self, &mut map, id)
    }

    fn register_recv_stream_locked(
        this: &Rc<Self>,
        map: &mut HashMap<StreamId, mpsc::Sender<RecvEvent>>,
        id: StreamId,
    ) -> RecvStream {
        let (tx, rx) = mpsc::channel(this.limits.recv_event_queue_len);
        map.insert(id, tx);
        RecvStream::new(
            id,
            this.clone(),
            rx,
            this.limits.initial_stream_window as u64,
            this.limits.stream_window_update_threshold as u64,
        )
    }

    fn register_send_flow(&self, id: StreamId, initial_max: u64) -> Rc<SendFlowState> {
        let flow = Rc::new(SendFlowState::new(initial_max));
        self.send_flows.borrow_mut().insert(id, flow.clone());
        flow
    }

    fn remove_send_flow(&self, id: StreamId) {
        self.send_flows.borrow_mut().remove(&id);
    }

    fn try_send_frame(&self, frame: Frame) -> std::result::Result<(), Error> {
        self.outbound_tx
            .borrow_mut()
            .try_send(OutboundCmd::Frame(frame))
            .map_err(|_| Error::Closed)
    }

    fn send_frame(&self, frame: Frame) -> Result<()> {
        self.try_send_frame(frame)
    }

    async fn protocol_error(&self, code: u64, reason: &str) -> Result<()> {
        let _ = self.try_send_frame(Frame::ConnectionClose {
            code,
            reason: reason.to_string(),
        });
        self.close_all().await;
        Err(Error::Protocol(reason.to_string()))
    }

    async fn close_all(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        self.streams.borrow_mut().clear();
        self.send_flows.borrow_mut().clear();
        *self.accept_uni_tx.lock().await = None;
        *self.accept_bi_tx.lock().await = None;
    }
}

impl websock_mux_proto::MuxSendStream for SendStream {
    fn write_buf<'a>(&'a self, data: Bytes) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { SendStream::write_buf(self, data) })
    }

    fn finish<'a>(&'a self) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { SendStream::finish(self) })
    }

    fn reset<'a>(&'a self, code: u64) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { SendStream::reset(self, code) })
    }

    fn closed(&self) -> bool {
        SendStream::closed(self)
    }
}

impl websock_mux_proto::MuxRecvStream for RecvStream {
    fn read_chunk<'a>(
        &'a mut self,
        max: usize,
    ) -> websock_proto::LocalBoxFuture<'a, Result<Option<Bytes>>> {
        Box::pin(async move { RecvStream::read_chunk(self, max).await })
    }

    fn stop<'a>(&'a self, code: u64) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { RecvStream::stop(self, code) })
    }

    fn closed(&self) -> bool {
        RecvStream::closed(self)
    }
}

impl websock_mux_proto::MuxSession for Session {
    type SendStream = SendStream;
    type RecvStream = RecvStream;

    fn open_uni<'a>(
        &'a self,
    ) -> websock_proto::LocalBoxFuture<
        'a,
        Result<<Self as websock_mux_proto::MuxSession>::SendStream>,
    > {
        Box::pin(async move { Session::open_uni(self) })
    }

    fn open_bi<'a>(
        &'a self,
    ) -> websock_proto::LocalBoxFuture<
        'a,
        Result<(
            <Self as websock_mux_proto::MuxSession>::SendStream,
            <Self as websock_mux_proto::MuxSession>::RecvStream,
        )>,
    > {
        Box::pin(async move { Session::open_bi(self) })
    }

    fn accept_uni<'a>(
        &'a self,
    ) -> websock_proto::LocalBoxFuture<
        'a,
        Result<<Self as websock_mux_proto::MuxSession>::RecvStream>,
    > {
        Box::pin(async move { Session::accept_uni(self).await })
    }

    fn accept_bi<'a>(
        &'a self,
    ) -> websock_proto::LocalBoxFuture<
        'a,
        Result<(
            <Self as websock_mux_proto::MuxSession>::SendStream,
            <Self as websock_mux_proto::MuxSession>::RecvStream,
        )>,
    > {
        Box::pin(async move { Session::accept_bi(self).await })
    }
}

fn io_closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "stream is closed")
}

fn io_invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
