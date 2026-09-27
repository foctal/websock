use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_channel::{mpsc, oneshot};
use futures_io::{AsyncRead as FuturesAsyncRead, AsyncWrite as FuturesAsyncWrite};
use futures_util::lock::Mutex;
use futures_util::stream::Stream;
use futures_util::task::AtomicWaker;
use futures_util::{FutureExt, StreamExt, future::poll_fn};
use wasm_bindgen_futures::spawn_local;
use websock_proto::{Error, Message, Result};

use websock_mux_proto::{Frame, StreamDir, StreamId, VarInt};

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
            max_ws_message_size: 1024 * 1024,
            max_stream_data_per_frame: 256 * 1024,
            max_open_streams: 1024,
            recv_event_queue_len: 128,
            outbound_queue_len: 256,
            max_batch_frames: 64,
            max_batch_bytes: 512 * 1024,
            initial_stream_window: 512 * 1024,
            stream_window_update_threshold: 256 * 1024,
            accept_uni_queue_len: 128,
            accept_bi_queue_len: 128,
        }
    }
}

impl Limits {
    /// Validate that the limits are non-zero and internally consistent.
    pub fn validate(&self) -> Result<()> {
        let non_zero = [
            ("max_ws_message_size", self.max_ws_message_size),
            ("max_stream_data_per_frame", self.max_stream_data_per_frame),
            ("max_open_streams", self.max_open_streams),
            ("recv_event_queue_len", self.recv_event_queue_len),
            ("outbound_queue_len", self.outbound_queue_len),
            ("max_batch_frames", self.max_batch_frames),
            ("max_batch_bytes", self.max_batch_bytes),
            ("initial_stream_window", self.initial_stream_window),
            (
                "stream_window_update_threshold",
                self.stream_window_update_threshold,
            ),
            ("accept_uni_queue_len", self.accept_uni_queue_len),
            ("accept_bi_queue_len", self.accept_bi_queue_len),
        ];
        if let Some((name, _)) = non_zero.into_iter().find(|(_, value)| *value == 0) {
            return Err(Error::Protocol(format!("{name} must be greater than zero")));
        }
        if self.max_stream_data_per_frame > self.max_ws_message_size {
            return Err(Error::Protocol(
                "max_stream_data_per_frame must not exceed max_ws_message_size".into(),
            ));
        }
        if self.max_batch_bytes > self.max_ws_message_size {
            return Err(Error::Protocol(
                "max_batch_bytes must not exceed max_ws_message_size".into(),
            ));
        }
        if self.max_batch_bytes < self.max_stream_data_per_frame.saturating_add(33) {
            return Err(Error::Protocol(
                "max_batch_bytes must accommodate a maximum-size stream frame".into(),
            ));
        }
        if self.stream_window_update_threshold > self.initial_stream_window {
            return Err(Error::Protocol(
                "stream_window_update_threshold must not exceed initial_stream_window".into(),
            ));
        }
        if self.max_ws_message_size as u64 > VarInt::MAX.into_inner()
            || self.initial_stream_window as u64 > VarInt::MAX.into_inner()
        {
            return Err(Error::Protocol(
                "byte and flow-control limits must fit in a mux varint".into(),
            ));
        }
        Ok(())
    }
}

pub struct Session {
    inner: Rc<SessionInner>,
    accept_uni: Rc<Mutex<mpsc::Receiver<RecvStream>>>,
    accept_bi: Rc<Mutex<mpsc::Receiver<(SendStream, RecvStream)>>>,
}

impl Session {
    pub(crate) fn new(conn: websock_wasm::Connection, limits: Limits) -> Result<Self> {
        limits.validate()?;
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
        Ok(session)
    }

    pub fn open_uni(&self) -> Result<SendStream> {
        let id = self.inner.next_stream_id(StreamDir::Uni)?;
        let flow = self.inner.register_send_flow(id, 0)?;
        if let Err(err) = self.inner.send_frame(Frame::OpenUni { id }) {
            self.inner.remove_send_flow(id);
            return Err(err);
        }
        Ok(SendStream::new(id, self.inner.clone(), flow))
    }

    pub fn open_bi(&self) -> Result<(SendStream, RecvStream)> {
        let id = self.inner.next_stream_id(StreamDir::Bi)?;
        let flow = self.inner.register_send_flow(id, 0)?;
        let recv = self.inner.clone().register_recv_stream(id);
        if let Err(err) = self.inner.send_frame(Frame::OpenBi { id }) {
            self.inner.streams.borrow_mut().remove(&id);
            self.inner.remove_send_flow(id);
            return Err(err);
        }
        if let Err(err) = self.inner.send_frame(Frame::MaxStreamData {
            id,
            max: self.inner.limits.initial_stream_window as u64,
        }) {
            self.inner.streams.borrow_mut().remove(&id);
            self.inner.remove_send_flow(id);
            return Err(err);
        }
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

    /// Close the WebSocket and wait for the session task to finish.
    pub async fn shutdown(&self) -> Result<()> {
        self.inner.request_shutdown();
        self.inner.wait_closed().await
    }

    /// Return whether the session has finished shutting down.
    pub fn is_closed(&self) -> bool {
        self.inner.task_finished.load(Ordering::Acquire)
    }
}

impl Clone for Session {
    fn clone(&self) -> Self {
        self.inner
            .session_handles
            .set(self.inner.session_handles.get() + 1);
        Self {
            inner: self.inner.clone(),
            accept_uni: self.accept_uni.clone(),
            accept_bi: self.accept_bi.clone(),
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let remaining = self.inner.session_handles.get() - 1;
        self.inner.session_handles.set(remaining);
        if remaining == 0 {
            self.inner.request_shutdown();
        }
    }
}

struct SendFlowState {
    max_data: AtomicU64,
    sent_data: AtomicU64,
    closed: AtomicBool,
    waker: AtomicWaker,
}

impl SendFlowState {
    fn new(initial_max: u64) -> Self {
        Self {
            max_data: AtomicU64::new(initial_max),
            sent_data: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        }
    }

    fn try_reserve(&self, requested: usize) -> usize {
        if requested == 0 || self.closed.load(Ordering::Acquire) {
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
        if self.closed.load(Ordering::Acquire) {
            return;
        }
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

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.waker.wake();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// A send direction. Choose direct write/finish methods or AsyncWrite on first
/// use; mixing APIs or overlapping direct operations through clones is rejected.
/// Cancelling a direct operation aborts the session because partial publication
/// cannot be rolled back. Dropping an unfinished handle resets it, or aborts
/// the session if the bounded control queue cannot accept the reset.
pub struct SendStream {
    id: StreamId,
    session: Rc<SessionInner>,
    finished: Rc<AtomicBool>,
    operation: Rc<AtomicBool>,
    api_mode: Rc<AtomicU64>,
    flow: Rc<SendFlowState>,
    outbound: mpsc::Sender<OutboundCmd>,
    close_in_flight: bool,
}

// Shared across clones: concurrent direct operations are rejected instead of
// allowing FIN/RESET to overtake a suspended write.
struct SendOperation(Rc<AtomicBool>);
impl Drop for SendOperation {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl SendStream {
    fn select_api(&self, mode: u64) -> Result<()> {
        if mode == 0 {
            return Ok(());
        }
        match self
            .api_mode
            .compare_exchange(0, mode, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(old) if old == mode => Ok(()),
            Err(_) => Err(Error::Protocol(
                "cannot mix direct and AsyncWrite APIs on a send stream".into(),
            )),
        }
    }
    fn begin_operation(&self, mode: u64) -> Result<SendOperation> {
        self.select_api(mode)?;
        if self.operation.swap(true, Ordering::AcqRel) {
            return Err(Error::Protocol(
                "concurrent send operations are unsupported".into(),
            ));
        }
        Ok(SendOperation(self.operation.clone()))
    }

    fn new(id: StreamId, session: Rc<SessionInner>, flow: Rc<SendFlowState>) -> Self {
        let outbound = session.outbound_tx.borrow().clone();
        Self {
            id,
            session,
            finished: Rc::new(AtomicBool::new(false)),
            operation: Rc::new(AtomicBool::new(false)),
            api_mode: Rc::new(AtomicU64::new(0)),
            flow,
            outbound,
            close_in_flight: false,
        }
    }

    pub async fn write(&self, data: &[u8]) -> Result<()> {
        self.write_buf(Bytes::copy_from_slice(data)).await
    }

    pub async fn write_buf(&self, data: Bytes) -> Result<()> {
        let _operation = self.begin_operation(1)?;
        self.session
            .complete_or_abort(async {
                if self.finished.load(Ordering::SeqCst)
                    || self.flow.is_closed()
                    || (self.session.closed.load(Ordering::SeqCst)
                        || self.session.shutdown_started.load(Ordering::Acquire))
                {
                    return Err(Error::Closed);
                }
                let mut offset = 0usize;
                // Reuse one sender so its reserved channel slot cannot be
                // renewed for every chunk while the queue is full.
                let mut outbound = self.outbound.clone();
                while offset < data.len() {
                    let wanted = (data.len() - offset)
                        .min(MAX_WRITE_CHUNK)
                        .min(self.session.limits.max_stream_data_per_frame);
                    if wanted == 0 {
                        return Err(Error::Protocol("stream frame payload limit is zero".into()));
                    }
                    let grant = poll_fn(|cx| {
                        self.flow.waker.register(cx.waker());
                        let grant = self.flow.try_reserve(wanted);
                        if grant == 0 {
                            if self.flow.is_closed()
                                || self.finished.load(Ordering::SeqCst)
                                || (self.session.closed.load(Ordering::SeqCst)
                                    || self.session.shutdown_started.load(Ordering::Acquire))
                            {
                                Poll::Ready(Err(Error::Closed))
                            } else {
                                Poll::Pending
                            }
                        } else {
                            Poll::Ready(Ok(grant))
                        }
                    })
                    .await?;
                    let chunk = data.slice(offset..offset + grant);
                    let mut frame = Some(Frame::Stream {
                        id: self.id,
                        data: chunk,
                        fin: false,
                    });
                    let queued = poll_fn(|cx| {
                        self.flow.waker.register(cx.waker());
                        if self.flow.is_closed() {
                            return Poll::Ready(Err(Error::Closed));
                        }
                        match outbound.poll_ready(cx) {
                            Poll::Pending => Poll::Pending,
                            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Closed)),
                            Poll::Ready(Ok(())) => Poll::Ready(
                                outbound
                                    .start_send(OutboundCmd::Frame(
                                        frame.take().expect("frame queued once"),
                                    ))
                                    .map_err(|_| Error::Closed),
                            ),
                        }
                    })
                    .await;
                    if queued.is_err() {
                        self.flow.release(grant);
                        return Err(Error::Closed);
                    }
                    offset += grant;
                }
                Ok(())
            })
            .await
    }

    pub async fn write_all(&self, data: &[u8]) -> Result<()> {
        self.write(data).await
    }

    pub async fn finish(&self) -> Result<()> {
        let _operation = self.begin_operation(1)?;
        if self.finished.load(Ordering::SeqCst) {
            return Ok(());
        }
        if self.flow.is_closed()
            || (self.session.closed.load(Ordering::SeqCst)
                || self.session.shutdown_started.load(Ordering::Acquire))
        {
            return Err(Error::Closed);
        }
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

    pub async fn reset(&self, code: u64) -> Result<()> {
        let _operation = self.begin_operation(0)?;
        VarInt::from_u64(code)
            .map_err(|_| Error::Protocol("reset code exceeds mux varint range".into()))?;
        self.finished.store(true, Ordering::SeqCst);
        self.session.remove_send_flow(self.id);
        self.session
            .send_frame(Frame::ResetStream { id: self.id, code })
    }

    pub fn closed(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
            || self.flow.is_closed()
            || (self.session.closed.load(Ordering::SeqCst)
                || self.session.shutdown_started.load(Ordering::Acquire))
    }
}

impl Clone for SendStream {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            session: self.session.clone(),
            finished: self.finished.clone(),
            operation: self.operation.clone(),
            api_mode: self.api_mode.clone(),
            flow: self.flow.clone(),
            outbound: self.outbound.clone(),
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
        if let Err(error) = this.select_api(2) {
            return Poll::Ready(Err(io::Error::other(error.to_string())));
        }
        let _operation = match this.begin_operation(2) {
            Ok(guard) => guard,
            Err(error) => return Poll::Ready(Err(io::Error::other(error.to_string()))),
        };
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.finished.load(Ordering::SeqCst)
            || this.flow.is_closed()
            || (this.session.closed.load(Ordering::SeqCst)
                || this.session.shutdown_started.load(Ordering::Acquire))
        {
            return Poll::Ready(Err(io_closed()));
        }

        {
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
                return if this.flow.is_closed()
                    || (this.session.closed.load(Ordering::SeqCst)
                        || this.session.shutdown_started.load(Ordering::Acquire))
                {
                    Poll::Ready(Err(io_closed()))
                } else {
                    Poll::Pending
                };
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
            // start_send owns these bytes. A later call may provide an
            // unrelated buffer, so acknowledge this chunk immediately.
            Poll::Ready(Ok(chunk_len))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(error) = this.select_api(2) {
            return Poll::Ready(Err(io::Error::other(error.to_string())));
        }
        match this.outbound.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => Poll::Ready(Err(io_closed())),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(error) = this.select_api(2) {
            return Poll::Ready(Err(io::Error::other(error.to_string())));
        }
        if !this.finished.load(Ordering::SeqCst)
            && (this.flow.is_closed()
                || (this.session.closed.load(Ordering::SeqCst)
                    || this.session.shutdown_started.load(Ordering::Acquire)))
        {
            return Poll::Ready(Err(io_closed()));
        }

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
        if Rc::strong_count(&self.finished) != 1 {
            return;
        }
        self.session.remove_send_flow(self.id);
        if !self.finished.load(Ordering::SeqCst)
            && self
                .session
                .try_send_frame(Frame::ResetStream {
                    id: self.id,
                    code: 0,
                })
                .is_err()
        {
            // A destructor cannot wait for capacity. Fail closed rather
            // than silently leaving the peer waiting for a terminal frame.
            self.session.request_shutdown();
        }
    }
}

#[derive(Debug)]
struct RecvEvent {
    data: Bytes,
    fin: bool,
}

struct RecvState {
    sender: mpsc::Sender<RecvEvent>,
    stopped: bool,
    received: u64,
    max_data: Rc<AtomicU64>,
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
    max_data: Rc<AtomicU64>,
    stop_sent: AtomicBool,
}

impl RecvStream {
    fn new(
        id: StreamId,
        session: Rc<SessionInner>,
        receiver: mpsc::Receiver<RecvEvent>,
        initial_window: u64,
        update_threshold: u64,
        max_data: Rc<AtomicU64>,
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
            max_data,
            stop_sent: AtomicBool::new(false),
        }
    }

    fn on_bytes_consumed(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        self.consumed = self.consumed.saturating_add(n as u64);
        let target = self
            .consumed
            .saturating_add(self.initial_window)
            .min(VarInt::MAX.into_inner());
        if target <= self.granted {
            return;
        }
        if target - self.granted < self.update_threshold {
            return;
        }
        if self.finished || self.stop_sent.load(Ordering::Acquire) {
            return;
        }
        self.granted = target;
        self.max_data.store(target, Ordering::Release);
        self.session.queue_credit(self.id, target);
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>> {
        if buf.is_empty() {
            return Ok(Some(0));
        }
        if self.pending.is_empty() {
            if self.finished {
                return Ok(None);
            }
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
        if buf.remaining_mut() == 0 {
            return Ok(Some(0));
        }
        if self.pending.is_empty() {
            if self.finished {
                return Ok(None);
            }
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
        if max == 0 {
            return Err(Error::Protocol(
                "read_chunk max must be greater than zero".into(),
            ));
        }
        if !self.pending.is_empty() {
            let amount = self.pending.len().min(max);
            let chunk = self.pending.split_to(amount);
            self.on_bytes_consumed(chunk.len());
            return Ok(Some(chunk));
        }
        if self.finished {
            return Ok(None);
        }
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
        VarInt::from_u64(code)
            .map_err(|_| Error::Protocol("stop code exceeds mux varint range".into()))?;
        if self.stop_sent.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        if !self.session.stop_recv_stream(self.id) {
            return Ok(());
        }
        self.session
            .send_frame(Frame::StopSending { id: self.id, code })
    }

    pub fn closed(&self) -> bool {
        self.finished
    }
}

impl Drop for RecvStream {
    fn drop(&mut self) {
        let needs_stop = self.session.stop_recv_stream(self.id);
        if needs_stop
            && !self.finished
            && !self.stop_sent.swap(true, Ordering::SeqCst)
            && self
                .session
                .try_send_frame(Frame::StopSending {
                    id: self.id,
                    code: 0,
                })
                .is_err()
        {
            // A destructor cannot wait for capacity. Fail closed rather
            // than silently leaving the peer waiting for a terminal frame.
            self.session.request_shutdown();
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
    pending_credit: RefCell<HashMap<StreamId, u64>>,
    credit_waker: AtomicWaker,
    accept_uni_tx: Mutex<Option<mpsc::Sender<RecvStream>>>,
    accept_bi_tx: Mutex<Option<mpsc::Sender<(SendStream, RecvStream)>>>,
    streams: RefCell<HashMap<StreamId, RecvState>>,
    send_flows: RefCell<HashMap<StreamId, Rc<SendFlowState>>>,
    next_uni: AtomicU64,
    next_bi: AtomicU64,
    next_peer_uni: AtomicU64,
    next_peer_bi: AtomicU64,
    closed: AtomicBool,
    shutdown_started: AtomicBool,
    session_handles: Cell<usize>,
    close_waiters: RefCell<Vec<oneshot::Sender<()>>>,
    task_finished: AtomicBool,
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
            pending_credit: RefCell::new(HashMap::new()),
            credit_waker: AtomicWaker::new(),
            accept_uni_tx: Mutex::new(Some(accept_uni_tx)),
            accept_bi_tx: Mutex::new(Some(accept_bi_tx)),
            streams: RefCell::new(HashMap::new()),
            send_flows: RefCell::new(HashMap::new()),
            next_uni: AtomicU64::new(0),
            next_bi: AtomicU64::new(0),
            next_peer_uni: AtomicU64::new(0),
            next_peer_bi: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            shutdown_started: AtomicBool::new(false),
            session_handles: Cell::new(1),
            close_waiters: RefCell::new(Vec::new()),
            task_finished: AtomicBool::new(false),
        }
    }

    async fn complete_or_abort<T>(&self, operation: impl std::future::Future<Output = T>) -> T {
        struct Guard<'a> {
            session: &'a SessionInner,
            complete: bool,
        }
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                if !self.complete {
                    self.session.request_shutdown();
                }
            }
        }
        let mut guard = Guard {
            session: self,
            complete: false,
        };
        let result = operation.await;
        guard.complete = true;
        result
    }

    fn queue_credit(&self, id: StreamId, maximum: u64) {
        let streams = self.streams.borrow();
        // A FIN/reset may retire the receive state before the application
        // finishes consuming buffered events. Such streams need no new credit.
        if streams.get(&id).is_none_or(|state| state.stopped) {
            return;
        }
        let mut pending = self.pending_credit.borrow_mut();
        if pending.len() >= self.limits.max_open_streams {
            // Retired streams must not consume the bounded update slots needed
            // by a new generation of live streams while the writer is stalled.
            pending.retain(|stream_id, _| streams.contains_key(stream_id));
        }
        if pending.len() >= self.limits.max_open_streams && !pending.contains_key(&id) {
            drop(pending);
            drop(streams);
            self.request_shutdown();
            return;
        }
        pending
            .entry(id)
            .and_modify(|old| *old = (*old).max(maximum))
            .or_insert(maximum);
        drop(pending);
        self.credit_waker.wake();
    }
    async fn next_credit(&self) -> Frame {
        poll_fn(|cx| {
            self.credit_waker.register(cx.waker());
            let mut pending = self.pending_credit.borrow_mut();
            match pending.keys().next().copied() {
                Some(id) => Poll::Ready(Frame::MaxStreamData {
                    id,
                    max: pending.remove(&id).expect("pending credit"),
                }),
                None => Poll::Pending,
            }
        })
        .await
    }
    fn request_shutdown(&self) {
        if !self.shutdown_started.swap(true, Ordering::AcqRel) {
            self.outbound_tx.borrow_mut().close_channel();
        }
    }

    async fn wait_closed(&self) -> Result<()> {
        if self.task_finished.load(Ordering::Acquire) {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        {
            let mut waiters = self.close_waiters.borrow_mut();
            waiters.retain(|waiter| !waiter.is_canceled());
            waiters.push(tx);
        }
        rx.await.map_err(|_| Error::Closed)
    }

    fn spawn_task(
        self: Rc<Self>,
        mut conn: websock_wasm::Connection,
        mut outbound_rx: mpsc::Receiver<OutboundCmd>,
    ) {
        let inner = self.clone();
        spawn_local(async move {
            let mut pending_frame = None;
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
                    out = async {
                        if let Some(frame) = pending_frame.take() { Some(OutboundCmd::Frame(frame)) }
                        else {
                            let queued = outbound_rx.next().fuse();
                            let credit = inner.next_credit().fuse();
                            futures_util::pin_mut!(queued, credit);
                            futures_util::select! {
                                command = queued => command,
                                frame = credit => Some(OutboundCmd::Frame(frame)),
                            }
                        }
                    }.fuse() => {
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
                                                pending_frame = Some(next_frame);
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

            {
                let close = conn.close().fuse();
                let deadline = gloo_timers::future::TimeoutFuture::new(5_000).fuse();
                futures_util::pin_mut!(close, deadline);
                futures_util::select! { _ = close => {}, _ = deadline => {} }
            }
            drop(conn);
            drop(outbound_rx);
            inner.close_all().await;
            inner.finish_task();
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
                if !self.validate_peer_stream_id(id) {
                    return self
                        .protocol_error(1, "OpenUni with non-monotonic StreamId")
                        .await;
                }

                let validation = {
                    let map = self.streams.borrow();
                    if map.len() >= self.limits.max_open_streams {
                        Err((3, "too many open streams"))
                    } else if map.contains_key(&id) {
                        Err((1, "duplicate stream open"))
                    } else {
                        Ok(())
                    }
                };
                if let Err((code, reason)) = validation {
                    return self.protocol_error(code, reason).await;
                }
                let recv = {
                    let mut map = self.streams.borrow_mut();
                    Self::register_recv_stream_locked(self, &mut map, id)
                };
                let _ = self.try_send_frame(Frame::MaxStreamData {
                    id,
                    max: self.limits.initial_stream_window as u64,
                });

                let mut accept = self.accept_uni_tx.lock().await;
                if let Some(tx) = accept.as_mut() {
                    match tx.try_send(recv) {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            if e.is_full() {
                                drop(accept);
                                self.protocol_error(3, "accept queue full").await
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
                if !self.validate_peer_stream_id(id) {
                    return self
                        .protocol_error(1, "OpenBi with non-monotonic StreamId")
                        .await;
                }

                let validation = {
                    let map = self.streams.borrow();
                    if map.len() >= self.limits.max_open_streams {
                        Err((3, "too many open streams"))
                    } else if map.contains_key(&id) {
                        Err((1, "duplicate stream open"))
                    } else {
                        Ok(())
                    }
                };
                if let Err((code, reason)) = validation {
                    return self.protocol_error(code, reason).await;
                }
                let recv = {
                    let mut map = self.streams.borrow_mut();
                    Self::register_recv_stream_locked(self, &mut map, id)
                };
                let _ = self.try_send_frame(Frame::MaxStreamData {
                    id,
                    max: self.limits.initial_stream_window as u64,
                });

                let flow = match self.register_send_flow(id, 0) {
                    Ok(flow) => flow,
                    Err(err) => return self.protocol_error(3, &err.to_string()).await,
                };
                let send = SendStream::new(id, self.clone(), flow);
                let mut accept = self.accept_bi_tx.lock().await;
                if let Some(tx) = accept.as_mut() {
                    match tx.try_send((send, recv)) {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            if e.is_full() {
                                drop(accept);
                                self.protocol_error(3, "accept queue full").await
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

                let result = {
                    let mut map = self.streams.borrow_mut();
                    match map.get_mut(&id) {
                        None => Err("Stream data on unknown stream"),
                        Some(state) => match state.received.checked_add(data.len() as u64) {
                            None => Err("stream data overflow"),
                            Some(received) if received > state.max_data.load(Ordering::Acquire) => {
                                Err("stream flow-control limit exceeded")
                            }
                            Some(received) => {
                                state.received = received;
                                let (remove, overflow) = if state.stopped {
                                    (fin, false)
                                } else {
                                    match state.sender.try_send(RecvEvent { data, fin }) {
                                        Ok(()) => (fin, false),
                                        Err(error) => (fin, error.is_full()),
                                    }
                                };
                                if remove {
                                    map.remove(&id);
                                };
                                Ok(overflow)
                            }
                        },
                    }
                };
                let overflow = match result {
                    Ok(overflow) => overflow,
                    Err(reason) => return self.protocol_error(2, reason).await,
                };
                if overflow {
                    return self.protocol_error(3, "receive queue full").await;
                }
                Ok(())
            }
            Frame::ResetStream { id, .. } => {
                let removed = { self.streams.borrow_mut().remove(&id).is_some() };
                if !removed && !self.is_retired_direction(id, true) {
                    return self.protocol_error(1, "reset on unknown stream").await;
                }
                Ok(())
            }
            Frame::StopSending { id, code } => {
                if self.remove_send_flow(id) {
                    self.send_frame(Frame::ResetStream { id, code })?;
                } else if !self.is_retired_direction(id, false) {
                    return self.protocol_error(1, "stop on unknown stream").await;
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
        map: &mut HashMap<StreamId, RecvState>,
        id: StreamId,
    ) -> RecvStream {
        let (tx, rx) = mpsc::channel(this.limits.recv_event_queue_len);
        let max_data = Rc::new(AtomicU64::new(this.limits.initial_stream_window as u64));
        map.insert(
            id,
            RecvState {
                sender: tx,
                stopped: false,
                received: 0,
                max_data: max_data.clone(),
            },
        );
        RecvStream::new(
            id,
            this.clone(),
            rx,
            this.limits.initial_stream_window as u64,
            this.limits.stream_window_update_threshold as u64,
            max_data,
        )
    }

    fn register_send_flow(&self, id: StreamId, initial_max: u64) -> Result<Rc<SendFlowState>> {
        let flow = Rc::new(SendFlowState::new(initial_max));
        let mut send_flows = self.send_flows.borrow_mut();
        if send_flows.len() >= self.limits.max_open_streams {
            return Err(Error::Protocol("too many open send streams".into()));
        }
        if send_flows.contains_key(&id) {
            return Err(Error::Protocol("duplicate send stream".into()));
        }
        send_flows.insert(id, flow.clone());
        Ok(flow)
    }

    fn stop_recv_stream(&self, id: StreamId) -> bool {
        let mut streams = self.streams.borrow_mut();
        let Some(state) = streams.get_mut(&id) else {
            return false;
        };
        state.stopped = true;
        self.pending_credit.borrow_mut().remove(&id);
        // Retain only bounded accounting until the peer's FIN or RESET arrives.
        true
    }

    fn is_retired_direction(&self, id: StreamId, receiving: bool) -> bool {
        let local = !id.initiator_is_server(); // The browser is always the client.
        if id.dir() == StreamDir::Uni && local == receiving {
            return false;
        }
        let next = match (local, id.dir()) {
            (true, StreamDir::Uni) => &self.next_uni,
            (true, StreamDir::Bi) => &self.next_bi,
            (false, StreamDir::Uni) => &self.next_peer_uni,
            (false, StreamDir::Bi) => &self.next_peer_bi,
        };
        // Monotonic IDs below the allocation frontier cannot be opened again.
        id.counter() < next.load(Ordering::Acquire)
    }

    fn remove_send_flow(&self, id: StreamId) -> bool {
        if let Some(flow) = self.send_flows.borrow_mut().remove(&id) {
            flow.close();
            true
        } else {
            false
        }
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

        self.pending_credit.borrow_mut().clear();
        self.streams.borrow_mut().clear();
        {
            let mut send_flows = self.send_flows.borrow_mut();
            for flow in send_flows.values() {
                flow.close();
            }
            send_flows.clear();
        }
        *self.accept_uni_tx.lock().await = None;
        *self.accept_bi_tx.lock().await = None;
    }

    fn finish_task(&self) {
        self.task_finished.store(true, Ordering::Release);
        for waiter in self.close_waiters.borrow_mut().drain(..) {
            let _ = waiter.send(());
        }
    }

    fn validate_peer_stream_id(&self, id: StreamId) -> bool {
        let next = match id.dir() {
            StreamDir::Uni => &self.next_peer_uni,
            StreamDir::Bi => &self.next_peer_bi,
        };
        let mut current = next.load(Ordering::SeqCst);
        loop {
            if id.counter() < current {
                return false;
            }
            match next.compare_exchange(
                current,
                id.counter().saturating_add(1),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }
}

impl websock_mux_proto::MuxSendStream for SendStream {
    fn write_buf<'a>(&'a self, data: Bytes) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { SendStream::write_buf(self, data).await })
    }

    fn finish<'a>(&'a self) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { SendStream::finish(self).await })
    }

    fn reset<'a>(&'a self, code: u64) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { SendStream::reset(self, code).await })
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

    fn shutdown<'a>(&'a self) -> websock_proto::LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { Session::shutdown(self).await })
    }

    fn closed(&self) -> bool {
        Session::is_closed(self)
    }
}

fn io_closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "stream is closed")
}

fn io_invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod phase2_tests {
    #[wasm_bindgen_test::wasm_bindgen_test(async)]
    async fn accept_queue_overflow_terminates_the_session() {
        for dir in [StreamDir::Uni, StreamDir::Bi] {
            let (tx, _outgoing) = mpsc::channel(32);
            let (uni_tx, _uni_rx) = mpsc::channel(1);
            let (bi_tx, _bi_rx) = mpsc::channel(1);
            let inner = Rc::new(SessionInner::new(Limits::default(), tx, uni_tx, bi_tx));
            let mut rejected = false;
            for counter in 0..8 {
                let id = StreamId::new(counter, true, dir).unwrap();
                let frame = match dir {
                    StreamDir::Uni => Frame::OpenUni { id },
                    StreamDir::Bi => Frame::OpenBi { id },
                };
                if inner.handle_frame(frame).await.is_err() {
                    rejected = true;
                    break;
                }
            }
            assert!(rejected, "accept queues must remain bounded");
            assert!(inner.closed.load(Ordering::SeqCst));
        }
    }

    #[wasm_bindgen_test::wasm_bindgen_test(async)]
    async fn receive_queue_overflow_terminates_the_session() {
        let (inner, mut outgoing) = lifecycle_session(Limits {
            recv_event_queue_len: 1,
            ..Limits::default()
        });
        let id = inner.next_stream_id(StreamDir::Bi).unwrap();
        let _recv = inner.clone().register_recv_stream(id);
        let mut rejected = false;
        for _ in 0..8 {
            if inner
                .handle_frame(Frame::Stream {
                    id,
                    data: Bytes::from_static(b"x"),
                    fin: false,
                })
                .await
                .is_err()
            {
                rejected = true;
                break;
            }
        }
        assert!(
            rejected,
            "queue overflow must terminate instead of sending a reset in the wrong direction"
        );
        assert!(inner.closed.load(Ordering::SeqCst));
        assert!(matches!(
            outgoing.try_recv().unwrap(),
            OutboundCmd::Frame(Frame::ConnectionClose { code: 3, .. })
        ));
    }

    #[wasm_bindgen_test::wasm_bindgen_test(async)]
    async fn reading_buffered_data_after_stop_does_not_expand_receive_credit() {
        let (inner, _outgoing) = lifecycle_session(Limits {
            initial_stream_window: 8,
            stream_window_update_threshold: 4,
            ..Limits::default()
        });
        let id = inner.next_stream_id(StreamDir::Bi).unwrap();
        let mut recv = inner.clone().register_recv_stream(id);
        inner
            .handle_frame(Frame::Stream {
                id,
                data: Bytes::from_static(b"1234"),
                fin: false,
            })
            .await
            .unwrap();
        recv.stop(0).unwrap();
        assert_eq!(recv.read_chunk(4).await.unwrap().unwrap().len(), 4);
        assert_eq!(recv.max_data.load(Ordering::Acquire), 8);
        assert!(
            inner
                .handle_frame(Frame::Stream {
                    id,
                    data: Bytes::from_static(b"56789"),
                    fin: false
                })
                .await
                .is_err()
        );
    }

    #[wasm_bindgen_test::wasm_bindgen_test(async)]
    async fn stop_wakes_an_async_writer_waiting_for_queue_capacity() {
        use std::future::Future;
        use std::task::{Context, Waker};

        let (inner, mut outgoing) = lifecycle_session(Limits::default());
        let id = inner.next_stream_id(StreamDir::Bi).unwrap();
        let flow = inner.register_send_flow(id, u64::MAX).unwrap();
        let send = SendStream::new(id, inner.clone(), flow);
        let mut write = Box::pin(send.write_buf(Bytes::from(vec![1; MAX_WRITE_CHUNK * 32])));
        assert!(
            write
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(inner.remove_send_flow(id));
        assert!(write.await.is_err());
        // Capacity becoming available cannot publish any further data.
        while outgoing.try_recv().is_ok() {}
        assert!(send.write(b"after stop").await.is_err());
        assert!(outgoing.try_recv().is_err());
    }

    fn lifecycle_session(limits: Limits) -> (Rc<SessionInner>, mpsc::Receiver<OutboundCmd>) {
        let (tx, rx) = mpsc::channel(8);
        let (uni_tx, _uni_rx) = mpsc::channel(1);
        let (bi_tx, _bi_rx) = mpsc::channel(1);
        let inner = Rc::new(SessionInner::new(limits, tx, uni_tx, bi_tx));
        (inner, rx)
    }

    #[wasm_bindgen_test::wasm_bindgen_test(async)]
    async fn stopped_receive_drains_in_flight_data_until_fin_without_new_credit() {
        let (inner, mut outgoing) = lifecycle_session(Limits::default());
        let id = inner.next_stream_id(StreamDir::Bi).unwrap();
        let recv = inner.clone().register_recv_stream(id);
        inner.queue_credit(id, 128);
        drop(recv);
        assert!(
            matches!(outgoing.try_recv().unwrap(), OutboundCmd::Frame(Frame::StopSending { id: stopped, .. }) if stopped == id)
        );
        assert!(inner.pending_credit.borrow().is_empty());
        for fin in [false, true] {
            inner
                .handle_frame(Frame::Stream {
                    id,
                    data: Bytes::from_static(b"in flight"),
                    fin,
                })
                .await
                .unwrap();
        }
        assert!(inner.streams.borrow().is_empty());
        // The opposite direction and subsequent streams remain usable.
        let flow = inner.register_send_flow(id, 64).unwrap();
        assert!(!flow.is_closed());
        let id = inner.next_stream_id(StreamDir::Bi).unwrap();
        let mut recv = inner.clone().register_recv_stream(id);
        inner
            .handle_frame(Frame::Stream {
                id,
                data: Bytes::from_static(b"live"),
                fin: true,
            })
            .await
            .unwrap();
        assert_eq!(
            recv.read_chunk(4).await.unwrap().unwrap(),
            Bytes::from_static(b"live")
        );
    }

    #[wasm_bindgen_test::wasm_bindgen_test(async)]
    async fn stopped_receive_still_enforces_the_original_credit_limit() {
        let limits = Limits {
            initial_stream_window: 4,
            stream_window_update_threshold: 4,
            ..Limits::default()
        };
        let (inner, _outgoing) = lifecycle_session(limits);
        let id = inner.next_stream_id(StreamDir::Bi).unwrap();
        let recv = inner.clone().register_recv_stream(id);
        drop(recv);
        inner.queue_credit(id, 1024);
        assert!(inner.pending_credit.borrow().is_empty());
        assert!(
            inner
                .handle_frame(Frame::Stream {
                    id,
                    data: Bytes::from_static(b"12345"),
                    fin: false
                })
                .await
                .is_err()
        );
    }

    #[wasm_bindgen_test::wasm_bindgen_test(async)]
    async fn received_fin_suppresses_drop_stop_and_tolerates_crossed_terminal_frames() {
        let (inner, mut outgoing) = lifecycle_session(Limits::default());
        let id = inner.next_stream_id(StreamDir::Bi).unwrap();
        let recv = inner.clone().register_recv_stream(id);
        inner
            .handle_frame(Frame::Stream {
                id,
                data: Bytes::new(),
                fin: true,
            })
            .await
            .unwrap();
        drop(recv);
        assert!(outgoing.try_recv().is_err());
        let flow = inner.register_send_flow(id, 64).unwrap();
        assert!(inner.remove_send_flow(id));
        assert!(flow.is_closed());
        inner
            .handle_frame(Frame::StopSending { id, code: 7 })
            .await
            .unwrap();
        inner
            .handle_frame(Frame::ResetStream { id, code: 7 })
            .await
            .unwrap();
        assert!(outgoing.try_recv().is_err());
    }

    #[wasm_bindgen_test::wasm_bindgen_test(async)]
    async fn stop_sending_acknowledges_with_reset_and_releases_send_flow() {
        let (inner, mut outgoing) = lifecycle_session(Limits::default());
        let id = inner.next_stream_id(StreamDir::Bi).unwrap();
        let flow = inner.register_send_flow(id, 64).unwrap();
        inner
            .handle_frame(Frame::StopSending { id, code: 7 })
            .await
            .unwrap();
        assert!(flow.is_closed());
        assert!(
            matches!(outgoing.try_recv().unwrap(), OutboundCmd::Frame(Frame::ResetStream { id: reset, code: 7 }) if reset == id)
        );
    }

    #[wasm_bindgen_test::wasm_bindgen_test(async)]
    async fn terminal_frames_for_future_or_wrong_direction_streams_are_errors() {
        for reset in [false, true] {
            for wrong_direction in [false, true] {
                let (inner, _outgoing) = lifecycle_session(Limits::default());
                let id = if wrong_direction {
                    if reset {
                        inner.next_stream_id(StreamDir::Uni).unwrap()
                    } else {
                        let id = StreamId::new(0, true, StreamDir::Uni).unwrap();
                        assert!(inner.validate_peer_stream_id(id));
                        id
                    }
                } else {
                    StreamId::new(100, false, StreamDir::Bi).unwrap()
                };
                let frame = if reset {
                    Frame::ResetStream { id, code: 0 }
                } else {
                    Frame::StopSending { id, code: 0 }
                };
                assert!(inner.handle_frame(frame).await.is_err());
            }
        }
    }

    #[wasm_bindgen_test::wasm_bindgen_test(async)]
    async fn retired_stream_credit_does_not_exhaust_update_slots() {
        let (tx, _rx) = mpsc::channel(4);
        let (uni_tx, _uni_rx) = mpsc::channel(1);
        let (bi_tx, _bi_rx) = mpsc::channel(1);
        let limits = Limits {
            max_open_streams: 1,
            ..Limits::default()
        };
        let inner = Rc::new(SessionInner::new(limits, tx, uni_tx, bi_tx));
        for index in 0..3 {
            let id = StreamId::new(index, false, StreamDir::Bi).unwrap();
            let mut recv = inner.clone().register_recv_stream(id);
            inner.queue_credit(id, 128);
            assert!(
                !inner.shutdown_started.load(Ordering::Acquire)
                    && !inner.closed.load(Ordering::Acquire),
                "retired credit must not close the session"
            );
            inner
                .handle_frame(Frame::Stream {
                    id,
                    data: Bytes::new(),
                    fin: true,
                })
                .await
                .unwrap();
            assert!(recv.read_chunk(1).await.unwrap().is_none());
        }
        assert_eq!(inner.pending_credit.borrow().len(), 1);
    }

    #[wasm_bindgen_test::wasm_bindgen_test(async)]
    async fn consumed_credit_survives_a_full_outbound_queue() {
        let (tx, _rx) = mpsc::channel(0);
        let (uni_tx, _uni_rx) = mpsc::channel(1);
        let (bi_tx, _bi_rx) = mpsc::channel(1);
        let limits = Limits {
            initial_stream_window: 64,
            stream_window_update_threshold: 32,
            ..Limits::default()
        };
        let inner = Rc::new(SessionInner::new(limits, tx, uni_tx, bi_tx));
        let id = StreamId::new(0, false, StreamDir::Bi).unwrap();
        let mut recv = inner.clone().register_recv_stream(id);
        inner
            .send_frame(Frame::MaxStreamData { id, max: 64 })
            .unwrap();
        for _ in 0..2 {
            inner
                .handle_frame(Frame::Stream {
                    id,
                    data: Bytes::from(vec![1; 32]),
                    fin: false,
                })
                .await
                .unwrap();
            assert_eq!(recv.read_chunk(32).await.unwrap().unwrap().len(), 32);
        }
        let Frame::MaxStreamData { max, .. } = inner.next_credit().await else {
            panic!("credit frame");
        };
        assert_eq!(
            max, 128,
            "updates coalesce and remain deliverable without another read"
        );
    }

    use super::*;
    use std::task::Waker;
    use wasm_bindgen_test::*;
    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn async_write_acks_owned_bytes_before_waiting_for_next_capacity() {
        let (tx, mut rx) = mpsc::channel(0);
        let (uni_tx, _uni_rx) = mpsc::channel(1);
        let (bi_tx, _bi_rx) = mpsc::channel(1);
        let inner = Rc::new(SessionInner::new(Limits::default(), tx, uni_tx, bi_tx));
        let id = StreamId::new(0, false, StreamDir::Bi).unwrap();
        let flow = inner.register_send_flow(id, 64).unwrap();
        let mut send = SendStream::new(id, inner, flow);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            Pin::new(&mut send).poll_write(&mut cx, b"previous-buffer"),
            Poll::Ready(Ok(15))
        ));
        assert!(
            Pin::new(&mut send)
                .poll_write(&mut cx, b"cancelled")
                .is_pending()
        );
        let frame = rx.try_recv().unwrap();
        let OutboundCmd::Frame(Frame::Stream { data, .. }) = frame else {
            panic!("data frame");
        };
        assert_eq!(data.as_ref(), b"previous-buffer");
        assert!(matches!(
            Pin::new(&mut send).poll_write(&mut cx, b"x"),
            Poll::Ready(Ok(1))
        ));
        let OutboundCmd::Frame(Frame::Stream { data, .. }) = rx.try_recv().unwrap() else {
            panic!("data frame");
        };
        assert_eq!(data.as_ref(), b"x");
    }
}
