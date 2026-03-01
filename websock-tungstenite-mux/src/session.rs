use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::task::{Context, Poll};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::future::poll_fn;
use futures_util::task::AtomicWaker;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::tungstenite;
use tokio_util::sync::PollSender;
use websock_proto::{Error, Result};

use websock_mux_proto::stream::{Frame, StreamDir, StreamId};

const MAX_WRITE_CHUNK: usize = 16 * 1024;

/// Session limits to prevent unbounded buffering / DoS.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Maximum size of a single WebSocket binary message accepted by the inbound task.
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
            // Safe defaults for a WebSocket fallback transport.
            max_ws_message_size: 1024 * 1024,      // 1 MiB
            max_stream_data_per_frame: 256 * 1024, // 256 KiB
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
    inner: Arc<SessionInner>,
    accept_uni: Arc<Mutex<mpsc::Receiver<RecvStream>>>,
    accept_bi: Arc<Mutex<mpsc::Receiver<(SendStream, RecvStream)>>>,
}

impl Session {
    pub(crate) fn new<S>(
        stream: tokio_tungstenite::WebSocketStream<S>,
        is_server: bool,
        limits: Limits,
    ) -> Result<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (outbound_tx, outbound_rx) = mpsc::channel(limits.outbound_queue_len);
        let (accept_uni_tx, accept_uni_rx) = mpsc::channel(limits.accept_uni_queue_len);
        let (accept_bi_tx, accept_bi_rx) = mpsc::channel(limits.accept_bi_queue_len);

        let inner = Arc::new(SessionInner::new(
            is_server,
            limits,
            outbound_tx,
            accept_uni_tx,
            accept_bi_tx,
        ));

        let session = Self {
            inner: inner.clone(),
            accept_uni: Arc::new(Mutex::new(accept_uni_rx)),
            accept_bi: Arc::new(Mutex::new(accept_bi_rx)),
        };

        inner.spawn_tasks(stream, outbound_rx);
        Ok(session)
    }

    pub async fn open_uni(&self) -> Result<SendStream> {
        let id = self.inner.next_stream_id(StreamDir::Uni)?;
        let flow = self
            .inner
            .register_send_flow(id, self.inner.limits.initial_stream_window as u64)
            .await;
        self.inner.send_frame(Frame::OpenUni { id }).await?;
        Ok(SendStream::new(id, self.inner.clone(), flow))
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream)> {
        let id = self.inner.next_stream_id(StreamDir::Bi)?;
        let flow = self
            .inner
            .register_send_flow(id, self.inner.limits.initial_stream_window as u64)
            .await;
        let recv = self.inner.register_recv_stream(id).await;
        self.inner.send_frame(Frame::OpenBi { id }).await?;
        self.inner.send_initial_credit(id).await?;
        Ok((SendStream::new(id, self.inner.clone(), flow), recv))
    }

    pub async fn accept_uni(&self) -> Result<RecvStream> {
        let mut rx = self.accept_uni.lock().await;
        rx.recv().await.ok_or(Error::Closed)
    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream)> {
        let mut rx = self.accept_bi.lock().await;
        rx.recv().await.ok_or(Error::Closed)
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
    session: Arc<SessionInner>,
    finished: Arc<AtomicBool>,
    flow: Arc<SendFlowState>,
    outbound_tx: mpsc::Sender<OutboundCmd>,
    outbound: PollSender<OutboundCmd>,
    flush_waiter: Option<oneshot::Receiver<Result<()>>>,
    fin_queued: bool,
}

impl SendStream {
    fn new(id: StreamId, session: Arc<SessionInner>, flow: Arc<SendFlowState>) -> Self {
        let outbound_tx = session.outbound_tx.clone();
        Self {
            id,
            session,
            finished: Arc::new(AtomicBool::new(false)),
            flow,
            outbound: PollSender::new(outbound_tx.clone()),
            outbound_tx,
            flush_waiter: None,
            fin_queued: false,
        }
    }

    pub async fn write(&self, data: &[u8]) -> Result<()> {
        self.write_buf(Bytes::copy_from_slice(data)).await
    }

    pub async fn write_buf(&self, data: Bytes) -> Result<()> {
        if self.finished.load(Ordering::SeqCst) || self.session.is_closed() {
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
            let grant = poll_fn(|cx| {
                self.flow.waker.register(cx.waker());
                let n = self.flow.try_reserve(wanted);
                if n == 0 {
                    if self.finished.load(Ordering::SeqCst) || self.session.is_closed() {
                        Poll::Ready(Err(Error::Closed))
                    } else {
                        Poll::Pending
                    }
                } else {
                    Poll::Ready(Ok(n))
                }
            })
            .await?;

            let chunk = data.slice(offset..offset + grant);
            if let Err(err) = self
                .session
                .send_frame(Frame::Stream {
                    id: self.id,
                    data: chunk,
                    fin: false,
                })
                .await
            {
                self.flow.release(grant);
                return Err(err);
            }
            offset += grant;
        }
        Ok(())
    }

    pub async fn write_all(&self, data: &[u8]) -> Result<()> {
        self.write(data).await
    }

    pub async fn finish(&self) -> Result<()> {
        if self
            .finished
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.session
                .send_frame(Frame::Stream {
                    id: self.id,
                    data: Bytes::new(),
                    fin: true,
                })
                .await?;
            self.session.remove_send_flow(self.id);
        }
        Ok(())
    }

    pub async fn reset(&self, code: u64) -> Result<()> {
        self.finished.store(true, Ordering::SeqCst);
        self.session.remove_send_flow(self.id);
        self.session
            .send_frame(Frame::ResetStream { id: self.id, code })
            .await
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
            outbound_tx: self.outbound_tx.clone(),
            outbound: PollSender::new(self.outbound_tx.clone()),
            flush_waiter: None,
            fin_queued: false,
        }
    }
}

impl AsyncWrite for SendStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.finished.load(Ordering::SeqCst) || this.session.is_closed() {
            return Poll::Ready(Err(io_closed()));
        }

        this.flow.waker.register(cx.waker());
        let wanted_len = buf
            .len()
            .min(MAX_WRITE_CHUNK)
            .min(this.session.limits.max_stream_data_per_frame);
        if wanted_len == 0 {
            return Poll::Ready(Err(io_invalid_input("stream frame payload limit is zero")));
        }
        let chunk_len = this.flow.try_reserve(wanted_len);
        if chunk_len == 0 {
            return Poll::Pending;
        }

        match this.outbound.poll_reserve(cx) {
            Poll::Pending => {
                this.flow.release(chunk_len);
                Poll::Pending
            }
            Poll::Ready(Err(_)) => {
                this.flow.release(chunk_len);
                Poll::Ready(Err(io_closed()))
            }
            Poll::Ready(Ok(())) => {
                let frame = Frame::Stream {
                    id: this.id,
                    data: Bytes::copy_from_slice(&buf[..chunk_len]),
                    fin: false,
                };
                match this.outbound.send_item(OutboundCmd::Frame(frame)) {
                    Ok(()) => Poll::Ready(Ok(chunk_len)),
                    Err(_) => {
                        this.flow.release(chunk_len);
                        Poll::Ready(Err(io_closed()))
                    }
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        loop {
            if let Some(waiter) = this.flush_waiter.as_mut() {
                match Pin::new(waiter).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(Ok(()))) => {
                        this.flush_waiter = None;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Ok(Err(err))) => {
                        this.flush_waiter = None;
                        return Poll::Ready(Err(io_from_error(err)));
                    }
                    Poll::Ready(Err(_)) => {
                        this.flush_waiter = None;
                        return Poll::Ready(Err(io_closed()));
                    }
                }
            }

            let (ack_tx, ack_rx) = oneshot::channel();
            match this.outbound.poll_reserve(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Ready(Err(io_closed())),
                Poll::Ready(Ok(())) => {
                    if this
                        .outbound
                        .send_item(OutboundCmd::Flush { ack: ack_tx })
                        .is_err()
                    {
                        return Poll::Ready(Err(io_closed()));
                    }
                    this.flush_waiter = Some(ack_rx);
                }
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        if !this.finished.load(Ordering::SeqCst) && !this.fin_queued {
            match this.outbound.poll_reserve(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Ready(Err(io_closed())),
                Poll::Ready(Ok(())) => {
                    let frame = Frame::Stream {
                        id: this.id,
                        data: Bytes::new(),
                        fin: true,
                    };
                    if this.outbound.send_item(OutboundCmd::Frame(frame)).is_err() {
                        return Poll::Ready(Err(io_closed()));
                    }
                    this.fin_queued = true;
                    this.finished.store(true, Ordering::SeqCst);
                    this.session.remove_send_flow(this.id);
                }
            }
        }

        Pin::new(this).poll_flush(cx)
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
    session: Arc<SessionInner>,
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
        session: Arc<SessionInner>,
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

    pub async fn read_chunk_internal(&mut self) -> Result<Option<Bytes>> {
        match self.receiver.recv().await {
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
        match self.receiver.recv().await {
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

    pub async fn stop(&self, code: u64) -> Result<()> {
        self.session
            .send_frame(Frame::StopSending { id: self.id, code })
            .await
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

impl AsyncRead for RecvStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        if !this.pending.is_empty() {
            let amt = this.pending.len().min(buf.remaining());
            buf.put_slice(&this.pending[..amt]);
            this.pending = this.pending.slice(amt..);
            this.on_bytes_consumed(amt);
            return Poll::Ready(Ok(()));
        }

        if this.finished {
            return Poll::Ready(Ok(()));
        }

        loop {
            match Pin::new(&mut this.receiver).poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.finished = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(event)) => {
                    if event.fin {
                        this.finished = true;
                    }
                    if event.data.is_empty() {
                        if event.fin {
                            return Poll::Ready(Ok(()));
                        }
                        continue;
                    }

                    let amt = event.data.len().min(buf.remaining());
                    buf.put_slice(&event.data[..amt]);
                    if amt < event.data.len() {
                        this.pending = event.data.slice(amt..);
                    }
                    this.on_bytes_consumed(amt);
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

pub(crate) enum OutboundCmd {
    Frame(Frame),
    Ws(tungstenite::Message),
    Flush { ack: oneshot::Sender<Result<()>> },
}

pub(crate) struct SessionInner {
    is_server: bool,
    limits: Limits,
    outbound_tx: mpsc::Sender<OutboundCmd>,
    accept_uni_tx: Mutex<Option<mpsc::Sender<RecvStream>>>,
    accept_bi_tx: Mutex<Option<mpsc::Sender<(SendStream, RecvStream)>>>,
    streams: Mutex<HashMap<StreamId, mpsc::Sender<RecvEvent>>>,
    send_flows: Mutex<HashMap<StreamId, Arc<SendFlowState>>>,
    next_uni: AtomicU64,
    next_bi: AtomicU64,
    closed: AtomicBool,
}

impl SessionInner {
    pub(crate) fn new(
        is_server: bool,
        limits: Limits,
        outbound_tx: mpsc::Sender<OutboundCmd>,
        accept_uni_tx: mpsc::Sender<RecvStream>,
        accept_bi_tx: mpsc::Sender<(SendStream, RecvStream)>,
    ) -> Self {
        Self {
            is_server,
            limits,
            outbound_tx,
            accept_uni_tx: Mutex::new(Some(accept_uni_tx)),
            accept_bi_tx: Mutex::new(Some(accept_bi_tx)),
            streams: Mutex::new(HashMap::new()),
            send_flows: Mutex::new(HashMap::new()),
            next_uni: AtomicU64::new(0),
            next_bi: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }
    }

    fn spawn_tasks<S>(
        self: Arc<Self>,
        stream: tokio_tungstenite::WebSocketStream<S>,
        mut outbound_rx: mpsc::Receiver<OutboundCmd>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut ws_sink, mut ws_stream) = stream.split();

        let inbound = self.clone();
        tokio::spawn(async move {
            while let Some(msg) = ws_stream.next().await {
                let msg = match msg {
                    Ok(m) => m,
                    Err(_) => break,
                };

                match msg {
                    tungstenite::Message::Binary(data) => {
                        if data.len() > inbound.limits.max_ws_message_size {
                            let _ = inbound.protocol_error(2, "ws message too large").await;
                            break;
                        }
                        let mut cursor = &data[..];
                        let mut frame_error = false;
                        while cursor.has_remaining() {
                            let frame = match Frame::decode(&mut cursor) {
                                Ok(f) => f,
                                Err(_) => {
                                    let _ = inbound.protocol_error(1, "invalid frame").await;
                                    frame_error = true;
                                    break;
                                }
                            };
                            if inbound.handle_frame(frame).await.is_err() {
                                frame_error = true;
                                break;
                            }
                        }
                        if frame_error {
                            break;
                        }
                    }
                    tungstenite::Message::Ping(p) => {
                        let _ = inbound
                            .outbound_tx
                            .try_send(OutboundCmd::Ws(tungstenite::Message::Pong(p)));
                    }
                    tungstenite::Message::Close(_) => break,
                    _ => {}
                }
            }

            inbound.close_all().await;
        });

        let outbound = self.clone();
        tokio::spawn(async move {
            async fn flush_batch<S>(
                ws_sink: &mut futures_util::stream::SplitSink<
                    tokio_tungstenite::WebSocketStream<S>,
                    tungstenite::Message,
                >,
                batch: &mut BytesMut,
            ) -> std::result::Result<(), tungstenite::Error>
            where
                S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
            {
                if batch.is_empty() {
                    return Ok(());
                }
                let payload = std::mem::take(batch).freeze();
                ws_sink.send(tungstenite::Message::Binary(payload)).await
            }

            let mut batch = BytesMut::new();
            let mut batch_frames = 0usize;

            while let Some(cmd) = outbound_rx.recv().await {
                match cmd {
                    OutboundCmd::Frame(frame) => {
                        let encoded = frame.encode().freeze();
                        let frame_len = encoded.len();

                        let max_bytes = outbound
                            .limits
                            .max_batch_bytes
                            .min(outbound.limits.max_ws_message_size);
                        if !batch.is_empty() && batch.len() + frame_len > max_bytes {
                            if flush_batch(&mut ws_sink, &mut batch).await.is_err() {
                                break;
                            }
                            batch_frames = 0;
                        }
                        batch.extend_from_slice(&encoded);
                        batch_frames += 1;

                        if batch_frames >= outbound.limits.max_batch_frames
                            || batch.len() >= max_bytes
                            || outbound_rx.is_empty()
                        {
                            if flush_batch(&mut ws_sink, &mut batch).await.is_err() {
                                break;
                            }
                            batch_frames = 0;
                        }
                    }
                    OutboundCmd::Ws(msg) => {
                        if flush_batch(&mut ws_sink, &mut batch).await.is_err() {
                            break;
                        }
                        batch_frames = 0;
                        if ws_sink.send(msg).await.is_err() {
                            break;
                        }
                    }
                    OutboundCmd::Flush { ack } => {
                        if flush_batch(&mut ws_sink, &mut batch).await.is_err() {
                            let _ = ack.send(Err(Error::Closed));
                            break;
                        }
                        batch_frames = 0;
                        let flush_res = ws_sink.flush().await.map_err(map_tungstenite_err);
                        let _ = ack.send(flush_res);
                    }
                }
            }
            outbound.close_all().await;
        });
    }

    pub(crate) async fn handle_frame(self: &Arc<Self>, frame: Frame) -> Result<()> {
        match frame {
            Frame::OpenUni { id } => {
                if id.dir() != StreamDir::Uni {
                    return self
                        .protocol_error(1, "OpenUni with non-uni StreamId")
                        .await;
                }
                if id.initiator_is_server() != self.peer_is_server() {
                    return self.protocol_error(1, "OpenUni with wrong initiator").await;
                }

                let recv = match self.try_register_inbound_recv_stream(id).await {
                    Ok(recv) => recv,
                    Err((code, reason)) => return self.protocol_error(code, reason).await,
                };

                let tx = { self.accept_uni_tx.lock().await.clone() };
                if let Some(tx) = tx {
                    match tx.try_send(recv) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            // Application is not accepting inbound streams fast enough.
                            // Reset this stream and keep the connection alive.
                            let mut streams = self.streams.lock().await;
                            streams.remove(&id);
                            let _ = self.try_send_frame(Frame::ResetStream { id, code: 3 });
                            return Ok(());
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => return Err(Error::Closed),
                    }
                } else {
                    return Err(Error::Closed);
                }
            }
            Frame::OpenBi { id } => {
                if id.dir() != StreamDir::Bi {
                    return self.protocol_error(1, "OpenBi with non-bi StreamId").await;
                }
                if id.initiator_is_server() != self.peer_is_server() {
                    return self.protocol_error(1, "OpenBi with wrong initiator").await;
                }

                let recv = match self.try_register_inbound_recv_stream(id).await {
                    Ok(recv) => recv,
                    Err((code, reason)) => return self.protocol_error(code, reason).await,
                };
                let flow = self
                    .register_send_flow(id, self.limits.initial_stream_window as u64)
                    .await;
                let send = SendStream::new(id, self.clone(), flow);

                let tx = { self.accept_bi_tx.lock().await.clone() };
                if let Some(tx) = tx {
                    match tx.try_send((send, recv)) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            let mut streams = self.streams.lock().await;
                            streams.remove(&id);
                            let _ = self.try_send_frame(Frame::ResetStream { id, code: 3 });
                            return Ok(());
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => return Err(Error::Closed),
                    }
                } else {
                    return Err(Error::Closed);
                }
            }
            Frame::Stream { id, data, fin } => {
                if data.len() > self.limits.max_stream_data_per_frame {
                    return self.protocol_error(2, "stream data too large").await;
                }
                let tx = {
                    let streams = self.streams.lock().await;
                    streams.get(&id).cloned()
                };
                let Some(tx) = tx else {
                    return self
                        .protocol_error(1, "Stream data on unknown stream")
                        .await;
                };
                if tx.send(RecvEvent { data, fin }).await.is_err() {
                    let mut streams = self.streams.lock().await;
                    streams.remove(&id);
                    return Ok(());
                }

                if fin {
                    self.remove_send_flow(id);
                    let mut streams = self.streams.lock().await;
                    streams.remove(&id);
                }
            }
            Frame::ResetStream { id, .. } | Frame::StopSending { id, .. } => {
                let mut streams = self.streams.lock().await;
                let removed_recv = streams.remove(&id).is_some();
                drop(streams);
                let mut send_flows = self.send_flows.lock().await;
                let removed_send = send_flows.remove(&id).is_some();
                if !removed_recv && !removed_send {
                    return self.protocol_error(1, "reset/stop on unknown stream").await;
                }
            }
            Frame::MaxStreamData { id, max } => {
                let flows = self.send_flows.lock().await;
                if let Some(flow) = flows.get(&id) {
                    flow.update_max(max);
                }
            }
            Frame::ConnectionClose { .. } => {
                let mut streams = self.streams.lock().await;
                streams.clear();
                let mut send_flows = self.send_flows.lock().await;
                send_flows.clear();
            }
        }
        Ok(())
    }

    async fn try_register_inbound_recv_stream(
        self: &Arc<Self>,
        id: StreamId,
    ) -> std::result::Result<RecvStream, (u64, &'static str)> {
        let (tx, rx) = mpsc::channel(self.limits.recv_event_queue_len);
        let mut streams = self.streams.lock().await;
        if streams.len() >= self.limits.max_open_streams {
            return Err((3, "too many open streams"));
        }
        if streams.contains_key(&id) {
            return Err((1, "duplicate stream open"));
        }
        streams.insert(id, tx);
        drop(streams);

        let initial_window = self.limits.initial_stream_window as u64;
        let recv = RecvStream::new(
            id,
            self.clone(),
            rx,
            initial_window,
            self.limits.stream_window_update_threshold as u64,
        );
        self.send_frame(Frame::MaxStreamData {
            id,
            max: initial_window,
        })
        .await
        .map_err(|_| (2, "failed to send initial stream credit"))?;
        Ok(recv)
    }

    pub(crate) async fn register_recv_stream(self: &Arc<Self>, id: StreamId) -> RecvStream {
        let (tx, rx) = mpsc::channel(self.limits.recv_event_queue_len);
        let mut streams = self.streams.lock().await;
        streams.insert(id, tx);
        drop(streams);
        let initial_window = self.limits.initial_stream_window as u64;
        RecvStream::new(
            id,
            self.clone(),
            rx,
            initial_window,
            self.limits.stream_window_update_threshold as u64,
        )
    }

    async fn send_initial_credit(&self, id: StreamId) -> Result<()> {
        self.send_frame(Frame::MaxStreamData {
            id,
            max: self.limits.initial_stream_window as u64,
        })
        .await
    }

    pub(crate) fn next_stream_id(&self, dir: StreamDir) -> Result<StreamId> {
        let counter = match dir {
            StreamDir::Uni => self.next_uni.fetch_add(1, Ordering::SeqCst),
            StreamDir::Bi => self.next_bi.fetch_add(1, Ordering::SeqCst),
        };
        StreamId::new(counter, self.is_server, dir).map_err(|e| Error::StreamId(e.to_string()))
    }

    async fn register_send_flow(&self, id: StreamId, initial_max: u64) -> Arc<SendFlowState> {
        let flow = Arc::new(SendFlowState::new(initial_max));
        let mut send_flows = self.send_flows.lock().await;
        send_flows.insert(id, flow.clone());
        flow
    }

    fn remove_send_flow(&self, id: StreamId) {
        if let Ok(mut send_flows) = self.send_flows.try_lock() {
            send_flows.remove(&id);
        }
    }

    pub(crate) async fn send_frame(&self, frame: Frame) -> Result<()> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        self.outbound_tx
            .send(OutboundCmd::Frame(frame))
            .await
            .map_err(|_| Error::Closed)
    }

    pub(crate) fn try_send_frame(&self, frame: Frame) -> Result<()> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        self.outbound_tx
            .try_send(OutboundCmd::Frame(frame))
            .map_err(|_| Error::Closed)
    }

    pub(crate) async fn close_all(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        // Close accept channels
        {
            let mut tx = self.accept_uni_tx.lock().await;
            tx.take();
        }
        {
            let mut tx = self.accept_bi_tx.lock().await;
            tx.take();
        }
        // Close existing streams
        {
            let mut streams = self.streams.lock().await;
            streams.clear();
        }
        {
            let mut send_flows = self.send_flows.lock().await;
            send_flows.clear();
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn protocol_error(self: &Arc<Self>, code: u64, reason: impl Into<String>) -> Result<()> {
        let reason = reason.into();

        // Notify the peer (it's okay if sending fails...)
        let _ = self.try_send_frame(Frame::ConnectionClose {
            code,
            reason: reason.clone(),
        });

        self.close_all().await;
        Err(Error::Protocol(reason))
    }

    fn peer_is_server(&self) -> bool {
        !self.is_server
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
        Box::pin(async move { RecvStream::stop(self, code).await })
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
        Box::pin(async move { Session::open_uni(self).await })
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
        Box::pin(async move { Session::open_bi(self).await })
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

fn io_from_error(err: Error) -> io::Error {
    match err {
        Error::Closed => io_closed(),
        Error::Io(message) => io::Error::other(message),
        Error::Protocol(message)
        | Error::InvalidUrl(message)
        | Error::Tls(message)
        | Error::StreamId(message)
        | Error::Unsupported(message)
        | Error::FrameDecode(message)
        | Error::Other(message) => io::Error::other(message),
    }
}

/// Map tungstenite errors into the shared error type.
pub(crate) fn map_tungstenite_err(e: tungstenite::Error) -> Error {
    use tungstenite::Error as E;
    match e {
        E::ConnectionClosed | E::AlreadyClosed => Error::Closed,
        E::Io(io) => Error::Io(io.to_string()),
        E::Tls(tls) => Error::Tls(tls.to_string()),
        E::Url(url) => Error::InvalidUrl(url.to_string()),
        E::Protocol(err) => Error::Protocol(err.to_string()),
        E::Utf8(err) => Error::Protocol(err),
        E::Capacity(err) => Error::Protocol(err.to_string()),
        E::HttpFormat(err) => Error::Protocol(err.to_string()),
        other => Error::Other(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::poll_fn;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::Poll;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn async_write_chunks_and_async_read_uses_pending_buffer() {
        let limits = Limits::default();
        let (outbound_tx, mut outbound_rx) = mpsc::channel(8);
        let (accept_uni_tx, _accept_uni_rx) = mpsc::channel(8);
        let (accept_bi_tx, _accept_bi_rx) = mpsc::channel(8);
        let inner = Arc::new(SessionInner::new(
            false,
            limits,
            outbound_tx,
            accept_uni_tx,
            accept_bi_tx,
        ));
        let id = StreamId::new(0, false, StreamDir::Bi).expect("stream id");
        let flow = inner.register_send_flow(id, u64::MAX).await;
        let mut send = SendStream::new(id, inner.clone(), flow);
        let mut recv = inner.register_recv_stream(id).await;

        let payload = vec![b'a'; MAX_WRITE_CHUNK + 37];
        let n1 = AsyncWriteExt::write(&mut send, &payload)
            .await
            .expect("write 1");
        assert_eq!(n1, MAX_WRITE_CHUNK);

        let first_frame = loop {
            let Some(cmd) = outbound_rx.recv().await else {
                panic!("first frame missing");
            };
            if let OutboundCmd::Frame(frame) = cmd
                && let Frame::Stream { .. } = frame
            {
                break frame;
            }
        };
        let Frame::Stream {
            id: first_id,
            data: first_data,
            fin: first_fin,
        } = first_frame
        else {
            panic!("unexpected first frame");
        };
        assert_eq!(first_id, id);
        assert_eq!(first_data.len(), MAX_WRITE_CHUNK);
        assert!(!first_fin);

        let n2 = AsyncWriteExt::write(&mut send, &payload[n1..])
            .await
            .expect("write 2");
        assert_eq!(n2, 37);

        let second_frame = loop {
            let Some(cmd) = outbound_rx.recv().await else {
                panic!("second frame missing");
            };
            if let OutboundCmd::Frame(frame) = cmd
                && let Frame::Stream { .. } = frame
            {
                break frame;
            }
        };
        let Frame::Stream {
            id: second_id,
            data: second_data,
            fin: second_fin,
        } = second_frame
        else {
            panic!("unexpected second frame");
        };
        assert_eq!(second_id, id);
        assert_eq!(second_data.len(), 37);
        assert!(!second_fin);

        inner
            .handle_frame(Frame::Stream {
                id,
                data: Bytes::from_static(b"hello"),
                fin: false,
            })
            .await
            .expect("stream frame");

        let mut read_buf = [0u8; 2];
        let r1 = AsyncReadExt::read(&mut recv, &mut read_buf)
            .await
            .expect("read 1");
        assert_eq!(r1, 2);
        assert_eq!(&read_buf, b"he");

        let mut read_buf2 = [0u8; 3];
        let r2 = AsyncReadExt::read(&mut recv, &mut read_buf2)
            .await
            .expect("read 2");
        assert_eq!(r2, 3);
        assert_eq!(&read_buf2, b"llo");
    }

    #[tokio::test]
    async fn poll_write_returns_pending_when_outbound_channel_is_full() {
        let limits = Limits::default();
        let (outbound_tx, mut outbound_rx) = mpsc::channel(1);
        let (accept_uni_tx, _accept_uni_rx) = mpsc::channel(1);
        let (accept_bi_tx, _accept_bi_rx) = mpsc::channel(1);
        let inner = Arc::new(SessionInner::new(
            false,
            limits,
            outbound_tx,
            accept_uni_tx,
            accept_bi_tx,
        ));
        let id = StreamId::new(0, false, StreamDir::Bi).expect("stream id");
        let flow = inner.register_send_flow(id, u64::MAX).await;
        let mut send = SendStream::new(id, inner, flow);

        let first = AsyncWriteExt::write(&mut send, b"x")
            .await
            .expect("first write");
        assert_eq!(first, 1);

        let pending = poll_fn(|cx| match Pin::new(&mut send).poll_write(cx, b"y") {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(Ok(_)) => Poll::Ready(false),
            Poll::Ready(Err(_)) => Poll::Ready(false),
        })
        .await;
        assert!(pending, "second write should wait for capacity");

        let _ = outbound_rx.recv().await;
        let second_ok = AsyncWriteExt::write(&mut send, b"y")
            .await
            .expect("second write result");
        assert_eq!(second_ok, 1);
    }
}
