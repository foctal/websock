# Production Readiness Review

Reviewed on 2026-07-18. This file records concrete production-readiness work for
the workspace. Completed items describe fixes made as part of the review; open
items remain release blockers or follow-up work.

## P0 — Safety and resource bounds

- [x] Enforce mux receive-side flow control. A peer could send more than
  the advertised `MaxStreamData` value, allowing each stream to exceed its
  configured memory budget.
  - Completion: track cumulative received bytes per stream, reject overflow and
    credit violations as protocol errors, and test the boundary and violation.
- [x] Bound the browser WebSocket receive queue. `websock-wasm` used an
  unbounded channel, so a fast peer can exhaust browser memory when the
  application consumes messages slowly.
  - Completion: use a bounded queue and close the socket on overflow.
- [x] Validate every mux `Limits` value before allocating channels or starting
  tasks. Zero-capacity channels currently panic, and inconsistent byte/window
  settings can deadlock or bypass configured bounds.
  - Completion: invalid limits return `Error::Protocol` on both native and WASM
    implementations, with regression tests.
- [x] Do not grant mux send credit before the peer advertises it. Using the local
  receive-window configuration as peer credit breaks interoperability when
  peers use different limits and weakens flow control.
  - Completion: new send streams start with zero credit and wake only after a
    valid `MaxStreamData` frame.

## P1 — Protocol correctness and lifecycle

- [x] Treat `ConnectionClose` as terminal and wake all blocked stream writers.
  The previous implementation cleared maps but left the session open.
- [x] Reject non-monotonic peer stream IDs and cap locally opened streams. A
  peer could reuse stale IDs, while local streams could grow the flow
  map without respecting `max_open_streams`.
- [x] Remove stream state reliably. The native `try_lock` cleanup path could
  silently retain entries when the mutex is contended.
- [x] Reset streams whose application receive queues are full instead of
  blocking the entire native session behind one slow stream.
- [x] Preserve bidirectional send state after a receive FIN, separate reset and
  stop-sending semantics, retain final-frame data across partial reads, and keep
  a stream alive when one of several send-stream clones is dropped.
- [x] Reject stream-counter and reset/stop-code values that exceed the 62-bit
  mux encoding range instead of wrapping IDs or panicking in background tasks.
- [x] Add native end-to-end tests for connect/subprotocol negotiation,
  text/binary round trips, graceful close, oversized-message rejection, mux uni
  streams, differing flow-control windows, and invalid-limit rejection.
- [ ] Extend native end-to-end tests with split operation, mux bidirectional
  streams, malformed wire frames, reset/stop propagation, and TLS round trips.
- [ ] Add browser tests under `wasm-bindgen-test` for connect failure, queue
  overflow, close, split ownership, and mux parity.
- [x] Ensure WebSocket-level frame/message limits are applied in Tungstenite
  configuration before a complete oversized message is buffered.
- [x] Expose shared WebSocket message/frame/write-buffer limits and enforce
  browser receive queue, message-size, and `bufferedAmount` bounds.
- [x] Advertise only HTTP/1.1 in the default ALPN list because the current
  Tungstenite transport does not implement WebSocket over HTTP/2.
- [ ] Add explicit connection/session shutdown APIs with completion semantics
  and task cancellation. Dropping the last mux session handle should not leave
  detached tasks and a socket alive indefinitely.

## P2 — API, observability, and release engineering

- [ ] Preserve structured error sources and I/O error kinds instead of reducing
  all transport errors to strings. This is a semver-sensitive public API change.
- [ ] Expose negotiated subprotocol and close-frame details consistently on
  native and browser connections.
- [ ] Define and document the mux wire protocol, compatibility policy, error
  codes, stream state machine, and flow-control rules.
- [ ] Expand crate-level and public-item documentation and enable
  `#![warn(missing_docs)]` incrementally.
- [x] Add baseline CI for formatting, Clippy with warnings denied, tests,
  documentation, Linux/macOS/Windows, and `wasm32-unknown-unknown`.
- [ ] Extend CI with an explicit MSRV job, minimal feature-set builds, dependency
  policy checks, and security audits.
- [ ] Declare and test the MSRV, add changelog/release guidance, and verify
  package contents with `cargo package --list` for every published crate.
- [ ] Review dependency features and duplicate native/mux TLS implementations to
  reduce compile time, binary size, and maintenance drift.
- [ ] Add fuzz targets for varint/frame decoding and stateful mux frame
  sequences, plus long-running concurrency and backpressure tests.
