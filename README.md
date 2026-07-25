[crates-badge]: https://img.shields.io/crates/v/websock.svg
[crates-url]: https://crates.io/crates/websock
[doc-url]: https://docs.rs/websock/latest/websock
[license-badge]: https://img.shields.io/crates/l/websock.svg
[examples-url]: https://github.com/foctal/websock/tree/main/websock/examples

# websock [![Crates.io][crates-badge]][crates-url] ![License][license-badge]

A minimal WebSocket library for native and WebAssembly.

## Workspace crates

- `websock`: top-level facade that selects native (`websock-tungstenite`) or browser (`websock-wasm`) transport.
- `websock-proto`: shared protocol types and error model.
- `websock-tungstenite`: native transport powered by `tokio` + `tungstenite` (optional TLS via `rustls`).
- `websock-wasm`: browser transport based on the WebSocket API.
- `websock-mux`: top-level facade for multiplexed streams over WebSocket.
- `websock-mux-proto`: frame and varint primitives for the multiplexing layer.
- `websock-tungstenite-mux`: native multiplexed transport.
- `websock-wasm-mux`: browser multiplexed transport.
- `websock-wasm-demo`: small browser demo application.

## Quick start

```toml
[dependencies]
websock = "0.5"
```

API documentation is available on [docs.rs][doc-url].  

If you need transport-specific features, depend on one of the transport crates directly.

### Native
See [examples][examples-url].

### WebAssembly

The `websock-wasm-demo` crate includes a small browser app that connects to an echo server.

## Resource limits

WebSocket clients and servers use conservative message, frame, and write-buffer
limits by default. Customize them with `WebSocketLimits`:

```rust
use websock::{ClientBuilder, WebSocketLimits};

let client = ClientBuilder::new()
    .with_limits(WebSocketLimits {
        max_message_size: 2 * 1024 * 1024,
        max_frame_size: 512 * 1024,
        max_write_buffer_size: 2 * 1024 * 1024,
    })
    .build();
```

The mux transports additionally expose `Limits` for stream counts, queue
capacities, batching, and per-stream flow-control windows. Invalid or
inconsistent limits return a protocol error rather than panicking.

The multiplexing wire format, stream lifecycle, flow control, compatibility
policy, and error codes are specified in [docs/mux-protocol.md](docs/mux-protocol.md).

## Benchmarking

Criterion benchmarks are available for `websock-mux-proto`.

```bash
cargo bench -p websock-mux-proto
```
