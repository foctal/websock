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
websock = "0.4"
```

API documentation is available on [docs.rs][doc-url].  

If you need transport-specific features, depend on one of the transport crates directly.

### Native
See [examples][examples-url].

### WebAssembly

The `websock-wasm-demo` crate includes a small browser app that connects to an echo server.

## Benchmarking

Criterion benchmarks are available for `websock-mux-proto`.

```bash
cargo bench -p websock-mux-proto
```
