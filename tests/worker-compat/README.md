# WASM dependency compatibility fixture

This unpublished standalone workspace compiles both public WebSocket facades
alongside workers-rs with its HTTP feature. The pinned SDK establishes a stable
regression case for dependency resolution; update the pin and lockfile deliberately.

```bash
cargo check --manifest-path tests/worker-compat/Cargo.toml --locked --target wasm32-unknown-unknown
```

The fixture requires Rust 1.91 because of the SDK. The library workspace retains
Rust 1.88. This fixture verifies a shared dependency graph and public types, not
Cloudflare runtime support for the browser connector.
