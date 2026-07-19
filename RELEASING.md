# Release Guide

Release and publishing operations are intentionally manual.

1. Confirm that `CHANGELOG.md` describes all user-visible changes and move the
   Unreleased entries under the new version and date.
2. Run `cargo fmt --all -- --check`.
3. Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
4. Run `cargo test --workspace --all-targets`.
5. Run `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS=-Dwarnings`.
6. Check the WASM packages for `wasm32-unknown-unknown`.
7. Run `cargo package --list -p <crate>` for every publishable crate and inspect
   each file list. Then run `cargo package -p <crate>` in dependency order.
8. Update versions and internal dependency requirements together.
9. Create the release commit and tag manually.
10. Publish crates in dependency order, then create the GitHub Release manually.

The publishable dependency order is:

1. `websock-proto`
2. `websock-mux-proto`
3. `websock-tungstenite` and `websock-wasm`
4. `websock-tungstenite-mux` and `websock-wasm-mux`
5. `websock` and `websock-mux`

`websock-wasm-demo` is not published.

For the manual package-content inspection in step 7, run:

```text
cargo package --list -p websock-proto
cargo package --list -p websock-mux-proto
cargo package --list -p websock-tungstenite
cargo package --list -p websock-wasm
cargo package --list -p websock-tungstenite-mux
cargo package --list -p websock-wasm-mux
cargo package --list -p websock
cargo package --list -p websock-mux
```
