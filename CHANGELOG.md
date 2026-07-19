# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Explicit mux session shutdown with task-completion semantics.
- Negotiated subprotocol and received close-frame metadata accessors.
- Native end-to-end coverage for split connections, bidirectional mux streams,
  malformed frames, reset/stop propagation, TLS, and session-handle cleanup.
- Browser integration coverage for connection failures, receive-queue overflow,
  close metadata, split ownership, and mux parity.
- A versioned mux wire-protocol specification.

### Changed

- The shared error API now retains native `std::io::Error` values and structured
  TLS and WebSocket transport sources instead of storing their display strings.
  This changes the public payload of `Error::Io` and `Error::Tls` and adds
  `Error::Transport`.
- The mux TLS helpers now reuse the native transport implementation.
- PEM parsing now uses `rustls-pki-types` directly instead of the unmaintained
  `rustls-pemfile` crate.
- Default TLS ALPN configuration advertises only HTTP/1.1.

### Fixed

- Non-boolean mux stream FIN fields are rejected as malformed.
