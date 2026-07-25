# WebSocket Multiplexing Protocol

This document specifies version 1 of the `websock` multiplexing protocol. A
connection using this protocol MUST negotiate the WebSocket subprotocol
`websock-mux-1`. Peers MUST use binary WebSocket messages; text messages are a
protocol error.

## Compatibility

The subprotocol name carries the wire-protocol major version. Implementations
with incompatible framing or stream semantics MUST use a different
subprotocol. Additive behavior that old peers can safely ignore may remain
within version 1, but version 1 currently defines no ignorable frame types.
Unknown frame types are therefore a protocol error.

One binary WebSocket message may contain one or more complete mux frames.
Frames MUST NOT span WebSocket messages. All integers use the QUIC
variable-length integer encoding and are limited to the inclusive range
`0..2^62-1`.

## Stream identifiers

A stream identifier encodes three fields:

```text
stream_id = stream_counter * 4 | direction_bit * 2 | initiator_bit
```

- `initiator_bit`: client is `0`; server is `1`.
- `direction_bit`: bidirectional is `0`; unidirectional is `1`.
- `stream_counter`: a monotonically increasing counter maintained separately
  for each direction.

A peer MUST open its streams with monotonically increasing identifiers. An
identifier with the wrong initiator or direction is a protocol error.

## Frames

Each frame starts with a varint tag followed by the listed fields:

| Tag | Frame | Fields |
| ---: | --- | --- |
| 0 | `OpenUni` | `stream_id` |
| 1 | `OpenBi` | `stream_id` |
| 2 | `Stream` | `stream_id`, `fin`, `length`, `length` data bytes |
| 3 | `ResetStream` | `stream_id`, application error code |
| 4 | `StopSending` | `stream_id`, application error code |
| 5 | `ConnectionClose` | connection error code, UTF-8 reason length, reason bytes |
| 6 | `MaxStreamData` | `stream_id`, cumulative byte limit |

The `fin` field MUST be `0` or `1`. Length-prefixed data MUST be fully present
in the containing WebSocket message. A malformed field, invalid UTF-8 reason,
unknown tag, or truncated frame is a protocol error.

## Stream state

`OpenUni` creates a receive stream for the peer. `OpenBi` creates both a send
and a receive side. Stream data may be sent only after the corresponding open
frame and MUST NOT be sent after FIN or reset.

A `Stream` frame with `fin = 1` closes only the sender's direction. Its data,
if any, remains readable before end-of-stream is reported. The other direction
of a bidirectional stream remains usable.

`ResetStream` abruptly terminates the sender's direction. `StopSending` asks
the peer to stop its sender direction; the peer then treats that send side as
closed. Application error codes are opaque to the protocol and MUST fit in a
varint.

## Flow control

New send streams start with zero credit. A receiver grants credit with
`MaxStreamData`, whose `max` field is the cumulative number of stream-data
bytes the sender may transmit. Values MUST be monotonic. A sender MUST block
when it has exhausted the latest advertised limit.

The receiver counts only `Stream` payload bytes, not frame overhead. Receiving
bytes beyond the advertised cumulative limit is a connection-level flow
control error. Implementations issue further cumulative credit as the
application consumes buffered bytes.

## Connection closure and error codes

`ConnectionClose` is terminal. After receiving it, a peer closes all streams,
wakes blocked operations, and closes the WebSocket session.

The following connection error codes are defined:

| Code | Meaning |
| ---: | --- |
| 0 | Graceful or unspecified closure |
| 1 | Malformed frame or stream state violation |
| 2 | Size or flow-control violation |
| 3 | Resource limit exceeded |

Other codes are reserved for future versions. WebSocket close frames remain
part of the underlying transport; either a mux `ConnectionClose` or an
underlying WebSocket closure terminates the session.
