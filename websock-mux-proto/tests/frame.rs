use bytes::{Bytes, BytesMut};
use std::io::Cursor;

use websock_mux_proto::{Frame, FrameDecodeError, StreamDir, StreamId, VarInt};

fn roundtrip(frame: &Frame) {
    let encoded = frame.encode().freeze();
    let mut cur = Cursor::new(encoded);
    let decoded = Frame::decode(&mut cur).unwrap();
    assert_eq!(&decoded, frame);
}

#[test]
fn frame_roundtrip_all_variants() {
    let bi_client = StreamId::new(0, false, StreamDir::Bi).unwrap();
    let uni_client = StreamId::new(1, false, StreamDir::Uni).unwrap();
    let bi_server = StreamId::new(2, true, StreamDir::Bi).unwrap();

    let frames = vec![
        Frame::OpenUni { id: uni_client },
        Frame::OpenBi { id: bi_client },
        Frame::Stream {
            id: bi_client,
            data: Bytes::from_static(b"hello"),
            fin: false,
        },
        Frame::Stream {
            id: bi_server,
            data: Bytes::from(vec![0u8; 1024]),
            fin: true,
        },
        Frame::ResetStream {
            id: bi_client,
            code: 0,
        },
        Frame::ResetStream {
            id: bi_client,
            code: 0xdead_beef,
        },
        Frame::StopSending {
            id: bi_server,
            code: 42,
        },
        Frame::MaxStreamData {
            id: bi_client,
            max: 4096,
        },
        Frame::ConnectionClose {
            code: 0,
            reason: "bye".to_string(),
        },
        Frame::ConnectionClose {
            code: 100,
            reason: "test".to_string(),
        },
    ];

    for f in frames {
        roundtrip(&f);
    }
}

#[test]
fn frame_encoded_len_matches_actual_size() {
    let id = StreamId::new(42, false, StreamDir::Bi).unwrap();
    let frame = Frame::Stream {
        id,
        data: Bytes::from(vec![1u8; 4096]),
        fin: true,
    };

    let encoded = frame.encode();
    assert_eq!(frame.encoded_len(), encoded.len());
}

#[test]
fn frame_decode_unknown_tag() {
    let mut buf = BytesMut::new();
    VarInt::from_u32(99).encode(&mut buf); // unknown tag
    let mut cur = Cursor::new(buf.freeze());

    let err = Frame::decode(&mut cur).unwrap_err();
    match err {
        FrameDecodeError::UnknownTag(99) => {}
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn frame_decode_unexpected_end_stream_payload() {
    // Build a Stream frame but truncate the payload.
    let id = StreamId::new(0, false, StreamDir::Bi).unwrap();

    let mut buf = BytesMut::new();
    VarInt::from_u32(2).encode(&mut buf); // Stream
    VarInt::from_u64(id.0).unwrap().encode(&mut buf);
    VarInt::from_u32(0).encode(&mut buf); // fin = false
    VarInt::from_u32(5).encode(&mut buf); // len=5
    buf.extend_from_slice(b"he"); // only 2 bytes, should be 5

    let mut cur = Cursor::new(buf.freeze());
    let err = Frame::decode(&mut cur).unwrap_err();
    match err {
        FrameDecodeError::UnexpectedEnd => {}
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn frame_decode_invalid_utf8_reason() {
    // ConnectionClose: tag=5, code=0, len=2, bytes=[0xFF,0xFF]
    let mut buf = BytesMut::new();
    VarInt::from_u32(5).encode(&mut buf);
    VarInt::from_u32(0).encode(&mut buf);
    VarInt::from_u32(2).encode(&mut buf);
    buf.extend_from_slice(&[0xFF, 0xFF]);

    let mut cur = Cursor::new(buf.freeze());
    let err = Frame::decode(&mut cur).unwrap_err();
    match err {
        FrameDecodeError::InvalidUtf8 => {}
        other => panic!("unexpected error: {other:?}"),
    }
}
