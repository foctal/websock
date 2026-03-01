use bytes::BytesMut;
use std::io::Cursor;

use websock_mux_proto::{VarInt, VarIntBoundsExceeded};

#[test]
fn varint_roundtrip_boundaries() {
    // (value, expected_encoded_size)
    let cases: &[(u64, usize)] = &[
        (0, 1),
        (1, 1),
        (63, 1),
        (64, 2),
        (16383, 2),
        (16384, 4),
        ((1u64 << 30) - 1, 4),
        (1u64 << 30, 8),
        ((1u64 << 62) - 1, 8),
    ];

    for &(value, expected_size) in cases {
        let v = VarInt::from_u64(value).unwrap();
        assert_eq!(v.size(), expected_size, "size mismatch for {}", value);

        let mut buf = BytesMut::new();
        v.encode(&mut buf);

        // encoded length should match
        assert_eq!(
            buf.len(),
            expected_size,
            "encoded length mismatch for {}",
            value
        );

        let mut cur = Cursor::new(buf.freeze());
        let decoded = VarInt::decode(&mut cur).unwrap();
        assert_eq!(
            decoded.into_inner(),
            value,
            "roundtrip mismatch for {}",
            value
        );

        // should consume all
        assert_eq!(cur.position() as usize, expected_size);
    }
}

#[test]
fn varint_rejects_too_large() {
    let too_large = 1u64 << 62;
    let err = VarInt::from_u64(too_large).unwrap_err();
    assert_eq!(err, VarIntBoundsExceeded);
}
