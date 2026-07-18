#![no_main]

use bytes::{Buf, Bytes};
use libfuzzer_sys::fuzz_target;
use websock_mux_proto::VarInt;

fuzz_target!(|data: &[u8]| {
    let mut input = Bytes::copy_from_slice(data);
    while input.has_remaining() {
        if VarInt::decode(&mut input).is_err() {
            break;
        }
    }
});
