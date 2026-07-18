#![no_main]

use std::collections::HashSet;

use bytes::{Buf, Bytes};
use libfuzzer_sys::fuzz_target;
use websock_mux_proto::{Frame, StreamId};

fuzz_target!(|data: &[u8]| {
    let mut input = Bytes::copy_from_slice(data);
    let mut receive_streams = HashSet::<StreamId>::new();
    let mut send_streams = HashSet::<StreamId>::new();

    while input.has_remaining() {
        let Ok(frame) = Frame::decode(&mut input) else {
            break;
        };
        match frame {
            Frame::OpenUni { id } => {
                receive_streams.insert(id);
            }
            Frame::OpenBi { id } => {
                receive_streams.insert(id);
                send_streams.insert(id);
            }
            Frame::Stream { id, fin, .. } => {
                if fin {
                    receive_streams.remove(&id);
                }
            }
            Frame::ResetStream { id, .. } => {
                receive_streams.remove(&id);
            }
            Frame::StopSending { id, .. } => {
                send_streams.remove(&id);
            }
            Frame::MaxStreamData { id, .. } => {
                let _ = send_streams.contains(&id);
            }
            Frame::ConnectionClose { .. } => {
                receive_streams.clear();
                send_streams.clear();
                break;
            }
        }
    }
});
