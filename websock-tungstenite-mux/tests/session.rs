use websock_tungstenite_mux::{ClientBuilder, Limits, ServerBuilder};

fn limits(window: usize) -> Limits {
    Limits {
        max_ws_message_size: 1024,
        max_stream_data_per_frame: 64,
        max_open_streams: 8,
        recv_event_queue_len: 4,
        outbound_queue_len: 8,
        max_batch_frames: 4,
        max_batch_bytes: 128,
        initial_stream_window: window,
        stream_window_update_threshold: window / 2,
        accept_uni_queue_len: 4,
        accept_bi_queue_len: 4,
    }
}

#[tokio::test]
async fn different_peer_windows_interoperate_with_backpressure() {
    let server = ServerBuilder::new()
        .with_limits(limits(64))
        .build()
        .await
        .expect("bind server");
    let address = server.local_addr().expect("server address");

    let server_task = tokio::spawn(async move {
        let session = server.accept().await.expect("accept session");
        let mut stream = session.accept_uni().await.expect("accept uni stream");
        let mut received = Vec::new();
        let mut buffer = [0_u8; 31];
        while let Some(read) = stream.read(&mut buffer).await.expect("read stream") {
            received.extend_from_slice(&buffer[..read]);
        }
        received
    });

    let client = ClientBuilder::new().with_limits(limits(128)).build();
    let session = client
        .connect(&format!("ws://{address}"))
        .await
        .expect("connect client");
    let stream = session.open_uni().await.expect("open uni stream");
    let payload = vec![42_u8; 256];
    stream.write_all(&payload).await.expect("write payload");
    stream.finish().await.expect("finish stream");

    assert_eq!(server_task.await.expect("server task"), payload);
}

#[tokio::test]
async fn invalid_limits_fail_before_connecting() {
    let invalid = Limits {
        outbound_queue_len: 0,
        ..Limits::default()
    };
    let client = ClientBuilder::new().with_limits(invalid).build();
    let err = match client.connect("ws://127.0.0.1:1").await {
        Ok(_) => panic!("invalid limits must fail"),
        Err(err) => err,
    };
    assert!(matches!(err, websock_proto::Error::Protocol(_)));
}
