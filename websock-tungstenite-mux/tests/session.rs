use futures_util::SinkExt;
use std::time::Duration;
use tokio_tungstenite::tungstenite;
use tungstenite::client::IntoClientRequest;
use tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
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

#[tokio::test]
async fn bidirectional_stream_round_trip_and_shutdown() {
    let server = ServerBuilder::new()
        .with_limits(limits(128))
        .build()
        .await
        .expect("bind server");
    let address = server.local_addr().expect("server address");

    let server_task = tokio::spawn(async move {
        let session = server.accept().await.expect("accept session");
        let (send, mut recv) = session.accept_bi().await.expect("accept bi stream");
        let mut request = [0_u8; 4];
        assert_eq!(
            recv.read(&mut request).await.expect("read request"),
            Some(4)
        );
        assert_eq!(&request, b"ping");
        send.write_all(b"pong").await.expect("write response");
        send.finish().await.expect("finish response");
        session.shutdown().await.expect("shutdown server session");
        assert!(session.is_closed());
    });

    let client = ClientBuilder::new().with_limits(limits(128)).build();
    let session = client
        .connect(&format!("ws://{address}"))
        .await
        .expect("connect client");
    let (send, mut recv) = session.open_bi().await.expect("open bi stream");
    send.write_all(b"ping").await.expect("write request");
    send.finish().await.expect("finish request");
    let mut response = [0_u8; 4];
    assert_eq!(
        recv.read(&mut response).await.expect("read response"),
        Some(4)
    );
    assert_eq!(&response, b"pong");
    assert_eq!(recv.read(&mut response).await.expect("read FIN"), None);
    session.shutdown().await.expect("shutdown client session");
    server_task.await.expect("server task");
}

#[tokio::test]
async fn reset_and_stop_sending_propagate_to_the_peer() {
    let server = ServerBuilder::new()
        .with_limits(limits(128))
        .build()
        .await
        .expect("bind server");
    let address = server.local_addr().expect("server address");

    let server_task = tokio::spawn(async move {
        let session = server.accept().await.expect("accept session");

        let mut reset_recv = session.accept_uni().await.expect("accept reset stream");
        let mut byte = [0_u8; 1];
        assert_eq!(
            reset_recv.read(&mut byte).await.expect("observe reset"),
            None
        );

        let stop_recv = session.accept_uni().await.expect("accept stopped stream");
        stop_recv.stop(7).await.expect("send stop");
        session.shutdown().await.expect("shutdown server session");
    });

    let client = ClientBuilder::new().with_limits(limits(128)).build();
    let session = client
        .connect(&format!("ws://{address}"))
        .await
        .expect("connect client");
    let reset_send = session.open_uni().await.expect("open reset stream");
    reset_send.reset(42).await.expect("reset stream");

    let stopped_send = session.open_uni().await.expect("open stopped stream");
    let error = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match stopped_send.write_all(b"x").await {
                Ok(()) => tokio::task::yield_now().await,
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("stop propagation timed out");
    assert!(matches!(error, websock_proto::Error::Closed));
    session.shutdown().await.expect("shutdown client session");
    server_task.await.expect("server task");
}

#[tokio::test]
async fn malformed_wire_frame_closes_the_session() {
    let server = ServerBuilder::new().build().await.expect("bind server");
    let address = server.local_addr().expect("server address");

    let server_task = tokio::spawn(async move {
        let session = server.accept().await.expect("accept session");
        let result = tokio::time::timeout(Duration::from_secs(2), session.accept_uni())
            .await
            .expect("session did not close");
        assert!(matches!(result, Err(websock_proto::Error::Closed)));
    });

    let mut request = format!("ws://{address}")
        .into_client_request()
        .expect("create request");
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        websock_mux_proto::SUBPROTOCOL
            .parse()
            .expect("protocol header"),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("connect raw WebSocket");
    socket
        .send(tungstenite::Message::Binary(vec![0xff].into()))
        .await
        .expect("send malformed frame");
    server_task.await.expect("server task");
}

#[tokio::test]
async fn tls_mux_round_trip() {
    let tls =
        websock_tungstenite_mux::tls::TlsConfig::new_insecure_config().expect("create TLS config");
    let server = ServerBuilder::new()
        .with_tls_config(tls.server_config)
        .with_default_alpn()
        .with_limits(limits(128))
        .build()
        .await
        .expect("bind TLS server");
    let address = server.local_addr().expect("server address");

    let server_task = tokio::spawn(async move {
        let session = server.accept().await.expect("accept TLS session");
        let mut recv = session.accept_uni().await.expect("accept TLS stream");
        let mut payload = [0_u8; 6];
        assert_eq!(
            recv.read(&mut payload).await.expect("read TLS data"),
            Some(6)
        );
        assert_eq!(&payload, b"secure");
        session
            .shutdown()
            .await
            .expect("shutdown TLS server session");
    });

    let client = ClientBuilder::new()
        .with_tls_config(tls.client_config)
        .with_default_alpn()
        .with_limits(limits(128))
        .build();
    let session = client
        .connect(&format!("wss://localhost:{}", address.port()))
        .await
        .expect("connect TLS client");
    let send = session.open_uni().await.expect("open TLS stream");
    send.write_all(b"secure").await.expect("write TLS data");
    send.finish().await.expect("finish TLS stream");
    session
        .shutdown()
        .await
        .expect("shutdown TLS client session");
    server_task.await.expect("server task");
}

#[tokio::test]
async fn dropping_last_session_handle_closes_the_peer() {
    let server = ServerBuilder::new().build().await.expect("bind server");
    let address = server.local_addr().expect("server address");

    let server_task = tokio::spawn(async move {
        let session = server.accept().await.expect("accept session");
        let result = tokio::time::timeout(Duration::from_secs(2), session.accept_uni())
            .await
            .expect("peer session remained open");
        assert!(matches!(result, Err(websock_proto::Error::Closed)));
    });

    let session = ClientBuilder::new()
        .build()
        .connect(&format!("ws://{address}"))
        .await
        .expect("connect client");
    let clone = session.clone();
    drop(session);
    tokio::task::yield_now().await;
    drop(clone);

    server_task.await.expect("server task");
}

#[tokio::test]
#[ignore = "long-running concurrency and backpressure coverage"]
async fn many_concurrent_streams_remain_bounded_under_backpressure() {
    const STREAMS: usize = 128;
    const BYTES_PER_STREAM: usize = 64 * 1024;

    let soak_limits = Limits {
        max_ws_message_size: 64 * 1024,
        max_stream_data_per_frame: 1024,
        max_open_streams: STREAMS * 2,
        recv_event_queue_len: 4,
        outbound_queue_len: 16,
        max_batch_frames: 8,
        max_batch_bytes: 8 * 1024,
        initial_stream_window: 4 * 1024,
        stream_window_update_threshold: 2 * 1024,
        accept_uni_queue_len: STREAMS,
        accept_bi_queue_len: 4,
    };
    let server = ServerBuilder::new()
        .with_limits(soak_limits.clone())
        .build()
        .await
        .expect("bind server");
    let address = server.local_addr().expect("server address");

    let server_task = tokio::spawn(async move {
        let session = server.accept().await.expect("accept session");
        let mut readers = Vec::with_capacity(STREAMS);
        for _ in 0..STREAMS {
            let mut recv = session.accept_uni().await.expect("accept stream");
            readers.push(tokio::spawn(async move {
                let mut total = 0;
                while let Some(chunk) = recv.read_chunk(257).await.expect("read chunk") {
                    total += chunk.len();
                    tokio::task::yield_now().await;
                }
                total
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.expect("reader task"), BYTES_PER_STREAM);
        }
        session.shutdown().await.expect("shutdown server session");
    });

    let session = ClientBuilder::new()
        .with_limits(soak_limits)
        .build()
        .connect(&format!("ws://{address}"))
        .await
        .expect("connect client");
    let mut writers = Vec::with_capacity(STREAMS);
    for _ in 0..STREAMS {
        let session = session.clone();
        writers.push(tokio::spawn(async move {
            let send = session.open_uni().await.expect("open stream");
            send.write_all(&vec![0x5a; BYTES_PER_STREAM])
                .await
                .expect("write stream");
            send.finish().await.expect("finish stream");
        }));
    }
    for writer in writers {
        writer.await.expect("writer task");
    }
    session.shutdown().await.expect("shutdown client session");
    server_task.await.expect("server task");
}
