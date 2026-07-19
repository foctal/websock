use futures_util::{SinkExt, StreamExt};
use std::error::Error as _;
use websock_proto::{Error, Message, WebSocketLimits};
use websock_tungstenite::{ClientBuilder, ServerBuilder};

#[tokio::test]
async fn client_server_round_trip_text_and_binary() {
    let server = ServerBuilder::new()
        .with_protocol("test.v1")
        .build()
        .await
        .expect("bind server");
    let address = server.local_addr().expect("server address");

    let server_task = tokio::spawn(async move {
        let mut connection = server.accept().await.expect("accept connection");
        assert_eq!(connection.negotiated_subprotocol(), Some("test.v1"));
        assert_eq!(
            connection.recv().await.expect("receive text"),
            Message::Text("hello".into())
        );
        connection
            .send(Message::Binary(b"world".as_slice().into()))
            .await
            .expect("send binary");
        connection.close().await.expect("close connection");
    });

    let client = ClientBuilder::new().with_protocol("test.v1").build();
    let mut connection = client
        .connect(&format!("ws://{address}"))
        .await
        .expect("connect client");
    assert_eq!(connection.negotiated_subprotocol(), Some("test.v1"));
    connection
        .send(Message::Text("hello".into()))
        .await
        .expect("send text");
    assert_eq!(
        connection.recv().await.expect("receive binary"),
        Message::Binary(b"world".as_slice().into())
    );
    server_task.await.expect("server task");
}

#[tokio::test]
async fn server_rejects_message_above_configured_limit() {
    let limits = WebSocketLimits {
        max_message_size: 64,
        max_frame_size: 64,
        max_write_buffer_size: 1024,
    };
    let server = ServerBuilder::new()
        .with_limits(limits)
        .build()
        .await
        .expect("bind server");
    let address = server.local_addr().expect("server address");

    let server_task = tokio::spawn(async move {
        let mut connection = server.accept().await.expect("accept connection");
        let err = connection
            .recv()
            .await
            .expect_err("oversized message must fail");
        assert!(matches!(err, Error::Transport(_)));
        assert!(
            err.source()
                .is_some_and(|source| source.is::<tokio_tungstenite::tungstenite::Error>())
        );
    });

    let client = ClientBuilder::new().build();
    let mut connection = client
        .connect(&format!("ws://{address}"))
        .await
        .expect("connect client");
    let _ = connection.send(Message::Binary(vec![0; 65].into())).await;
    server_task.await.expect("server task");
}

#[tokio::test]
async fn invalid_server_limits_return_an_error() {
    let limits = WebSocketLimits {
        max_message_size: 0,
        ..WebSocketLimits::default()
    };
    let err = match ServerBuilder::new().with_limits(limits).build().await {
        Ok(_) => panic!("invalid limits must fail"),
        Err(err) => err,
    };
    assert!(matches!(err, Error::Protocol(_)));
}

#[tokio::test]
async fn split_connection_sends_and_receives_independently() {
    let server = ServerBuilder::new().build().await.expect("bind server");
    let address = server.local_addr().expect("server address");

    let server_task = tokio::spawn(async move {
        let mut connection = server.accept().await.expect("accept connection");
        let message = connection.recv().await.expect("receive message");
        connection.send(message).await.expect("echo message");
        assert!(matches!(connection.recv().await, Err(Error::Closed)));
    });

    let connection = ClientBuilder::new()
        .build()
        .connect(&format!("ws://{address}"))
        .await
        .expect("connect client");
    let (mut sink, mut stream) = websock_tungstenite::stream::split(connection);
    sink.send(Message::Text("split".into()))
        .await
        .expect("send through split sink");
    assert_eq!(
        stream.next().await.expect("stream item").expect("message"),
        Message::Text("split".into())
    );
    sink.close().await.expect("close split sink");
    server_task.await.expect("server task");
}

#[tokio::test]
async fn tls_client_server_round_trip() {
    let (certificates, key) =
        websock_tungstenite::tls::generate_self_signed_pair_der(vec!["localhost".into()])
            .expect("generate certificate");
    let certificate_bytes = certificates
        .iter()
        .map(|certificate| certificate.as_ref().to_vec())
        .collect::<Vec<_>>();
    let server = ServerBuilder::new()
        .with_certificate(certificates, key)
        .expect("configure certificate")
        .with_default_alpn()
        .build()
        .await
        .expect("bind TLS server");
    let address = server.local_addr().expect("server address");

    let server_task = tokio::spawn(async move {
        let mut connection = server.accept().await.expect("accept TLS connection");
        assert_eq!(
            connection.recv().await.expect("receive TLS message"),
            Message::Text("secure".into())
        );
        connection
            .send(Message::Binary(b"reply".as_slice().into()))
            .await
            .expect("send TLS reply");
        connection.close().await.expect("close TLS connection");
    });

    let client = ClientBuilder::new()
        .with_default_alpn()
        .with_server_certificates(certificate_bytes)
        .expect("configure client roots");
    let mut connection = client
        .connect(&format!("wss://localhost:{}", address.port()))
        .await
        .expect("connect TLS client");
    connection
        .send(Message::Text("secure".into()))
        .await
        .expect("send TLS message");
    assert_eq!(
        connection.recv().await.expect("receive TLS reply"),
        Message::Binary(b"reply".as_slice().into())
    );
    server_task.await.expect("server task");
}

#[tokio::test]
async fn received_close_frame_details_are_preserved() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind raw server");
    let address = listener.local_addr().expect("server address");
    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept TCP connection");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("accept WebSocket");
        socket
            .send(tokio_tungstenite::tungstenite::Message::Close(Some(
                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code:
                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy,
                    reason: "policy".into(),
                },
            )))
            .await
            .expect("send close frame");
    });

    let mut connection = ClientBuilder::new()
        .build()
        .connect(&format!("ws://{address}"))
        .await
        .expect("connect client");
    assert!(matches!(connection.recv().await, Err(Error::Closed)));
    assert_eq!(
        connection.close_frame(),
        Some(websock_proto::CloseFrame {
            code: 1008,
            reason: "policy".into(),
        })
    );
    server_task.await.expect("server task");
}
