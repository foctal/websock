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
        assert!(matches!(err, Error::Protocol(_)));
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
