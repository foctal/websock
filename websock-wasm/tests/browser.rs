use futures_util::StreamExt;
use gloo_timers::future::TimeoutFuture;
use wasm_bindgen_test::*;
use websock_proto::{Error, Message};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test(async)]
async fn connect_failure_is_reported() {
    let result = websock_wasm::connect(
        "not-a-websocket-url",
        websock_proto::ConnectOptions::default(),
    )
    .await;
    assert!(result.is_err());
}

#[wasm_bindgen_test(async)]
async fn receive_queue_overflow_closes_the_connection() {
    let mut connection = websock_wasm::connect(
        "ws://127.0.0.1:32123/overflow",
        websock_proto::ConnectOptions::default(),
    )
    .await
    .expect("connect overflow endpoint");

    TimeoutFuture::new(250).await;
    let mut received = 0;
    let terminal = loop {
        match connection.recv().await {
            Ok(_) => received += 1,
            Err(error) => break error,
        }
    };
    assert!(received < 256);
    assert!(matches!(terminal, Error::Closed | Error::Protocol(_)));
}

#[wasm_bindgen_test(async)]
async fn close_frame_details_are_preserved() {
    let mut connection = websock_wasm::connect(
        "ws://127.0.0.1:32123/close",
        websock_proto::ConnectOptions::default(),
    )
    .await
    .expect("connect close endpoint");

    assert!(matches!(connection.recv().await, Err(Error::Closed)));
    assert_eq!(
        connection.close_frame(),
        Some(websock_proto::CloseFrame {
            code: 1008,
            reason: "browser-test".into(),
        })
    );
}

#[wasm_bindgen_test(async)]
async fn dropping_one_split_half_keeps_the_other_half_alive() {
    let connection = websock_wasm::connect(
        "ws://127.0.0.1:32123/delayed",
        websock_proto::ConnectOptions::default(),
    )
    .await
    .expect("connect delayed endpoint");
    let (sink, mut stream) = websock_wasm::stream::split(connection);
    drop(sink);

    assert_eq!(
        stream.next().await.expect("stream item").expect("message"),
        Message::Text("still-open".into())
    );
}
