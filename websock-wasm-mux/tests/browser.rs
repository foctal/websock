use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test(async)]
async fn bidirectional_stream_matches_native_behavior() {
    let client = websock_wasm_mux::ClientBuilder::new().build();
    let session = client
        .connect("ws://127.0.0.1:32124")
        .await
        .expect("connect mux endpoint");
    let (send, mut recv) = session.open_bi().expect("open bi stream");

    send.write_all(b"browser-mux").await.expect("write request");
    send.finish().await.expect("finish request");

    let mut response = Vec::new();
    while let Some(chunk) = recv.read_chunk(1024).await.expect("read response") {
        response.extend_from_slice(&chunk);
    }
    assert_eq!(response, b"browser-mux");
    session.shutdown().await.expect("shutdown mux session");
}

#[wasm_bindgen_test(async)]
async fn batch_boundary_preserves_the_dequeued_next_frame() {
    let limits = websock_wasm_mux::Limits {
        max_stream_data_per_frame: 64,
        max_batch_bytes: 100,
        ..Default::default()
    };
    let client = websock_wasm_mux::ClientBuilder::new()
        .limits(limits)
        .build();
    let session = client.connect("ws://127.0.0.1:32124").await.unwrap();
    let (send, mut recv) = session.open_bi().unwrap();
    let expected: Vec<u8> = (0..2000).map(|n| (n % 251) as u8).collect();
    send.write_all(&expected).await.unwrap();
    send.finish().await.unwrap();
    let mut actual = Vec::new();
    while let Some(chunk) = recv.read_chunk(128).await.unwrap() {
        actual.extend_from_slice(&chunk);
    }
    assert_eq!(actual, expected);
    session.shutdown().await.unwrap();
}
