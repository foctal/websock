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
