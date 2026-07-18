pub const DEFAULT_MUX_URL: &str = "wss://localhost:9001";

pub async fn run_mux_bi_demo(url: &str, log: impl Fn(&str)) {
    log("[mux bi demo] start");

    let client = websock_mux::ClientBuilder::new()
        .with_system_roots()
        .expect("client config failed");

    log(&format!(
        "[mux bi demo] Connecting to mux server at {url}..."
    ));
    log(&format!(
        "[mux bi demo] Using WebSocket options: {:?}",
        client.options()
    ));

    // Mux Session
    let session = match client.connect(url).await {
        Ok(s) => s,
        Err(e) => {
            log(&format!("[mux bi demo] connect failed: {e:?}"));
            return;
        }
    };

    let (send, mut recv) = session.open_bi().expect("open_bi failed");

    send.write(b"hello mux from wasm")
        .await
        .expect("send failed");
    send.finish().await.expect("finish failed");

    let mut buf = vec![0u8; 1024];
    while let Some(n) = recv.read(&mut buf).await.expect("read failed") {
        let text = String::from_utf8_lossy(&buf[..n]);
        log(&format!("[mux bi demo] recv: {}", text));
    }

    log("[mux bi demo] done");
}
