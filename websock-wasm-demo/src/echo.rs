use futures_util::{SinkExt, StreamExt};
use websock::{ClientBuilder, Message};

//pub const DEFAULT_ECHO_URL: &str = "wss://echo.websocket.org";

pub async fn run_conn_demo(url: &str, log: impl Fn(&str)) {
    log("[conn demo] start");

    let client = ClientBuilder::new()
        .with_system_roots()
        .expect("client config failed");

    log(&format!("[conn demo] Connecting to server at {url}..."));
    log(&format!(
        "[conn demo] Using WebSocket options: {:?}",
        client.options()
    ));

    let mut conn = client.connect(url).await.expect("connect failed");

    conn.send(Message::Text("hello from wasm".into()))
        .await
        .expect("send failed");

    let msg = conn.recv().await.expect("recv failed");
    log(&format!("[conn demo] got: {msg:?}"));

    conn.close().await.expect("close failed");
    log("[conn demo] done");
}

pub async fn run_split_demo(url: &str, log: impl Fn(&str)) {
    log("[split demo] start");

    let client = ClientBuilder::new()
        .with_system_roots()
        .expect("client config failed");

    log(&format!("[split demo] Connecting to server at {url}..."));
    log(&format!(
        "[split demo] Using WebSocket options: {:?}",
        client.options()
    ));

    let conn = client.connect(url).await.expect("connect failed");
    let (mut sink, mut stream) = websock::stream::split(conn);

    sink.send(Message::Text("hello from wasm split".into()))
        .await
        .expect("send failed");

    let msg = stream
        .next()
        .await
        .expect("stream closed")
        .expect("recv failed");

    log(&format!("[split demo] got: {msg:?}"));
    log("[split demo] done");
}
