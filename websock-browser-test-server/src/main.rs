//! Local WebSocket endpoints used by the browser integration tests.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite;
use websock_tungstenite_mux::ServerBuilder;

const WEBSOCKET_ADDRESS: &str = "127.0.0.1:32123";
const MUX_ADDRESS: &str = "127.0.0.1:32124";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let websocket = serve_websocket_endpoints();
    let mux = serve_mux_endpoint();
    tokio::try_join!(websocket, mux)?;
    Ok(())
}

async fn serve_websocket_endpoints() -> std::io::Result<()> {
    let listener = TcpListener::bind(WEBSOCKET_ADDRESS).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let _ = handle_websocket(stream).await;
        });
    }
}

#[allow(clippy::result_large_err)]
async fn handle_websocket(
    stream: TcpStream,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = Arc::new(Mutex::new(String::new()));
    let callback_path = Arc::clone(&path);
    let mut socket = tokio_tungstenite::accept_hdr_async(
        stream,
        move |request: &tungstenite::handshake::server::Request, response| {
            *callback_path
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                request.uri().path().to_owned();
            Ok(response)
        },
    )
    .await?;
    let path = path
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();

    match path.as_str() {
        "/overflow" => {
            for value in 0..256_u16 {
                socket
                    .send(tungstenite::Message::Binary(
                        value.to_be_bytes().to_vec().into(),
                    ))
                    .await?;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
            socket.close(None).await?;
        }
        "/close" => {
            socket
                .send(tungstenite::Message::Close(Some(
                    tungstenite::protocol::CloseFrame {
                        code: tungstenite::protocol::frame::coding::CloseCode::Policy,
                        reason: "browser-test".into(),
                    },
                )))
                .await?;
        }
        "/delayed" => {
            tokio::time::sleep(Duration::from_millis(100)).await;
            socket
                .send(tungstenite::Message::Text("still-open".into()))
                .await?;
            let _ = socket.next().await;
        }
        _ => {
            while let Some(message) = socket.next().await {
                let message = message?;
                if message.is_close() {
                    break;
                }
                socket.send(message).await?;
            }
        }
    }
    Ok(())
}

async fn serve_mux_endpoint() -> std::io::Result<()> {
    let address: std::net::SocketAddr = MUX_ADDRESS.parse().expect("valid mux test address");
    let server = ServerBuilder::new()
        .with_addr(address)
        .build()
        .await
        .map_err(std::io::Error::other)?;

    loop {
        let session = match server.accept().await {
            Ok(session) => session,
            Err(_) => continue,
        };
        tokio::spawn(async move {
            while let Ok((send, mut recv)) = session.accept_bi().await {
                while let Ok(Some(chunk)) = recv.read_chunk(16 * 1024).await {
                    if send.write_buf(chunk).await.is_err() {
                        return;
                    }
                }
                if send.finish().await.is_err() {
                    return;
                }
            }
        });
    }
}
