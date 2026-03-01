//! Echo client example for the websock mux transport.
//!
//! This demonstrates opening a bidirectional stream and echoing bytes.

use clap::Parser;
use rustls::{RootCertStore, client::ClientConfig};
use std::path;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use url::Url;
use websock_mux::{
    Client, ClientBuilder,
    tls::{self, TlsClientConfigBuilder},
};

const DEFAULT_ECHO_URL: &str = "ws://127.0.0.1:9001";

/// Command-line arguments for the echo mux client.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// WebSocket server URL (ws:// or wss://).
    #[arg(short, long)]
    url: Option<Url>,

    /// Accept the server certificates at this path, encoded as PEM.
    #[arg(long)]
    tls_cert: Option<path::PathBuf>,

    /// Dangerous: Disable TLS certificate verification.
    #[arg(long, default_value = "false")]
    tls_disable_verify: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber for logging in this example.
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("failed to set global tracing subscriber");

    let args = Args::parse();
    let url = args
        .url
        .clone()
        .unwrap_or_else(|| Url::parse(DEFAULT_ECHO_URL).expect("default url"));

    let client: Client = build_client(&url, &args)?;
    tracing::info!("connecting to {}", url);

    let session = client.connect(url.as_str()).await?;
    tracing::info!("connected");

    tracing::info!("opening bidirectional stream");
    let (send, mut recv) = session.open_bi().await?;
    tracing::info!("opened bidirectional stream");

    send.write_all(b"hello mux").await?;
    send.finish().await?;

    while let Some(chunk) = recv.read_chunk(1024).await? {
        tracing::info!("Received: {}", String::from_utf8_lossy(&chunk));
    }
    Ok(())
}

/// Build a mux client based on the URL scheme and CLI options.
fn build_client(url: &Url, args: &Args) -> anyhow::Result<Client> {
    // Plain WS requires no TLS configuration.
    if url.scheme() == "ws" {
        return Ok(ClientBuilder::new().build());
    }

    // From here: wss:// with TLS enabled.
    let is_local = is_localhost_url(url);

    let tls_cfg: ClientConfig = if args.tls_disable_verify {
        tracing::warn!("disabling TLS certificate verification");
        TlsClientConfigBuilder::new_insecure()?.build()
    } else if let Some(path) = &args.tls_cert {
        let certs = tls::cert::load_certs(path)?;
        anyhow::ensure!(!certs.is_empty(), "could not find certificate");

        let mut roots = RootCertStore::empty();
        for c in certs {
            let _ = roots.add(c);
        }

        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    } else if is_local {
        tracing::warn!(
            "no --tls-cert provided and target looks local ({}); \
             using insecure mode for quick testing (equivalent to --tls-disable-verify)",
            url
        );
        TlsClientConfigBuilder::new_insecure()?.build()
    } else {
        TlsClientConfigBuilder::new_with_native_certs()?.build()
    };

    Ok(ClientBuilder::new()
        .with_tls_config(tls_cfg)
        .with_default_alpn()
        .build())
}

/// Determine whether a URL points to a loopback host.
fn is_localhost_url(url: &Url) -> bool {
    match url.host_str() {
        Some("localhost") => true,
        Some(host) => host == "127.0.0.1" || host == "::1",
        None => false,
    }
}
