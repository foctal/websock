//! Echo client example for the websock native transport.

use clap::Parser;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::path;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use url::Url;
use websock::{Client, ClientBuilder, Message};

const DEFAULT_ECHO_URL: &str = "wss://echo.websocket.org";

/// Command-line arguments for the echo client.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// WebSocket server URL (ws:// or wss://). If omitted, uses a public echo server.
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

    let mut conn = client.connect(url.as_str()).await?;
    conn.send(Message::Text("hello".into())).await?;
    let msg = conn.recv().await?;
    tracing::info!("received message: {:?}", msg);
    conn.close().await?;
    Ok(())
}

/// Build a client based on the URL scheme and CLI options.
fn build_client(url: &Url, args: &Args) -> anyhow::Result<Client> {
    // Plain WS requires no TLS configuration.
    if url.scheme() == "ws" {
        return Ok(ClientBuilder::new().build());
    }

    // From here: wss:// with TLS enabled.
    let is_local = is_localhost_url(url);

    if args.tls_disable_verify {
        tracing::warn!("disabling TLS certificate verification");
        Ok(ClientBuilder::new()
            .dangerous()
            .with_no_certificate_verification()?)
    } else if let Some(path) = &args.tls_cert {
        let chain = load_pem_certs(path)?;
        anyhow::ensure!(!chain.is_empty(), "could not find certificate");
        Ok(ClientBuilder::new().with_server_certificates(chain)?)
    } else if is_local {
        tracing::warn!(
            "no --tls-cert provided and target looks local ({}); \
             using insecure mode for quick testing (equivalent to --tls-disable-verify)",
            url
        );
        Ok(ClientBuilder::new()
            .dangerous()
            .with_no_certificate_verification()?)
    } else {
        Ok(ClientBuilder::new().with_system_roots()?)
    }
}

/// Determine whether a URL points to a loopback host.
fn is_localhost_url(url: &Url) -> bool {
    match url.host_str() {
        Some("localhost") => true,
        Some(host) => host == "127.0.0.1" || host == "::1",
        None => false,
    }
}

/// Load PEM-encoded certificates from disk into DER bytes.
fn load_pem_certs(p: &path::Path) -> anyhow::Result<Vec<Vec<u8>>> {
    let certs = CertificateDer::pem_file_iter(p)?.collect::<Result<Vec<_>, _>>()?;
    Ok(certs.into_iter().map(|c| c.to_vec()).collect())
}
