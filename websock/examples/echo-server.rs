//! Echo server example for the websock native transport.

use clap::Parser;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::{fs, io, path};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use websock::{Server, ServerBuilder};

/// Command-line arguments for the echo server.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Bind address for the server.
    #[arg(short, long, default_value = "127.0.0.1:9001")]
    addr: std::net::SocketAddr,

    /// Use the certificates at this path, encoded as PEM (enables wss://).
    #[arg(long)]
    tls_cert: Option<path::PathBuf>,

    /// Use the private key at this path, encoded as PEM (enables wss://).
    #[arg(long)]
    tls_key: Option<path::PathBuf>,
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

    let mut builder = ServerBuilder::new()
        .with_addr(args.addr)
        .with_default_alpn();

    match (&args.tls_cert, &args.tls_key) {
        (Some(cert_path), Some(key_path)) => {
            let (chain, key) = load_pem_cert_and_key(cert_path, key_path)?;
            builder = builder.with_certificate(chain, key)?;
            tracing::info!("TLS enabled (wss://)");
        }
        (None, None) => {
            tracing::warn!("TLS disabled (ws://). Provide --tls-cert/--tls-key to enable wss://");
        }
        _ => anyhow::bail!("both --tls-cert and --tls-key must be provided together, or neither"),
    }

    let server: Server = builder.build().await?;
    let scheme = if args.tls_cert.is_some() { "wss" } else { "ws" };
    tracing::info!("listening on {} ({}://{})", args.addr, scheme, args.addr);

    loop {
        let mut conn = server.accept().await?;
        tracing::info!("accepted connection: {:?}", conn.peer_addr());
        tokio::spawn(async move {
            loop {
                match conn.recv().await {
                    Ok(msg) => {
                        tracing::info!("received message: {:?}", msg);
                        if conn.send(msg).await.is_err() {
                            break;
                        }
                        tracing::info!("echoed message");
                    }
                    Err(_) => break,
                }
            }
            let _ = conn.close().await;
            tracing::info!("connection closed");
        });
    }
}

/// Load a PEM-encoded certificate chain and private key from disk.
fn load_pem_cert_and_key(
    cert_path: &path::Path,
    key_path: &path::Path,
) -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let chain_file = fs::File::open(cert_path)?;
    let mut chain_reader = io::BufReader::new(chain_file);
    let chain: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut chain_reader).collect::<Result<_, _>>()?;
    anyhow::ensure!(!chain.is_empty(), "could not find certificate");

    let key_file = fs::File::open(key_path)?;
    let mut key_reader = io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| anyhow::anyhow!("missing private key"))?;

    Ok((chain, key))
}
