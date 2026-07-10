use std::{future::Future, sync::Arc};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use fastwebsockets::{FragmentCollector, handshake};
use http_body_util::{BodyExt, Empty};
use hyper::upgrade::Upgraded;
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use url::Url;

pub type WsStream = FragmentCollector<TokioIo<Upgraded>>;

/// Establishes a TLS stream to the host/port of `url`.
async fn tls_stream(
    url: &Url,
    tls: Arc<ClientConfig>,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("URL has no host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("URL has no port"))?;
    let server_name = rustls::pki_types::ServerName::try_from(host)
        .map_err(|error| anyhow!("invalid TLS server name: {error}"))?
        .to_owned();
    let tcp = TcpStream::connect((host, port))
        .await
        .context("TCP connect failed")?;
    tcp.set_nodelay(true).context("could not set TCP_NODELAY")?;
    TlsConnector::from(tls)
        .connect(server_name, tcp)
        .await
        .context("TLS handshake failed")
}

/// One-shot HTTPS request that returns the full response body. Used for the
/// KuCoin bullet-public token handshake before opening its WebSocket.
pub async fn https_request(
    method: Method,
    url: &str,
    tls: Arc<ClientConfig>,
) -> Result<Bytes> {
    let url = Url::parse(url).context("invalid HTTPS URL")?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("URL has no host"))?
        .to_owned();
    let stream = tls_stream(&url, tls).await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .context("HTTP handshake failed")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let path = url[url::Position::BeforePath..].to_owned();
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("Host", host)
        .header("Content-Length", "0")
        .body(Empty::<Bytes>::new())
        .context("invalid HTTPS request")?;
    let response = sender
        .send_request(request)
        .await
        .context("HTTPS request failed")?;
    if !response.status().is_success() {
        return Err(anyhow!("HTTPS request returned {}", response.status()));
    }
    let body = response
        .into_body()
        .collect()
        .await
        .context("reading HTTPS response failed")?
        .to_bytes();
    Ok(body)
}

pub struct SpawnExecutor;

impl<F> hyper::rt::Executor<F> for SpawnExecutor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        tokio::spawn(future);
    }
}

pub fn tls_config() -> Arc<ClientConfig> {
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

pub async fn connect(url: &str, tls: Arc<ClientConfig>) -> Result<WsStream> {
    let url = Url::parse(url).context("invalid websocket URL")?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("websocket URL has no host"))?;
    let tls = tls_stream(&url, tls).await?;
    let request = Request::builder()
        .uri(url.as_str())
        .header("Host", host)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .context("invalid websocket upgrade request")?;
    let (mut ws, _) = handshake::client(&SpawnExecutor, request, tls)
        .await
        .map_err(|error| anyhow!("websocket handshake failed: {error:?}"))?;
    ws.set_auto_pong(true);
    Ok(FragmentCollector::new(ws))
}
