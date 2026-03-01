use crate::bc::model::*;
use crate::Result;
use crate::{bc::codex::BcCodex, Credentials};
use delegate::delegate;
use futures::{sink::Sink, stream::Stream};
use log::trace;
use socket2::{SockRef, TcpKeepalive};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::timeout;
use tokio_util::codec::{Decoder, Encoder, Framed};

/// Connection timeout for TCP connect
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Time before first keepalive probe is sent (idle time)
const TCP_KEEPALIVE_TIME: Duration = Duration::from_secs(60);

/// Interval between keepalive probes
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Number of keepalive probes before declaring connection dead
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
const TCP_KEEPALIVE_RETRIES: u32 = 6;

pub(crate) struct TcpSource {
    inner: Framed<TcpStream, BcCodex>,
}

impl TcpSource {
    pub(crate) async fn new<T: Into<String>, U: Into<String>>(
        addr: SocketAddr,
        username: T,
        password: Option<U>,
        debug: bool,
    ) -> Result<TcpSource> {
        let stream = connect_to(addr).await?;

        let codex = if debug {
            BcCodex::new_with_debug(Credentials::new(username, password))
        } else {
            BcCodex::new(Credentials::new(username, password))
        };
        Ok(Self {
            inner: Framed::new(stream, codex),
        })
    }
}

impl Stream for TcpSource {
    type Item = std::result::Result<<BcCodex as Decoder>::Item, <BcCodex as Decoder>::Error>;

    delegate! {
        to Pin::new(&mut self.inner) {
            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
        }
    }

    delegate! {
        to self.inner {
            fn size_hint(&self) -> (usize, Option<usize>);
        }
    }
}

impl Sink<Bc> for TcpSource {
    type Error = <BcCodex as Encoder<Bc>>::Error;

    delegate! {
        to Pin::new(&mut self.inner) {
            fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
            fn start_send(mut self: Pin<&mut Self>, item: Bc) -> std::result::Result<(), Self::Error>;
            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
            fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
        }
    }
}

/// Helper to create a TcpStream with a connect timeout and socket options configured.
///
/// Configures:
/// - Connection timeout (10 seconds)
/// - TCP_NODELAY (disable Nagle's algorithm for lower latency)
/// - TCP keepalive (detect dead connections)
async fn connect_to(addr: SocketAddr) -> Result<TcpStream> {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };

    // Connect with timeout
    let stream = timeout(TCP_CONNECT_TIMEOUT, socket.connect(addr))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "TCP connection to {} timed out after {:?}",
                    addr, TCP_CONNECT_TIMEOUT
                ),
            )
        })??;

    // Set TCP_NODELAY to disable Nagle's algorithm
    // This reduces latency for small packets (important for camera control)
    if let Err(e) = stream.set_nodelay(true) {
        log::warn!("Failed to set TCP_NODELAY: {}", e);
    }

    // Configure TCP keepalive using socket2
    configure_keepalive(&stream);

    Ok(stream)
}

/// Configure TCP keepalive on the stream using socket2.
///
/// Keepalive parameters:
/// - Time: 60 seconds (idle time before first probe)
/// - Interval: 10 seconds (time between probes)
/// - Retries: 6 (probes before declaring dead) - Linux/macOS only
///
/// Total detection time: 60 + (6 * 10) = 120 seconds max
fn configure_keepalive(stream: &TcpStream) {
    let sock_ref = SockRef::from(stream);

    let mut keepalive = TcpKeepalive::new().with_time(TCP_KEEPALIVE_TIME);

    // with_interval is not available on all platforms
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "windows"
    ))]
    {
        keepalive = keepalive.with_interval(TCP_KEEPALIVE_INTERVAL);
    }

    // with_retries is only available on some platforms
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
    {
        keepalive = keepalive.with_retries(TCP_KEEPALIVE_RETRIES);
    }

    match sock_ref.set_tcp_keepalive(&keepalive) {
        Ok(()) => {
            trace!(
                "TCP keepalive configured: time={:?}, interval={:?}",
                TCP_KEEPALIVE_TIME,
                TCP_KEEPALIVE_INTERVAL
            );
        }
        Err(e) => {
            log::warn!("Failed to set TCP keepalive: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_tcp_nodelay_is_set() {
        // Create a local listener
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Connect to it
        let connect_fut = connect_to(addr);
        let accept_fut = listener.accept();

        let (stream, _) = tokio::join!(connect_fut, accept_fut);
        let stream = stream.unwrap();

        // Verify TCP_NODELAY is set
        assert!(stream.nodelay().unwrap(), "TCP_NODELAY should be enabled");
    }

    #[tokio::test]
    async fn test_connection_timeout() {
        use crate::bc_protocol::errors::Error;

        // Use a non-routable IP address that will timeout
        // 10.255.255.1 is a TEST-NET address that should not respond
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 255, 255, 1), 9999));

        let start = std::time::Instant::now();
        let result = connect_to(addr).await;
        let elapsed = start.elapsed();

        // Should fail with timeout
        assert!(result.is_err(), "Connection should fail");

        let err = result.unwrap_err();
        // The error should be an Io error with TimedOut kind
        match err {
            Error::Io(io_err) => {
                assert_eq!(
                    io_err.kind(),
                    std::io::ErrorKind::TimedOut,
                    "Should be timeout error"
                );
            }
            other => panic!("Expected Io error, got: {:?}", other),
        }

        // Should timeout within reasonable bounds (allow some slack)
        assert!(
            elapsed < Duration::from_secs(15),
            "Should timeout within 15 seconds, took {:?}",
            elapsed
        );
        assert!(
            elapsed >= Duration::from_secs(9),
            "Should wait at least 9 seconds, took {:?}",
            elapsed
        );
    }
}
