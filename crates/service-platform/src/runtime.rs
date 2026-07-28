//! Fail-fast Axum server lifecycle with configured transport timeouts.

use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{Router, http::StatusCode};
use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{Mutex, watch},
    task::{JoinHandle, JoinSet},
    time::{Instant, Sleep, sleep_until, timeout},
};
use tower_http::timeout::{RequestBodyTimeoutLayer, ResponseBodyTimeoutLayer, TimeoutLayer};

use crate::{
    config::AdminConfig,
    errors::{AppError, ErrorType, Result},
};

/// HTTP transport and handler timeout policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminServerOptions {
    pub read_header_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub idle_timeout: Duration,
}

impl Default for AdminServerOptions {
    fn default() -> Self {
        Self {
            read_header_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(120),
        }
    }
}

impl From<&AdminConfig> for AdminServerOptions {
    fn from(config: &AdminConfig) -> Self {
        Self {
            read_header_timeout: config.read_header_timeout,
            read_timeout: config.read_timeout,
            write_timeout: config.write_timeout,
            idle_timeout: config.idle_timeout,
        }
    }
}

struct RunningServer {
    address: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<io::Result<()>>,
}

/// Owns an HTTP listener, accepted connections, and graceful shutdown.
pub struct AdminServer {
    configured_address: String,
    router: Router,
    options: AdminServerOptions,
    running: Arc<Mutex<Option<RunningServer>>>,
}

impl AdminServer {
    pub fn new(address: impl Into<String>, router: Router) -> Self {
        Self::with_options(address, router, AdminServerOptions::default())
    }

    pub fn with_options(
        address: impl Into<String>,
        router: Router,
        options: AdminServerOptions,
    ) -> Self {
        Self {
            configured_address: address.into(),
            router,
            options,
            running: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn start(&self) -> Result<SocketAddr> {
        let mut running = self.running.lock().await;
        if running.is_some() {
            return Err(AppError::new(
                ErrorType::Conflict,
                "admin server is already started",
            ));
        }
        let address = normalize_bind_address(&self.configured_address);
        let listener = TcpListener::bind(&address).await.map_err(|error| {
            AppError::wrap(
                ErrorType::ServiceUnavailable,
                format!("bind admin server {address}"),
                error,
            )
        })?;
        let actual = listener.local_addr().map_err(|error| {
            AppError::wrap(ErrorType::Internal, "read admin listener address", error)
        })?;
        let router = self
            .router
            .clone()
            .layer(ResponseBodyTimeoutLayer::new(self.options.write_timeout))
            .layer(RequestBodyTimeoutLayer::new(self.options.read_timeout))
            .layer(TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                self.options.write_timeout,
            ));
        let options = self.options;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move { serve(listener, router, options, shutdown_rx).await });
        *running = Some(RunningServer {
            address: actual,
            shutdown: shutdown_tx,
            task,
        });
        Ok(actual)
    }

    pub async fn stop(&self) -> Result<()> {
        let running = self.running.lock().await.take();
        let Some(running) = running else {
            return Ok(());
        };
        let _ = running.shutdown.send(true);
        let mut task = running.task;
        if let Ok(joined) = timeout(Duration::from_secs(10), &mut task).await {
            joined
                .map_err(|error| AppError::wrap(ErrorType::Internal, "join admin server", error))?
                .map_err(|error| {
                    AppError::wrap(ErrorType::ServiceUnavailable, "serve admin API", error)
                })
        } else {
            task.abort();
            let _ = task.await;
            Err(AppError::new(
                ErrorType::Timeout,
                "timed out waiting for admin server shutdown",
            ))
        }
    }

    pub async fn address(&self) -> Option<SocketAddr> {
        self.running
            .lock()
            .await
            .as_ref()
            .map(|server| server.address)
    }
}

async fn serve(
    listener: TcpListener,
    router: Router,
    options: AdminServerOptions,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let router = router.clone();
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    if let Err(error) =
                        serve_connection(stream, router, options, connection_shutdown).await
                    {
                        tracing::debug!(%peer, error = %error, "Admin connection closed");
                    }
                });
            }
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = joined {
                    tracing::warn!(error = %error, "Admin connection task failed");
                }
            }
        }
    }

    while let Some(joined) = connections.join_next().await {
        if let Err(error) = joined {
            tracing::warn!(error = %error, "Admin connection task failed during shutdown");
        }
    }
    Ok(())
}

async fn serve_connection(
    stream: TcpStream,
    router: Router,
    options: AdminServerOptions,
    mut shutdown: watch::Receiver<bool>,
) -> std::result::Result<(), hyper::Error> {
    let io = TokioIo::new(IdleTimeoutIo::new(stream, options.idle_timeout));
    let service = TowerToHyperService::new(router);
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(Some(options.read_header_timeout));
    let connection = builder.serve_connection(io, service).with_upgrades();
    tokio::pin!(connection);

    tokio::select! {
        result = &mut connection => result,
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                connection.as_mut().graceful_shutdown();
                match timeout(options.write_timeout, &mut connection).await {
                    Ok(result) => result,
                    Err(_) => Ok(()),
                }
            } else {
                connection.await
            }
        }
    }
}

struct IdleTimeoutIo {
    stream: TcpStream,
    timeout: Duration,
    deadline: Pin<Box<Sleep>>,
}

impl IdleTimeoutIo {
    fn new(stream: TcpStream, timeout: Duration) -> Self {
        Self {
            stream,
            timeout,
            deadline: Box::pin(sleep_until(Instant::now() + timeout)),
        }
    }

    fn reset(&mut self) {
        self.deadline.as_mut().reset(Instant::now() + self.timeout);
    }

    fn timed_out(&mut self, context: &mut Context<'_>) -> bool {
        self.deadline.as_mut().poll(context).is_ready()
    }

    fn timeout_error() -> io::Error {
        io::Error::new(io::ErrorKind::TimedOut, "admin connection idle timeout")
    }
}

impl AsyncRead for IdleTimeoutIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.timed_out(context) {
            return Poll::Ready(Err(Self::timeout_error()));
        }
        let before = buffer.filled().len();
        match Pin::new(&mut this.stream).poll_read(context, buffer) {
            Poll::Ready(Ok(())) => {
                if buffer.filled().len() > before {
                    this.reset();
                }
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

impl AsyncWrite for IdleTimeoutIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.timed_out(context) {
            return Poll::Ready(Err(Self::timeout_error()));
        }
        match Pin::new(&mut this.stream).poll_write(context, buffer) {
            Poll::Ready(Ok(written)) => {
                if written > 0 {
                    this.reset();
                }
                Poll::Ready(Ok(written))
            }
            result => result,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.timed_out(context) {
            return Poll::Ready(Err(Self::timeout_error()));
        }
        Pin::new(&mut this.stream).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(context)
    }
}

fn normalize_bind_address(address: &str) -> String {
    address
        .strip_prefix(':')
        .map_or_else(|| address.to_owned(), |port| format!("0.0.0.0:{port}"))
}

#[cfg(test)]
mod tests {
    use axum::{Router, routing::get};
    use tokio::{
        io::AsyncReadExt,
        net::{TcpListener, TcpStream},
        time::{sleep, timeout},
    };

    use super::*;

    #[tokio::test]
    async fn server_binds_before_returning_and_stops_cleanly() {
        let server = AdminServer::new(
            "127.0.0.1:0",
            Router::new().route("/", get(|| async { "ok" })),
        );
        let address = server.start().await.unwrap();
        let body = reqwest::get(format!("http://{address}/"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(body, "ok");
        server.stop().await.unwrap();
        assert!(server.address().await.is_none());
    }

    #[tokio::test]
    async fn bind_failure_leaves_server_restartable_and_duplicate_start_conflicts() {
        let blocker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = blocker.local_addr().unwrap();
        let server = AdminServer::new(
            address.to_string(),
            Router::new().route("/", get(|| async { "ok" })),
        );

        let error = server.start().await.unwrap_err();
        assert_eq!(error.error_type, ErrorType::ServiceUnavailable);
        assert!(server.address().await.is_none());
        drop(blocker);

        assert_eq!(server.start().await.unwrap(), address);
        assert_eq!(
            server.start().await.unwrap_err().error_type,
            ErrorType::Conflict
        );
        server.stop().await.unwrap();
        assert_eq!(server.start().await.unwrap(), address);
        server.stop().await.unwrap();
    }

    #[test]
    fn legacy_port_only_bind_address_is_normalized() {
        assert_eq!(normalize_bind_address(":8080"), "0.0.0.0:8080");
        assert_eq!(normalize_bind_address("127.0.0.1:8080"), "127.0.0.1:8080");
    }

    #[tokio::test]
    async fn configured_handler_and_idle_timeouts_are_enforced() {
        let options = AdminServerOptions {
            read_header_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_millis(10),
            idle_timeout: Duration::from_millis(20),
        };
        let server = AdminServer::with_options(
            "127.0.0.1:0",
            Router::new().route(
                "/slow",
                get(|| async {
                    sleep(Duration::from_millis(100)).await;
                    "late"
                }),
            ),
            options,
        );
        let address = server.start().await.unwrap();

        let response = reqwest::get(format!("http://{address}/slow"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);

        let mut idle = TcpStream::connect(address).await.unwrap();
        sleep(Duration::from_millis(50)).await;
        let mut byte = [0_u8; 1];
        let closed = timeout(Duration::from_secs(1), idle.read(&mut byte))
            .await
            .unwrap();
        assert!(closed.is_err() || closed.unwrap() == 0);

        server.stop().await.unwrap();
    }
}
