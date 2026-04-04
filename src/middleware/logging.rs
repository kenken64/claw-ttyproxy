//! HTTP request/response logging middleware.

use axum::{
    extract::{ConnectInfo, Request},
    middleware::Next,
    response::Response,
};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{debug, info};

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a monotonically increasing request ID.
pub fn next_request_id() -> String {
    let seq = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("req-{seq}")
}

/// Axum middleware that logs every HTTP request/response with headers and timing.
pub async fn log_request(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let request_id = next_request_id();
    let method = request.method().clone();
    let uri = request.uri().clone();
    let version = request.version();
    let start = Instant::now();

    let headers: Vec<String> = request
        .headers()
        .iter()
        .map(|(k, v)| format!("  {}: {}", k, v.to_str().unwrap_or("<binary>")))
        .collect();

    info!(
        request_id = %request_id,
        method = %method,
        uri = %uri,
        version = ?version,
        remote_addr = %addr,
        "incoming request"
    );
    debug!(
        request_id = %request_id,
        headers = %headers.join("\n"),
        "request headers"
    );

    let response = next.run(request).await;

    let elapsed = start.elapsed();
    let status = response.status();

    info!(
        request_id = %request_id,
        method = %method,
        uri = %uri,
        status = status.as_u16(),
        elapsed_ms = elapsed.as_millis() as u64,
        "response sent"
    );

    let resp_headers: Vec<String> = response
        .headers()
        .iter()
        .map(|(k, v)| format!("  {}: {}", k, v.to_str().unwrap_or("<binary>")))
        .collect();
    debug!(
        request_id = %request_id,
        headers = %resp_headers.join("\n"),
        "response headers"
    );

    response
}
