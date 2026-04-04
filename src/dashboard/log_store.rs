//! Thread-safe log store with broadcast for the web dashboard.
//!
//! Keeps a bounded ring buffer of log entries and broadcasts new entries
//! to all connected SSE clients.

use chrono::Utc;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub id: u64,
    pub timestamp: String,
    /// "request" | "response" | "error" | "info"
    pub kind: String,
    /// Which panel: "incoming" (from client) or "outgoing" (from Claude)
    pub direction: String,
    pub request_id: String,
    pub endpoint: String,
    pub model: String,
    pub summary: String,
    /// Full detail (request body, response body, etc.)
    pub detail: String,
    pub elapsed_ms: Option<u64>,
    pub bytes: Option<u64>,
    pub chunks: Option<u64>,
}

#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Inner>,
}

struct Inner {
    entries: RwLock<VecDeque<LogEntry>>,
    max_entries: usize,
    counter: std::sync::atomic::AtomicU64,
    tx: broadcast::Sender<LogEntry>,
}

impl LogStore {
    pub fn new(max_entries: usize) -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            inner: Arc::new(Inner {
                entries: RwLock::new(VecDeque::with_capacity(max_entries)),
                max_entries,
                counter: std::sync::atomic::AtomicU64::new(1),
                tx,
            }),
        }
    }

    /// Push a new log entry. Broadcasts to SSE listeners and stores in ring buffer.
    pub fn push(&self, mut entry: LogEntry) {
        entry.id = self
            .inner
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if entry.timestamp.is_empty() {
            entry.timestamp = Utc::now().to_rfc3339();
        }

        // Store
        if let Ok(mut entries) = self.inner.entries.write() {
            if entries.len() >= self.inner.max_entries {
                entries.pop_front();
            }
            entries.push_back(entry.clone());
        }

        // Broadcast (ignore errors = no listeners)
        let _ = self.inner.tx.send(entry);
    }

    /// Get all stored entries (most recent last).
    pub fn entries(&self) -> Vec<LogEntry> {
        self.inner
            .entries
            .read()
            .map(|e| e.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Subscribe to new entries via a broadcast receiver.
    pub fn subscribe(&self) -> broadcast::Receiver<LogEntry> {
        self.inner.tx.subscribe()
    }

    // -----------------------------------------------------------------------
    // Convenience builders
    // -----------------------------------------------------------------------

    pub fn log_incoming_request(
        &self,
        request_id: &str,
        endpoint: &str,
        model: &str,
        summary: &str,
        detail: &str,
    ) {
        self.push(LogEntry {
            id: 0,
            timestamp: String::new(),
            kind: "request".into(),
            direction: "incoming".into(),
            request_id: request_id.into(),
            endpoint: endpoint.into(),
            model: model.into(),
            summary: summary.into(),
            detail: detail.into(),
            elapsed_ms: None,
            bytes: None,
            chunks: None,
        });
    }

    pub fn log_outgoing_response(
        &self,
        request_id: &str,
        endpoint: &str,
        model: &str,
        summary: &str,
        detail: &str,
        elapsed_ms: u64,
        bytes: u64,
        chunks: Option<u64>,
    ) {
        self.push(LogEntry {
            id: 0,
            timestamp: String::new(),
            kind: "response".into(),
            direction: "outgoing".into(),
            request_id: request_id.into(),
            endpoint: endpoint.into(),
            model: model.into(),
            summary: summary.into(),
            detail: detail.into(),
            elapsed_ms: Some(elapsed_ms),
            bytes: Some(bytes),
            chunks,
        });
    }

    pub fn log_error(
        &self,
        request_id: &str,
        endpoint: &str,
        error: &str,
    ) {
        self.push(LogEntry {
            id: 0,
            timestamp: String::new(),
            kind: "error".into(),
            direction: "outgoing".into(),
            request_id: request_id.into(),
            endpoint: endpoint.into(),
            model: String::new(),
            summary: format!("Error: {}", error),
            detail: error.into(),
            elapsed_ms: None,
            bytes: None,
            chunks: None,
        });
    }
}
