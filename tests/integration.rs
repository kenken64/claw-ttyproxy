//! Integration tests for ttyproxy.
//!
//! These tests spin up the full API server using a mock `claude` binary
//! so no real Claude Code invocation is needed.

use reqwest::Client;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

use ttyproxy::api::handlers::AppState;
use ttyproxy::dashboard::log_store::LogStore;
use ttyproxy::proxy::claude::ClaudeRunner;
use ttyproxy::proxy::BackendRunner;
use ttyproxy::usage::{TokenUsageConfig, TokenUsageTracker};

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// Path to the compiled mock-claude binary.
fn mock_claude_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_mock-claude"))
}

/// Spin up the API server on a random free port. Returns the base URL.
async fn start_test_server() -> String {
    start_test_server_with_tracker(None).await
}

async fn start_test_server_with_tracker(
    token_usage_tracker: Option<Arc<TokenUsageTracker>>,
) -> String {
    let log_store = LogStore::new(100);
    let state = AppState {
        runner: Arc::new(Mutex::new(BackendRunner::Claude(ClaudeRunner::new(
            mock_claude_bin().to_string_lossy().to_string(),
            true, // dangerously_skip_permissions
        )))),
        model_name: "claude-code:latest".into(),
        log_store,
        token_usage_tracker,
    };

    let app = ttyproxy::build_api_router(state);

    // Bind to port 0 to get a random free port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    format!("http://{addr}")
}

fn client() -> Client {
    Client::new()
}

fn temp_db_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ttyproxy-{name}-{}-{nanos}.sqlite3",
        std::process::id()
    ))
}

fn exhausted_quota_tracker() -> Arc<TokenUsageTracker> {
    let db_path = temp_db_path("quota-exhausted");
    let tracker = TokenUsageTracker::open(TokenUsageConfig {
        db_path: db_path.clone(),
        redis_url: None,
        usage_channel: "openclaw:token_usage:v1".into(),
        quota_channel: "2ndbrain:token-quota".into(),
        openclaw_instance: "test-openclaw".into(),
        profile_id: Some("profile-1".into()),
        enforce_quota: true,
        flush_interval_ms: 1_000,
    })
    .unwrap();

    let conn = Connection::open(db_path).unwrap();
    conn.execute(
        "insert into token_quota_state (
            openclaw_instance,
            profile_id,
            llm_token_quota,
            llm_token_used,
            remaining_tokens,
            source,
            received_at,
            raw_payload
        ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            "test-openclaw",
            "profile-1",
            10_i64,
            10_i64,
            0_i64,
            "test",
            "2026-06-12T00:00:00Z",
            "{}"
        ],
    )
    .unwrap();

    tracker
}

fn paused_quota_tracker() -> Arc<TokenUsageTracker> {
    let db_path = temp_db_path("quota-paused");
    let tracker = TokenUsageTracker::open(TokenUsageConfig {
        db_path: db_path.clone(),
        redis_url: None,
        usage_channel: "openclaw:token_usage:v1".into(),
        quota_channel: "2ndbrain:token-quota".into(),
        openclaw_instance: "test-openclaw".into(),
        profile_id: Some("profile-1".into()),
        enforce_quota: true,
        flush_interval_ms: 1_000,
    })
    .unwrap();

    let conn = Connection::open(db_path).unwrap();
    conn.execute(
        "insert into token_quota_state (
            openclaw_instance,
            profile_id,
            llm_token_quota,
            llm_token_used,
            remaining_tokens,
            openclaw_tokens_paused,
            openclaw_tokens_paused_at,
            openclaw_tokens_pause_reason,
            source,
            received_at,
            raw_payload
        ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            "test-openclaw",
            "profile-1",
            100_i64,
            25_i64,
            75_i64,
            1_i64,
            "2026-06-21T10:00:00Z",
            "user_pause",
            "test",
            "2026-06-21T10:00:00Z",
            "{}"
        ],
    )
    .unwrap();

    tracker
}

// ---------------------------------------------------------------------------
// Health / metadata tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health_get() {
    let base = start_test_server().await;
    let resp = client().get(&base).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "Ollama is running");
}

#[tokio::test]
async fn test_health_head() {
    let base = start_test_server().await;
    let resp = client().head(&base).send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_version() {
    let base = start_test_server().await;
    let resp = client()
        .get(format!("{base}/api/version"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["version"], "0.1.0-ttyproxy");
}

// ---------------------------------------------------------------------------
// Tags (list models)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tags() {
    let base = start_test_server().await;
    let resp = client()
        .get(format!("{base}/api/tags"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();

    let models = body["models"].as_array().unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["name"], "claude-code:latest");
    assert_eq!(models[0]["details"]["family"], "claude");
    assert!(models[0]["digest"].as_str().unwrap().starts_with("sha256:"));
}

// ---------------------------------------------------------------------------
// Show model
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_show_model() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/show"))
        .json(&json!({ "model": "claude-code:latest" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();

    assert!(body["modelfile"].as_str().unwrap().contains("ttyproxy"));
    assert_eq!(body["details"]["family"], "claude");
}

#[tokio::test]
async fn test_show_model_no_model_field() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/show"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// ---------------------------------------------------------------------------
// Chat (non-streaming)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_chat_non_streaming() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "model": "claude-code:latest",
            "messages": [
                { "role": "user", "content": "Hello, world!" }
            ],
            "stream": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();

    assert_eq!(body["done"], true);
    assert_eq!(body["done_reason"], "stop");
    assert_eq!(body["model"], "claude-code:latest");
    assert_eq!(body["message"]["role"], "assistant");

    let content = body["message"]["content"].as_str().unwrap();
    assert!(content.contains("Mock response"));
    assert!(content.contains("dangerously_skip_permissions=true"));
}

#[tokio::test]
async fn test_chat_non_streaming_with_system() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "messages": [
                { "role": "system", "content": "You are a helpful assistant." },
                { "role": "user", "content": "What is 2+2?" }
            ],
            "stream": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["done"], true);

    // The system message should be included in the prompt sent to Claude
    let content = body["message"]["content"].as_str().unwrap();
    assert!(content.contains("Mock response"));
}

#[tokio::test]
async fn test_chat_non_streaming_multi_turn() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "messages": [
                { "role": "user", "content": "Hi" },
                { "role": "assistant", "content": "Hello!" },
                { "role": "user", "content": "How are you?" }
            ],
            "stream": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["done"], true);
    assert!(body["total_duration"].as_u64().is_some());
}

// ---------------------------------------------------------------------------
// Chat (streaming)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_chat_streaming() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "model": "claude-code:latest",
            "messages": [
                { "role": "user", "content": "Stream test" }
            ],
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/x-ndjson"
    );

    let body = resp.text().await.unwrap();
    let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();

    // Should have at least 2 lines: content chunks + final done
    assert!(lines.len() >= 2, "expected >=2 lines, got {}", lines.len());

    // Parse each line as JSON
    for line in &lines {
        let val: Value = serde_json::from_str(line).expect("each line should be valid JSON");
        assert_eq!(val["message"]["role"], "assistant");
        assert!(val["model"].as_str().is_some());
    }

    // Last line should have done=true
    let last: Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    assert_eq!(last["done"], true);
    assert_eq!(last["done_reason"], "stop");

    // Non-last lines should have done=false
    if lines.len() > 1 {
        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["done"], false);
    }
}

#[tokio::test]
async fn test_chat_streaming_default() {
    // stream defaults to true when omitted
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "messages": [{ "role": "user", "content": "default stream" }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/x-ndjson"
    );
}

// ---------------------------------------------------------------------------
// Generate (non-streaming)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_generate_non_streaming() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/generate"))
        .json(&json!({
            "model": "claude-code:latest",
            "prompt": "Write a haiku",
            "stream": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();

    assert_eq!(body["done"], true);
    assert_eq!(body["done_reason"], "stop");
    assert_eq!(body["model"], "claude-code:latest");

    let response_text = body["response"].as_str().unwrap();
    assert!(response_text.contains("Mock response"));
    assert!(response_text.contains("dangerously_skip_permissions=true"));
}

#[tokio::test]
async fn test_generate_non_streaming_with_system() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/generate"))
        .json(&json!({
            "prompt": "What is Rust?",
            "system": "You are a programming expert.",
            "stream": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["done"], true);
}

// ---------------------------------------------------------------------------
// Generate (streaming)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_generate_streaming() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/generate"))
        .json(&json!({
            "prompt": "Stream gen test",
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
    assert!(lines.len() >= 2);

    // Last line done=true
    let last: Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    assert_eq!(last["done"], true);
    assert!(last["context"].is_array());
}

// ---------------------------------------------------------------------------
// Embeddings (stub)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_embeddings() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/embeddings"))
        .json(&json!({
            "model": "claude-code:latest",
            "prompt": "Hello"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();

    let embeddings = body["embeddings"].as_array().unwrap();
    assert_eq!(embeddings.len(), 1);
    assert_eq!(embeddings[0].as_array().unwrap().len(), 384);
}

#[tokio::test]
async fn test_embed_alias() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/embed"))
        .json(&json!({ "model": "test", "input": "hi" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// ---------------------------------------------------------------------------
// Stub endpoints
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pull() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/pull"))
        .json(&json!({ "name": "claude-code:latest" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "success");
}

#[tokio::test]
async fn test_delete() {
    let base = start_test_server().await;
    let resp = client()
        .delete(format!("{base}/api/delete"))
        .json(&json!({ "name": "claude-code:latest" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_copy() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/copy"))
        .json(&json!({ "source": "claude-code:latest", "destination": "copy" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_chat_empty_messages() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "messages": [],
            "stream": false
        }))
        .send()
        .await
        .unwrap();
    // Should still succeed — Claude gets an empty prompt
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_chat_with_options() {
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "messages": [{ "role": "user", "content": "test" }],
            "stream": false,
            "options": {
                "temperature": 0.7,
                "num_predict": 100
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_chat_quota_exhausted_returns_assistant_message() {
    let base = start_test_server_with_tracker(Some(exhausted_quota_tracker())).await;
    let resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "messages": [{ "role": "user", "content": "hello" }],
            "stream": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["done"], true);
    assert_eq!(body["message"]["role"], "assistant");
    let content = body["message"]["content"].as_str().unwrap();
    assert!(content.contains("used all assigned AI credits"));
    assert!(content.contains("2ndBrain"));
    assert!(!content.contains("Mock response"));
}

#[tokio::test]
async fn test_chat_quota_exhausted_streams_assistant_message() {
    let base = start_test_server_with_tracker(Some(exhausted_quota_tracker())).await;
    let resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "messages": [{ "role": "user", "content": "hello" }],
            "stream": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/x-ndjson"
    );

    let body = resp.text().await.unwrap();
    let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
    assert!(lines.len() >= 2);

    let first: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["done"], false);
    assert_eq!(first["message"]["role"], "assistant");
    assert!(first["message"]["content"]
        .as_str()
        .unwrap()
        .contains("used all assigned AI credits"));

    let last: Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    assert_eq!(last["done"], true);
}

#[tokio::test]
async fn test_chat_paused_returns_assistant_message() {
    let base = start_test_server_with_tracker(Some(paused_quota_tracker())).await;
    let resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "messages": [{ "role": "user", "content": "hello" }],
            "stream": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["done"], true);
    assert_eq!(body["message"]["role"], "assistant");
    let content = body["message"]["content"].as_str().unwrap();
    assert!(content.contains("OpenClaw AI usage is paused"));
    assert!(content.contains("2ndBrain"));
    assert!(!content.contains("Mock response"));
}

#[tokio::test]
async fn test_model_name_passthrough() {
    // When a custom model name is specified, it should be echoed back
    let base = start_test_server().await;
    let resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "model": "my-custom-model",
            "messages": [{ "role": "user", "content": "test" }],
            "stream": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["model"], "my-custom-model");
}

// ---------------------------------------------------------------------------
// Dashboard log store
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_log_store_records_chat() {
    let log_store = LogStore::new(100);
    let state = AppState {
        runner: Arc::new(Mutex::new(BackendRunner::Claude(ClaudeRunner::new(
            mock_claude_bin().to_string_lossy().to_string(),
            true,
        )))),
        model_name: "claude-code:latest".into(),
        log_store: log_store.clone(),
        token_usage_tracker: None,
    };

    let app = ttyproxy::build_api_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let base = format!("http://{addr}");

    // Make a non-streaming chat request
    let _resp = client()
        .post(format!("{base}/api/chat"))
        .json(&json!({
            "messages": [{ "role": "user", "content": "log test" }],
            "stream": false
        }))
        .send()
        .await
        .unwrap();

    // Give the server a moment to process
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let entries = log_store.entries();
    // Should have at least an incoming request + outgoing response
    assert!(
        entries.len() >= 2,
        "expected >=2 log entries, got {}",
        entries.len()
    );

    let incoming = entries.iter().find(|e| e.direction == "incoming").unwrap();
    assert_eq!(incoming.endpoint, "/api/chat");
    assert_eq!(incoming.kind, "request");

    let outgoing = entries.iter().find(|e| e.direction == "outgoing").unwrap();
    assert_eq!(outgoing.endpoint, "/api/chat");
    assert_eq!(outgoing.kind, "response");
    assert!(outgoing.elapsed_ms.is_some());
    assert!(outgoing.bytes.unwrap() > 0);
}
