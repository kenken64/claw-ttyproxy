//! AWS Bedrock backend runner.
//!
//! Calls the Bedrock InvokeModel / InvokeModelWithResponseStream APIs using
//! bearer-token authentication (`Authorization: Bearer <token>`).
//!
//! Streaming responses use the AWS Event Stream binary protocol; each frame
//! payload is `{"bytes":"<base64>"}` where the decoded content is a standard
//! Anthropic SSE event (same format the Claude CLI emits in stream-json mode).

use crate::api::types::ChatMessage;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use futures::StreamExt;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

pub struct BedrockRunner {
    client: reqwest::Client,
    endpoint_base: String,
    model_id: String,
    bearer_token: String,
    max_tokens: u32,
}

impl BedrockRunner {
    pub fn new(
        bearer_token: String,
        model_id: String,
        region: String,
        max_tokens: u32,
        timeout_ms: u64,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(timeout_ms))
            .build()
            .expect("failed to build reqwest client for bedrock");
        let endpoint_base = format!("https://bedrock-runtime.{region}.amazonaws.com");
        info!(
            model = %model_id,
            region = %region,
            endpoint = %endpoint_base,
            max_tokens,
            "BedrockRunner initialized"
        );
        Self {
            client,
            endpoint_base,
            model_id,
            bearer_token,
            max_tokens,
        }
    }

    // -----------------------------------------------------------------------
    // Public entry points
    // -----------------------------------------------------------------------

    pub async fn run_blocking(
        &self,
        prompt: &str,
        request_id: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.invoke(self.body_from_prompt(prompt), request_id).await
    }

    pub async fn run_blocking_chat(
        &self,
        messages: &[ChatMessage],
        request_id: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.invoke(self.body_from_messages(messages), request_id)
            .await
    }

    pub async fn run_streaming(
        &self,
        prompt: &str,
        request_id: &str,
    ) -> Result<mpsc::Receiver<String>, Box<dyn std::error::Error + Send + Sync>> {
        self.invoke_stream(self.body_from_prompt(prompt), request_id)
            .await
    }

    pub async fn run_streaming_chat(
        &self,
        messages: &[ChatMessage],
        request_id: &str,
    ) -> Result<mpsc::Receiver<String>, Box<dyn std::error::Error + Send + Sync>> {
        self.invoke_stream(self.body_from_messages(messages), request_id)
            .await
    }

    // -----------------------------------------------------------------------
    // Request body builders
    // -----------------------------------------------------------------------

    fn body_from_prompt(&self, prompt: &str) -> serde_json::Value {
        serde_json::json!({
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": self.max_tokens,
        })
    }

    fn body_from_messages(&self, messages: &[ChatMessage]) -> serde_json::Value {
        // Collect system turns into the top-level `system` field.
        let system_parts: Vec<&str> = messages
            .iter()
            .filter(|m| m.role == "system")
            .map(|m| m.content.as_str())
            .collect();

        // Pass user/assistant turns as-is in the messages array.
        let anthropic_messages: Vec<serde_json::Value> = messages
            .iter()
            .filter(|m| m.role == "user" || m.role == "assistant")
            .map(|m| serde_json::json!({"role": m.role, "content": m.content}))
            .collect();

        let mut body = serde_json::json!({
            "anthropic_version": "bedrock-2023-05-31",
            "messages": anthropic_messages,
            "max_tokens": self.max_tokens,
        });
        if !system_parts.is_empty() {
            body["system"] = serde_json::Value::String(system_parts.join("\n\n"));
        }
        body
    }

    // -----------------------------------------------------------------------
    // Shared HTTP implementations
    // -----------------------------------------------------------------------

    async fn invoke(
        &self,
        body: serde_json::Value,
        request_id: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/model/{}/invoke", self.endpoint_base, self.model_id);
        let start = Instant::now();
        info!(request_id = %request_id, %url, "bedrock invoke");
        debug!(request_id = %request_id, %body, "bedrock invoke body");

        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            error!(request_id = %request_id, %status, %text, "bedrock invoke error");
            return Err(format!("Bedrock {status}: {text}").into());
        }

        let val: serde_json::Value = resp.json().await?;
        let elapsed = start.elapsed();
        debug!(request_id = %request_id, response = %val, "bedrock invoke response");
        info!(
            request_id = %request_id,
            elapsed_ms = elapsed.as_millis() as u64,
            "bedrock invoke complete"
        );

        // Anthropic response: content[].type == "text" → content[].text
        let text = val["content"]
            .as_array()
            .and_then(|blocks| {
                blocks
                    .iter()
                    .filter_map(|b| {
                        if b["type"].as_str() == Some("text") {
                            b["text"].as_str().map(|s| s.to_string())
                        } else {
                            None
                        }
                    })
                    .reduce(|mut acc, t| {
                        acc.push_str(&t);
                        acc
                    })
            })
            .unwrap_or_default();

        Ok(text)
    }

    async fn invoke_stream(
        &self,
        body: serde_json::Value,
        request_id: &str,
    ) -> Result<mpsc::Receiver<String>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/model/{}/invoke-with-response-stream",
            self.endpoint_base, self.model_id
        );
        info!(request_id = %request_id, %url, "bedrock invoke-with-response-stream");
        debug!(request_id = %request_id, %body, "bedrock stream body");

        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            error!(request_id = %request_id, %status, %text, "bedrock stream error");
            return Err(format!("Bedrock {status}: {text}").into());
        }

        let (tx, rx) = mpsc::channel::<String>(256);
        let req_id = request_id.to_string();
        let start = Instant::now();

        tokio::spawn(async move {
            let mut byte_stream = resp.bytes_stream();
            let mut buf: Vec<u8> = Vec::new();
            let mut chunk_count: u64 = 0;
            let mut total_bytes: u64 = 0;

            while let Some(result) = byte_stream.next().await {
                match result {
                    Ok(bytes) => {
                        buf.extend_from_slice(&bytes);
                        // Drain complete event-stream frames from the buffer.
                        loop {
                            match parse_event_frame(&buf) {
                                Some((payload, consumed)) => {
                                    buf.drain(..consumed);
                                    if let Some(text) =
                                        extract_text(&payload, &req_id)
                                    {
                                        if !text.is_empty() {
                                            chunk_count += 1;
                                            total_bytes += text.len() as u64;
                                            trace!(
                                                request_id = %req_id,
                                                chunk = chunk_count,
                                                bytes = text.len(),
                                                "bedrock chunk"
                                            );
                                            if tx.send(text).await.is_err() {
                                                warn!(request_id = %req_id, "channel closed");
                                                return;
                                            }
                                        }
                                    }
                                }
                                None => break, // need more bytes
                            }
                        }
                    }
                    Err(e) => {
                        error!(request_id = %req_id, error = %e, "bedrock stream read error");
                        break;
                    }
                }
            }

            info!(
                request_id = %req_id,
                total_chunks = chunk_count,
                total_bytes,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "bedrock stream finished"
            );
        });

        Ok(rx)
    }
}

// ---------------------------------------------------------------------------
// AWS Event Stream binary frame parser
// ---------------------------------------------------------------------------
//
// Frame layout (all multi-byte integers are big-endian):
//   [0..4]   total_length  (u32) – byte length of the entire message
//   [4..8]   headers_length (u32) – byte length of the headers section
//   [8..12]  prelude_crc   (u32) – CRC32 of bytes [0..8] (skipped here)
//   [12 .. 12+headers_length]  headers (variable)
//   [12+headers_length .. total_length-4]  payload (JSON)
//   [total_length-4 .. total_length]       message_crc (u32, skipped)

fn parse_event_frame(buf: &[u8]) -> Option<(Vec<u8>, usize)> {
    if buf.len() < 12 {
        return None;
    }
    let total_len = u32::from_be_bytes(buf[0..4].try_into().ok()?) as usize;
    let headers_len = u32::from_be_bytes(buf[4..8].try_into().ok()?) as usize;

    if buf.len() < total_len {
        return None; // incomplete frame
    }

    let payload_start = 12 + headers_len;
    let payload_end = total_len.checked_sub(4)?;

    let payload = if payload_start <= payload_end {
        buf[payload_start..payload_end].to_vec()
    } else {
        vec![]
    };

    Some((payload, total_len))
}

/// Decode an event-stream payload and return the text content, if any.
///
/// Normal chunk format:  `{"bytes": "<base64 Anthropic SSE JSON>"}`
/// Error frame format:   `{"__type": "...", "message": "..."}`
fn extract_text(payload: &[u8], request_id: &str) -> Option<String> {
    if payload.is_empty() {
        return None;
    }

    let outer: serde_json::Value = serde_json::from_slice(payload).ok()?;

    // Error event
    if let Some(err_type) = outer.get("__type").and_then(|v| v.as_str()) {
        let msg = outer
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown bedrock error");
        error!(request_id = %request_id, error_type = %err_type, %msg, "bedrock error event");
        return Some(format!("[Bedrock error {err_type}: {msg}]"));
    }

    // Normal chunk: base64-wrapped Anthropic SSE event
    let b64 = outer.get("bytes")?.as_str()?;
    let decoded = B64.decode(b64).ok()?;
    let inner: serde_json::Value = serde_json::from_slice(&decoded).ok()?;

    let event_type = inner.get("type")?.as_str()?;
    debug!(request_id = %request_id, %event_type, "bedrock inner event");

    match event_type {
        "content_block_delta" => inner
            .get("delta")
            .and_then(|d| d.get("text"))
            .and_then(|t| t.as_str())
            .map(|s| s.to_string()),
        _ => None,
    }
}
