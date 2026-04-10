//! Streaming body builders for Ollama-compatible NDJSON responses.

use crate::api::types::{ChatMessage, ChatResponse, GenerateResponse, now_iso};
use crate::dashboard::log_store::LogStore;
use axum::body::Body;
use std::convert::Infallible;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};
use tracing::{info, trace};

/// Build a streaming `Body` that emits Ollama chat NDJSON chunks from a channel.
pub fn chat_stream_body(
    model: String,
    mut rx: mpsc::Receiver<String>,
    request_id: String,
    log_store: LogStore,
) -> Body {
    let stream = async_stream::stream! {
        let start = Instant::now();
        let mut chunk_count: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut full_text = String::new();
        // Send empty keepalive chunks every 15s so the client doesn't time out
        // while claude is busy doing tool calls (which emit no content deltas).
        let mut keepalive = interval(Duration::from_secs(15));
        keepalive.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                biased;
                maybe_chunk = rx.recv() => {
                    match maybe_chunk {
                        Some(chunk) => {
                            chunk_count += 1;
                            total_bytes += chunk.len() as u64;
                            full_text.push_str(&chunk);

                            eprint!("{chunk}");

                            trace!(
                                request_id = %request_id,
                                chunk_num = chunk_count,
                                chunk_bytes = chunk.len(),
                                total_bytes_so_far = total_bytes,
                                "emitting chat stream chunk"
                            );

                            let resp = ChatResponse {
                                model: model.clone(),
                                created_at: now_iso(),
                                message: ChatMessage::assistant(chunk),
                                done: false,
                                done_reason: None,
                                total_duration: None,
                                load_duration: None,
                                prompt_eval_count: None,
                                prompt_eval_duration: None,
                                eval_count: None,
                                eval_duration: None,
                            };
                            let mut line = serde_json::to_string(&resp).unwrap();
                            line.push('\n');
                            yield Ok::<_, Infallible>(line);
                        }
                        None => break, // channel closed, claude subprocess finished
                    }
                }
                _ = keepalive.tick() => {
                    // Empty chunk to keep the HTTP stream alive during long tool-call phases
                    let resp = ChatResponse {
                        model: model.clone(),
                        created_at: now_iso(),
                        message: ChatMessage::assistant(String::new()),
                        done: false,
                        done_reason: None,
                        total_duration: None,
                        load_duration: None,
                        prompt_eval_count: None,
                        prompt_eval_duration: None,
                        eval_count: None,
                        eval_duration: None,
                    };
                    let mut line = serde_json::to_string(&resp).unwrap();
                    line.push('\n');
                    yield Ok::<_, Infallible>(line);
                }
            }
        }

        eprintln!("\n--- [ttyproxy] stream done ---");

        let elapsed = start.elapsed();
        let done_resp = ChatResponse {
            model: model.clone(),
            created_at: now_iso(),
            message: ChatMessage::assistant(""),
            done: true,
            done_reason: Some("stop".into()),
            total_duration: Some(elapsed.as_nanos() as u64),
            load_duration: Some(0),
            prompt_eval_count: Some(0),
            prompt_eval_duration: Some(0),
            eval_count: Some(chunk_count as u32),
            eval_duration: Some(elapsed.as_nanos() as u64),
        };
        let line = serde_json::to_string(&done_resp).unwrap() + "\n";

        info!(
            request_id = %request_id,
            total_chunks = chunk_count,
            total_bytes = total_bytes,
            elapsed_ms = elapsed.as_millis() as u64,
            "chat stream completed"
        );

        log_store.log_outgoing_response(
            &request_id,
            "/api/chat",
            &model,
            &format!("{}B, {} chunks in {:.1}s", total_bytes, chunk_count, elapsed.as_secs_f64()),
            &full_text,
            elapsed.as_millis() as u64,
            total_bytes,
            Some(chunk_count),
        );

        yield Ok::<_, Infallible>(line);
    };

    Body::from_stream(stream)
}

/// Build a streaming `Body` that emits Ollama generate NDJSON chunks from a channel.
pub fn generate_stream_body(
    model: String,
    mut rx: mpsc::Receiver<String>,
    request_id: String,
    log_store: LogStore,
) -> Body {
    let stream = async_stream::stream! {
        let start = Instant::now();
        let mut chunk_count: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut full_text = String::new();
        let mut keepalive = interval(Duration::from_secs(15));
        keepalive.tick().await;

        loop {
            tokio::select! {
                biased;
                maybe_chunk = rx.recv() => {
                    match maybe_chunk {
                        Some(chunk) => {
                            chunk_count += 1;
                            total_bytes += chunk.len() as u64;
                            full_text.push_str(&chunk);

                            eprint!("{chunk}");

                            trace!(
                                request_id = %request_id,
                                chunk_num = chunk_count,
                                chunk_bytes = chunk.len(),
                                total_bytes_so_far = total_bytes,
                                "emitting generate stream chunk"
                            );

                            let resp = GenerateResponse {
                                model: model.clone(),
                                created_at: now_iso(),
                                response: chunk,
                                done: false,
                                done_reason: None,
                                context: None,
                                total_duration: None,
                                load_duration: None,
                                prompt_eval_count: None,
                                prompt_eval_duration: None,
                                eval_count: None,
                                eval_duration: None,
                            };
                            let mut line = serde_json::to_string(&resp).unwrap();
                            line.push('\n');
                            yield Ok::<_, Infallible>(line);
                        }
                        None => break,
                    }
                }
                _ = keepalive.tick() => {
                    let resp = GenerateResponse {
                        model: model.clone(),
                        created_at: now_iso(),
                        response: String::new(),
                        done: false,
                        done_reason: None,
                        context: None,
                        total_duration: None,
                        load_duration: None,
                        prompt_eval_count: None,
                        prompt_eval_duration: None,
                        eval_count: None,
                        eval_duration: None,
                    };
                    let mut line = serde_json::to_string(&resp).unwrap();
                    line.push('\n');
                    yield Ok::<_, Infallible>(line);
                }
            }
        }

        eprintln!("\n--- [ttyproxy] stream done ---");

        let elapsed = start.elapsed();
        let done_resp = GenerateResponse {
            model: model.clone(),
            created_at: now_iso(),
            response: String::new(),
            done: true,
            done_reason: Some("stop".into()),
            context: Some(vec![]),
            total_duration: Some(elapsed.as_nanos() as u64),
            load_duration: Some(0),
            prompt_eval_count: Some(0),
            prompt_eval_duration: Some(0),
            eval_count: Some(chunk_count as u32),
            eval_duration: Some(elapsed.as_nanos() as u64),
        };
        let line = serde_json::to_string(&done_resp).unwrap() + "\n";

        info!(
            request_id = %request_id,
            total_chunks = chunk_count,
            total_bytes = total_bytes,
            elapsed_ms = elapsed.as_millis() as u64,
            "generate stream completed"
        );

        log_store.log_outgoing_response(
            &request_id,
            "/api/generate",
            &model,
            &format!("{}B, {} chunks in {:.1}s", total_bytes, chunk_count, elapsed.as_secs_f64()),
            &full_text,
            elapsed.as_millis() as u64,
            total_bytes,
            Some(chunk_count),
        );

        yield Ok::<_, Infallible>(line);
    };

    Body::from_stream(stream)
}
