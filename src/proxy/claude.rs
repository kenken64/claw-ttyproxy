//! Claude Code CLI subprocess runner.
//!
//! Spawns `claude -p` as a child process, sends the prompt via stdin,
//! and captures stdout. Stderr is inherited so Claude's progress UI
//! appears on the host terminal (TTY passthrough).
//!
//! ## Cross-platform shell support
//!
//! On **Windows**, `claude` is typically installed as a `.cmd` or `.ps1` wrapper
//! (e.g. via npm global install). These wrappers can't be executed directly by
//! `Command::new()` — they must be run through a shell. We detect the OS at
//! runtime and spawn via `cmd /C` (Windows) or directly (Unix/macOS).
//!
//! Override the shell with `TTYPROXY_SHELL`:
//! - `TTYPROXY_SHELL=powershell` — use `powershell -Command` on Windows
//! - `TTYPROXY_SHELL=bash` — force bash (useful on WSL)
//! - `TTYPROXY_SHELL=none` — call the binary directly (no shell wrapper)

use std::process::Stdio;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

/// How to invoke the claude binary.
#[derive(Debug, Clone)]
enum ShellMode {
    /// Call the binary directly (Linux/macOS default, or explicit `none`).
    Direct,
    /// Wrap with `cmd /C <binary> <args>` (Windows default).
    Cmd,
    /// Wrap with `powershell -NoProfile -Command <binary> <args>`.
    PowerShell,
    /// Wrap with `bash -c "<binary> <args>"`.
    Bash,
}

pub struct ClaudeRunner {
    claude_bin: String,
    dangerously_skip_permissions: bool,
    shell_mode: ShellMode,
}

impl ClaudeRunner {
    pub fn new(claude_bin: String, dangerously_skip_permissions: bool) -> Self {
        let shell_mode = Self::detect_shell_mode();

        info!(
            binary = %claude_bin,
            dangerously_skip_permissions = dangerously_skip_permissions,
            shell_mode = ?shell_mode,
            os = std::env::consts::OS,
            "ClaudeRunner initialized"
        );
        if dangerously_skip_permissions {
            warn!("--dangerously-skip-permissions is ENABLED — claude will run without permission prompts");
        }
        Self {
            claude_bin,
            dangerously_skip_permissions,
            shell_mode,
        }
    }

    /// Detect the appropriate shell mode for the current platform.
    /// Can be overridden with `TTYPROXY_SHELL` env var.
    fn detect_shell_mode() -> ShellMode {
        if let Ok(shell) = std::env::var("TTYPROXY_SHELL") {
            return match shell.to_lowercase().as_str() {
                "cmd" => ShellMode::Cmd,
                "powershell" | "pwsh" => ShellMode::PowerShell,
                "bash" | "sh" => ShellMode::Bash,
                "none" | "direct" => ShellMode::Direct,
                _ => {
                    warn!(shell = %shell, "unknown TTYPROXY_SHELL value, falling back to auto-detect");
                    Self::auto_detect_shell()
                }
            };
        }
        Self::auto_detect_shell()
    }

    fn auto_detect_shell() -> ShellMode {
        match std::env::consts::OS {
            "windows" => ShellMode::Cmd,
            _ => ShellMode::Direct, // Linux, macOS — claude binary is directly executable
        }
    }

    /// Build the base args for a claude invocation.
    fn base_args(&self, output_format: &str) -> Vec<String> {
        let mut args = vec![
            "-p".to_string(),
            "--output-format".to_string(),
            output_format.to_string(),
        ];
        if self.dangerously_skip_permissions {
            args.push("--dangerously-skip-permissions".to_string());
        }
        args
    }

    /// Create a `Command` configured for the detected shell mode.
    fn build_command(&self, claude_args: &[String]) -> Command {
        match &self.shell_mode {
            ShellMode::Direct => {
                let mut cmd = Command::new(&self.claude_bin);
                cmd.args(claude_args);
                cmd
            }
            ShellMode::Cmd => {
                let mut cmd = Command::new("cmd");
                cmd.arg("/C");
                cmd.arg(&self.claude_bin);
                cmd.args(claude_args);
                cmd
            }
            ShellMode::PowerShell => {
                let mut cmd = Command::new("powershell");
                cmd.args(["-NoProfile", "-NonInteractive", "-Command"]);
                // Build a single command string for PowerShell
                let full_cmd = format!(
                    "& '{}' {}",
                    self.claude_bin,
                    claude_args
                        .iter()
                        .map(|a| format!("'{}'", a))
                        .collect::<Vec<_>>()
                        .join(" ")
                );
                cmd.arg(&full_cmd);
                cmd
            }
            ShellMode::Bash => {
                let mut cmd = Command::new("bash");
                cmd.arg("-c");
                let full_cmd = format!(
                    "'{}' {}",
                    self.claude_bin,
                    claude_args
                        .iter()
                        .map(|a| format!("'{}'", a))
                        .collect::<Vec<_>>()
                        .join(" ")
                );
                cmd.arg(&full_cmd);
                cmd
            }
        }
    }

    /// Run claude in streaming mode.
    ///
    /// Returns a channel receiver that yields text chunks as they arrive.
    /// Stderr is inherited (goes straight to the host TTY).
    pub async fn run_streaming(
        &self,
        prompt: &str,
        request_id: &str,
    ) -> Result<mpsc::Receiver<String>, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        info!(
            request_id = %request_id,
            binary = %self.claude_bin,
            prompt_bytes = prompt.len(),
            mode = "stream-json",
            "spawning claude subprocess"
        );
        debug!(request_id = %request_id, prompt = %prompt, "full prompt content");

        let args = self.base_args("stream-json");
        debug!(request_id = %request_id, args = ?args, shell_mode = ?self.shell_mode, "claude args");

        let mut child = self
            .build_command(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let pid = child.id().unwrap_or(0);
        info!(request_id = %request_id, pid = pid, "claude subprocess spawned");

        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            stdin.write_all(prompt.as_bytes()).await?;
            stdin.shutdown().await?;
            debug!(request_id = %request_id, "stdin written and closed");
        }

        let stdout = child.stdout.take().ok_or("failed to capture stdout")?;
        let reader = BufReader::new(stdout);
        let mut lines = reader.lines();

        let (tx, rx) = mpsc::channel::<String>(256);
        let req_id = request_id.to_string();

        tokio::spawn(async move {
            let mut chunk_count: u64 = 0;
            let mut total_bytes: u64 = 0;
            let mut line_count: u64 = 0;

            while let Ok(Some(line)) = lines.next_line().await {
                line_count += 1;
                trace!(
                    request_id = %req_id,
                    line_num = line_count,
                    line_bytes = line.len(),
                    "raw stdout line from claude"
                );
                debug!(
                    request_id = %req_id,
                    line_num = line_count,
                    line = %line,
                    "claude stdout"
                );

                if line.is_empty() {
                    continue;
                }

                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) {
                    let event_type =
                        val.get("type").and_then(|t| t.as_str()).unwrap_or("unknown");
                    trace!(request_id = %req_id, event_type = %event_type, "parsed claude event");

                    match event_type {
                        "content_block_delta" => {
                            if let Some(delta) = val.get("delta") {
                                if let Some(text) = delta.get("text").and_then(|t| t.as_str()) {
                                    chunk_count += 1;
                                    total_bytes += text.len() as u64;
                                    trace!(
                                        request_id = %req_id,
                                        chunk = chunk_count,
                                        chunk_bytes = text.len(),
                                        total_bytes = total_bytes,
                                        "content_block_delta chunk"
                                    );
                                    if tx.send(text.to_string()).await.is_err() {
                                        warn!(request_id = %req_id, "channel closed, aborting stream");
                                        break;
                                    }
                                }
                            }
                        }
                        "assistant" => {
                            if let Some(content) = val.get("content").and_then(|c| c.as_str()) {
                                if !content.is_empty() {
                                    chunk_count += 1;
                                    total_bytes += content.len() as u64;
                                    debug!(
                                        request_id = %req_id,
                                        chunk = chunk_count,
                                        content_bytes = content.len(),
                                        "assistant text content"
                                    );
                                    if tx.send(content.to_string()).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            if let Some(blocks) = val.get("content").and_then(|c| c.as_array()) {
                                for block in blocks {
                                    if let Some(text) =
                                        block.get("text").and_then(|t| t.as_str())
                                    {
                                        chunk_count += 1;
                                        total_bytes += text.len() as u64;
                                        debug!(
                                            request_id = %req_id,
                                            chunk = chunk_count,
                                            block_bytes = text.len(),
                                            "assistant content block"
                                        );
                                        if tx.send(text.to_string()).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        "result" => {
                            if let Some(result) = val.get("result").and_then(|r| r.as_str()) {
                                if !result.is_empty() {
                                    chunk_count += 1;
                                    total_bytes += result.len() as u64;
                                    info!(
                                        request_id = %req_id,
                                        result_bytes = result.len(),
                                        "final result received"
                                    );
                                    debug!(request_id = %req_id, result = %result, "result content");
                                    let _ = tx.send(result.to_string()).await;
                                }
                            }
                        }
                        "error" => {
                            if let Some(err) = val.get("error") {
                                let msg = err
                                    .get("message")
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("unknown error");
                                error!(
                                    request_id = %req_id,
                                    error_message = %msg,
                                    error_json = %err,
                                    "claude returned error"
                                );
                                let _ = tx.send(format!("[Error: {msg}]")).await;
                            }
                        }
                        _ => {
                            debug!(
                                request_id = %req_id,
                                event_type = %event_type,
                                "skipping non-content event"
                            );
                        }
                    }
                } else {
                    warn!(
                        request_id = %req_id,
                        line = %line,
                        "non-JSON line from claude stdout"
                    );
                    if !line.trim().is_empty() {
                        chunk_count += 1;
                        total_bytes += line.len() as u64;
                        if tx.send(line).await.is_err() {
                            break;
                        }
                    }
                }
            }

            let status = child.wait().await;
            let elapsed = start.elapsed();
            info!(
                request_id = %req_id,
                pid = pid,
                exit_status = ?status,
                elapsed_ms = elapsed.as_millis() as u64,
                total_chunks = chunk_count,
                total_bytes = total_bytes,
                total_lines = line_count,
                "claude subprocess finished (streaming)"
            );
        });

        Ok(rx)
    }

    /// Run claude in blocking mode - wait for full response.
    /// Stderr is inherited (TTY passthrough).
    pub async fn run_blocking(
        &self,
        prompt: &str,
        request_id: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        info!(
            request_id = %request_id,
            binary = %self.claude_bin,
            prompt_bytes = prompt.len(),
            mode = "text",
            "spawning claude subprocess (blocking)"
        );
        debug!(request_id = %request_id, prompt = %prompt, "full prompt content");

        let args = self.base_args("text");
        debug!(request_id = %request_id, args = ?args, shell_mode = ?self.shell_mode, "claude args");

        let mut child = self
            .build_command(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let pid = child.id().unwrap_or(0);
        info!(request_id = %request_id, pid = pid, "claude subprocess spawned (blocking)");

        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            stdin.write_all(prompt.as_bytes()).await?;
            stdin.shutdown().await?;
            debug!(request_id = %request_id, "stdin written and closed");
        }

        let mut stdout = child.stdout.take().ok_or("failed to capture stdout")?;
        let mut output = String::new();
        stdout.read_to_string(&mut output).await?;

        let status = child.wait().await?;
        let elapsed = start.elapsed();

        if status.success() {
            info!(
                request_id = %request_id,
                pid = pid,
                exit_code = 0,
                elapsed_ms = elapsed.as_millis() as u64,
                response_bytes = output.len(),
                "claude subprocess finished (blocking)"
            );
        } else {
            error!(
                request_id = %request_id,
                pid = pid,
                exit_status = %status,
                elapsed_ms = elapsed.as_millis() as u64,
                response_bytes = output.len(),
                "claude subprocess failed"
            );
        }

        debug!(request_id = %request_id, response = %output.trim(), "full claude response");

        Ok(output.trim().to_string())
    }
}
