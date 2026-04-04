# claw-ttyproxy

An Ollama-compatible HTTP proxy that intercepts all Ollama API requests and routes them through **Claude Code CLI** as a subprocess. Any application that speaks the Ollama protocol (e.g. Open WebUI, Continue, etc.) can use Claude Code as its backend without any code changes.

```
                                  claw-ttyproxy
                           +--------------------------+
                           |                          |
  OpenClaw / Open WebUI    |   :11435  Ollama API     |    Claude Code CLI
  or any Ollama client     |   +-----------------+    |    (subprocess)
          |                |   |  /api/chat       |   |         |
          |  HTTP POST     |   |  /api/generate   |   | stdin   |  stdout
          +--------------->|   |  /api/tags       |+--+-------->|-------+
          |                |   |  /api/show       |   | prompt  | stream|
          |  NDJSON stream |   |  /api/embeddings |   |         |  JSON |
          |<---------------+   +-----------------+    |<--------+-------+
          |                |                          |
          |                |   :11436  Dashboard      |
          |                |   +-----------------+    |
          |   Browser      |   |  Live log viewer |   |
          +--------------->|   |  SSE stream      |   |
                           |   +-----------------+    |
                           |                          |
                           +--------------------------+
```

## Architecture

```
claw-cc-ttyproxy/
+-- Cargo.toml
+-- README.md
+-- src/
|   +-- main.rs                    # Entrypoint - starts API + dashboard servers
|   +-- lib.rs                     # Library root - exports build_api_router()
|   +-- config.rs                  # Env-based configuration
|   +-- api/
|   |   +-- mod.rs
|   |   +-- types.rs               # Ollama request/response types + helpers
|   |   +-- handlers.rs            # Route handlers for all Ollama endpoints
|   +-- proxy/
|   |   +-- mod.rs
|   |   +-- claude.rs              # Claude Code CLI subprocess runner (TTY passthrough)
|   |   +-- stream.rs              # NDJSON streaming body builders
|   +-- middleware/
|   |   +-- mod.rs
|   |   +-- logging.rs             # HTTP request/response logging middleware
|   +-- dashboard/
|       +-- mod.rs                 # Dashboard router + SSE endpoint
|       +-- log_store.rs           # Thread-safe ring buffer + broadcast for live logs
|       +-- index.html             # Embedded web UI (single-file, no build step)
+-- tests/
    +-- mock_claude.rs             # Mock claude CLI binary for testing
    +-- integration.rs             # 23 integration tests against all endpoints
```

### Module Responsibilities

| Module | Purpose |
|--------|---------|
| `api::types` | All Ollama-compatible request/response structs, `messages_to_prompt()` converter, helpers |
| `api::handlers` | Route handlers for `/api/chat`, `/api/generate`, `/api/tags`, `/api/show`, `/api/embeddings`, stubs for `/api/pull`, `/api/delete`, `/api/copy` |
| `proxy::claude` | Spawns `claude -p` as a child process. Stdin receives the prompt, stdout is parsed for response chunks. Stderr is inherited for TTY passthrough so Claude's progress UI appears in the host terminal |
| `proxy::stream` | Converts a `tokio::sync::mpsc::Receiver<String>` of Claude chunks into Ollama-compatible NDJSON `Body` streams |
| `middleware::logging` | Axum middleware that logs every HTTP request/response with headers, timing, and unique request IDs |
| `dashboard::log_store` | Thread-safe bounded ring buffer (default 500 entries) with `tokio::sync::broadcast` for real-time SSE push to connected browsers |
| `dashboard` | Serves the web UI on a separate port. HTML/CSS/JS is embedded at compile time via `include_str!()` |
| `config` | Reads all configuration from environment variables with sensible defaults |

### Request Flow

```
1. Client sends POST /api/chat with Ollama JSON body
                |
2. logging middleware assigns request ID, logs headers + timing
                |
3. handlers::chat() extracts messages, model, stream flag
                |
4. messages_to_prompt() converts chat messages to a single prompt string
                |
5. log_store.log_incoming_request() -> dashboard SSE broadcast
                |
6. ClaudeRunner::run_streaming() or run_blocking()
   |-- spawns: claude -p --output-format stream-json --dangerously-skip-permissions
   |-- writes prompt to stdin, closes stdin
   |-- reads stdout line-by-line, parses JSON events
   |-- sends text chunks through mpsc channel
   |-- stderr inherited -> host TTY (you see Claude's progress)
                |
7. stream::chat_stream_body() wraps channel into NDJSON Body
   |-- each chunk -> {"model":"...","message":{"role":"assistant","content":"chunk"},"done":false}
   |-- final    -> {"model":"...","message":{"role":"assistant","content":""},"done":true}
                |
8. log_store.log_outgoing_response() -> dashboard SSE broadcast
                |
9. Response streamed back to client
```

## Features

- **Full Ollama API compatibility** - drop-in replacement for any Ollama client
- **Streaming & non-streaming** - supports both `stream: true` (NDJSON chunks) and `stream: false` (single JSON response)
- **TTY passthrough** - Claude Code's stderr goes directly to your terminal so you see its progress UI
- **Live web dashboard** - two-panel view of incoming requests and outgoing responses with real-time SSE updates
- **Comprehensive logging** - request IDs, headers, full body dumps, timing, chunk counts at configurable verbosity (`RUST_LOG=trace|debug|info`)
- **33 tests** - 10 unit tests + 23 integration tests using a mock Claude binary
- **`--dangerously-skip-permissions`** - enabled by default so Claude runs non-interactively

## Supported Endpoints

| Method | Endpoint | Status |
|--------|----------|--------|
| `GET` | `/` | Health check ("Ollama is running") |
| `HEAD` | `/` | Health check |
| `GET` | `/api/version` | Returns proxy version |
| `GET` | `/api/tags` | Lists `claude-code:latest` model |
| `POST` | `/api/show` | Returns model details |
| `POST` | `/api/chat` | Chat completion (streaming + non-streaming) |
| `POST` | `/api/generate` | Text generation (streaming + non-streaming) |
| `POST` | `/api/embeddings` | Stub (returns zero vector) |
| `POST` | `/api/embed` | Stub (alias) |
| `POST` | `/api/pull` | Stub (returns success) |
| `DELETE` | `/api/delete` | Stub (returns 200) |
| `POST` | `/api/copy` | Stub (returns 200) |

## Getting Started

### Prerequisites

- [Rust](https://rustup.rs/) 1.70+
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) installed and authenticated (`claude` in PATH)

### Install & Run

```bash
git clone https://github.com/kenken64/claw-ttyproxy.git
cd claw-ttyproxy
cargo build --release
cargo run --release --bin ttyproxy
```

The proxy starts two servers:

| Port | Service |
|------|---------|
| **11435** | Ollama-compatible API |
| **11436** | Web dashboard |

### Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDR` | `127.0.0.1:11435` | API server bind address |
| `CLAUDE_BIN` | `claude` | Path to Claude Code CLI binary |
| `MODEL_NAME` | `claude-code:latest` | Model name reported in API responses |
| `DANGEROUSLY_SKIP_PERMISSIONS` | `true` | Pass `--dangerously-skip-permissions` to Claude |
| `TTYPROXY_SHELL` | auto-detect | Shell mode: `cmd`, `powershell`, `bash`, `none` (see below) |
| `RUST_LOG` | `ttyproxy=debug` | Log verbosity (`trace`, `debug`, `info`, `warn`, `error`) |

### Cross-Platform Shell Support

The proxy auto-detects the correct way to invoke the `claude` CLI based on your OS:

| Platform | Default shell mode | How `claude` is invoked |
|----------|-------------------|------------------------|
| **Linux / macOS** | `direct` | `claude -p --output-format ...` (binary called directly) |
| **Windows** | `cmd` | `cmd /C claude -p --output-format ...` (needed for `.cmd` wrappers) |

Override with `TTYPROXY_SHELL` if auto-detect doesn't work for your setup:

```bash
# Force PowerShell on Windows
TTYPROXY_SHELL=powershell cargo run --release --bin ttyproxy

# Force bash (e.g. WSL, Git Bash on Windows)
TTYPROXY_SHELL=bash cargo run --release --bin ttyproxy

# Call the binary directly (skip shell wrapper)
TTYPROXY_SHELL=none cargo run --release --bin ttyproxy
```

### Example

```bash
# Point any Ollama client at the proxy
curl http://127.0.0.1:11435/api/chat \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-code:latest",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": false
  }'
```

### Log Levels

```bash
RUST_LOG=trace cargo run --bin ttyproxy   # Every chunk + raw Claude stdout lines
RUST_LOG=ttyproxy=debug cargo run --bin ttyproxy  # Full request/response bodies + headers
RUST_LOG=ttyproxy=info cargo run --bin ttyproxy   # Request summaries and timing only
```

## Dashboard

Open **http://127.0.0.1:11436** in your browser.

- **Left panel**: Incoming requests (client -> ttyproxy)
- **Right panel**: Outgoing responses (Claude Code -> client)
- Click any entry to see full detail (complete prompt/response body, timing, byte counts)
- Filter by request ID, endpoint, or model name
- Real-time updates via Server-Sent Events (SSE)
- Auto-scroll toggle

## Testing

Tests use a mock `claude` binary that returns deterministic responses — no real Claude API calls needed.

```bash
cargo test
```

```
running 10 tests         # unit tests (types, helpers)
running 23 tests         # integration tests (all endpoints, streaming, dashboard)
test result: ok. 33 passed; 0 failed
```

## Tech Stack

- **[Axum](https://github.com/tokio-rs/axum)** - HTTP framework
- **[Tokio](https://tokio.rs/)** - Async runtime + subprocess management
- **[Serde](https://serde.rs/)** - JSON serialization
- **[tracing](https://tracing.rs/)** - Structured logging
- **[tower-http](https://github.com/tower-rs/tower-http)** - CORS middleware
- **[async-stream](https://docs.rs/async-stream)** - NDJSON streaming bodies
- **[reqwest](https://docs.rs/reqwest)** - HTTP client (dev, for integration tests)

## License

MIT
