# claw-ttyproxy

An Ollama-compatible HTTP proxy that intercepts all Ollama API requests and routes them through one of two backends — **Claude Code CLI** (as a subprocess) or **AWS Bedrock** (over HTTPS with bearer-token auth). Any application that speaks the Ollama protocol (e.g. Open WebUI, Continue, etc.) can use Claude as its backend without any code changes.

The backend is selected automatically: if `AWS_BEARER_TOKEN_BEDROCK` is set the proxy uses Bedrock, otherwise it falls back to the Claude CLI. See [Backends](#backends).

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
|   |   +-- mod.rs                 # BackendRunner enum (Claude CLI | Bedrock) dispatch
|   |   +-- claude.rs              # Claude Code CLI subprocess runner (TTY passthrough)
|   |   +-- bedrock.rs             # AWS Bedrock runner (InvokeModel + event-stream parser)
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
| `proxy::bedrock` | Calls the Bedrock `InvokeModel` / `InvokeModelWithResponseStream` APIs over HTTPS with `Authorization: Bearer <token>`; parses the AWS event-stream binary frames into Anthropic SSE text chunks |
| `proxy` | `BackendRunner` enum that dispatches each request to either the Claude CLI or Bedrock runner, chosen at startup |
| `proxy::stream` | Converts a `tokio::sync::mpsc::Receiver<String>` of response chunks into Ollama-compatible NDJSON `Body` streams |
| `middleware::logging` | Axum middleware that logs every HTTP request/response with headers, timing, and unique request IDs |
| `dashboard::log_store` | Thread-safe bounded ring buffer (default 500 entries) with `tokio::sync::broadcast` for real-time SSE push to connected browsers |
| `dashboard` | Serves the web UI on a separate port. HTML/CSS/JS is embedded at compile time via `include_str!()` |
| `config` | Reads all configuration from environment variables (and a `.env` file) with sensible defaults; selects and validates the active backend |

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
   |-- spawns: claude -p --output-format stream-json --verbose
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
- **Two backends** - Claude Code CLI (subprocess) or AWS Bedrock (HTTPS, bearer-token auth), selected automatically from the environment
- **`.env` support** - loads a `.env` file at startup so backend config doesn't depend on the launcher exporting vars; process env still takes precedence
- **Streaming & non-streaming** - supports both `stream: true` (NDJSON chunks) and `stream: false` (single JSON response)
- **TTY passthrough** - Claude Code's stderr goes directly to your terminal so you see its progress UI
- **Live web dashboard** - two-panel view of incoming requests and outgoing responses with real-time SSE updates
- **Comprehensive logging** - request IDs, headers, full body dumps, timing, chunk counts at configurable verbosity (`RUST_LOG=trace|debug|info`)
- **38 tests** - 15 unit tests + 23 integration tests using a mock Claude binary
- **`--verbose`** - automatically added for `stream-json` output format (required by Claude CLI)
- **`--dangerously-skip-permissions`** - optional; disabled by default and not supported when running as root

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
- For the **Claude CLI backend**: [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) installed and authenticated (`claude` in PATH)
- For the **Bedrock backend**: an `AWS_BEARER_TOKEN_BEDROCK` with access to the configured model/region (no Claude CLI needed)

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

Configuration is read from environment variables. At startup the proxy also loads a `.env` file (see [Backends](#backends) for the resolution order); existing process env vars always take precedence over `.env` values.

**Common**

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDR` | `127.0.0.1:11435` | API server bind address |
| `MODEL_NAME` | `claude-code:latest` | Model name reported in API responses |
| `RUST_LOG` | `ttyproxy=debug` | Log verbosity (`trace`, `debug`, `info`, `warn`, `error`) |
| `TTYPROXY_ENV` | *(unset)* | Explicit path to a `.env` file (overrides auto-discovery) |

**Claude CLI backend** (used when `AWS_BEARER_TOKEN_BEDROCK` is unset)

| Variable | Default | Description |
|----------|---------|-------------|
| `CLAUDE_BIN` | `claude` | Path to Claude Code CLI binary |
| `DANGEROUSLY_SKIP_PERMISSIONS` | `false` | Pass `--dangerously-skip-permissions` to Claude (not supported when running as root) |
| `TTYPROXY_SHELL` | auto-detect | Shell mode: `cmd`, `powershell`, `bash`, `none` (see below) |

**AWS Bedrock backend** (used when `AWS_BEARER_TOKEN_BEDROCK` is set)

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_BEARER_TOKEN_BEDROCK` | *(unset)* | Bedrock API bearer token. **Setting this switches the proxy to the Bedrock backend.** Value only — no `export VAR=` prefix |
| `BEDROCK_MODEL_ID` | `global.anthropic.claude-sonnet-4-6` | Bedrock model ID |
| `AWS_REGION` | `us-east-1` | AWS region for the `bedrock-runtime` endpoint |
| `BEDROCK_MAX_TOKENS` | `8192` | Max tokens for Bedrock responses |
| `WEB_CHAT_REQUEST_TIMEOUT_MS` | `180000` | HTTP request timeout (ms) for Bedrock calls |

**Bedrock token usage / 2ndBrain quota bridge**

Token tracking is active for the Bedrock backend by default. The proxy stores every Bedrock usage event in SQLite, publishes usage deltas to Redis for 2ndBrain.ceo, and listens for Redis quota state updates from 2ndBrain. Quota remains owned by 2ndBrain through its `profiles.llm_token_quota` and `profiles.llm_token_used` fields; the proxy only blocks once it has received a known exhausted quota state.

| Variable | Default | Description |
|----------|---------|-------------|
| `TOKEN_USAGE_TRACKING` | `true` | Enable SQLite token usage tracking for Bedrock requests |
| `TOKEN_USAGE_DB_PATH` | `ttyproxy-token-usage.sqlite3` | SQLite ledger path |
| `TOKEN_QUOTA_REDIS_URL` | `TOKEN_USAGE_REDIS_URL` / `REDIS_URL` fallback | Redis connection URL for publish/subscribe |
| `TOKEN_QUOTA_REDIS_CHANNEL` | `2ndbrain:token-quota` | Channel where 2ndBrain publishes `token_quota.updated` quota events |
| `TOKEN_USAGE_REDIS_CHANNEL` | `openclaw:token_usage:v1` | Channel where ttyproxy publishes Bedrock token usage events |
| `TOKEN_USAGE_ENFORCE_QUOTA` | `true` | Return `402` for Bedrock requests when a known quota state is exhausted |
| `TOKEN_USAGE_REDIS_FLUSH_INTERVAL_MS` | `10000` | Retry interval for unpublished SQLite events |
| `OPENCLAW_INSTANCE` | host name | OpenClaw instance id used to match 2ndBrain quota updates |
| `OPENCLAW_PROFILE_ID` | *(unset)* | Optional 2ndBrain profile/user id used to match quota events that do not include an OpenClaw instance id |

### Backends

The proxy picks its backend **once at startup**:

- `AWS_BEARER_TOKEN_BEDROCK` **set** → all requests go to **AWS Bedrock** over HTTPS.
- `AWS_BEARER_TOKEN_BEDROCK` **unset** → requests are handled by the **Claude CLI** subprocess.

The active backend is printed in the startup banner (`Ollama-compatible proxy -> AWS Bedrock (...)` or `-> Claude CLI (...)`). If any Bedrock variable is set but no usable token is found, the proxy logs a prominent warning and falls back to the Claude CLI backend rather than failing silently.

#### `.env` loading

On startup the proxy loads a `.env` file so backend config doesn't depend on the launcher exporting variables. Resolution order (first hit wins; existing process env vars always take precedence over file values):

1. `$TTYPROXY_ENV` — explicit path override
2. `<directory of the executable>/.env` — robust when launched as a daemon from an unrelated working directory
3. `.env` discovered from the current directory upward

> **Note:** put the **value only** in `.env` — `AWS_BEARER_TOKEN_BEDROCK=ABSK...`, **not** `export AWS_BEARER_TOKEN_BEDROCK=...`. A stray `export VAR=`/`VAR=` prefix and surrounding quotes/whitespace are stripped automatically, but the raw value is cleanest.

#### Bedrock quickstart

```bash
cat > .env <<'EOF'
AWS_BEARER_TOKEN_BEDROCK=ABSK...your-token...
BEDROCK_MODEL_ID=global.anthropic.claude-sonnet-4-6
AWS_REGION=ap-southeast-1
BEDROCK_MAX_TOKENS=64000
WEB_CHAT_REQUEST_TIMEOUT_MS=180000
TOKEN_QUOTA_REDIS_URL=redis://default:...@host:port
TOKEN_QUOTA_REDIS_CHANNEL=2ndbrain:token-quota
OPENCLAW_INSTANCE=your-openclaw-instance-name
OPENCLAW_PROFILE_ID=supabase-profile-id
EOF

cargo run --release --bin ttyproxy   # banner should read: -> AWS Bedrock (...)
```

> The deployed binary must contain the Bedrock feature; builds from before it was added route to the Claude CLI regardless of the environment.

#### 2ndBrain Redis contract

2ndBrain publishes quota updates to `TOKEN_QUOTA_REDIS_CHANNEL`:

```json
{
  "actor": {
    "email": "admin@example.com",
    "userId": "admin-user-id"
  },
  "availableTokens": 97500,
  "deltaTokens": 2500,
  "email": "user@example.com",
  "event": "token_quota.updated",
  "llmTokenQuota": 100000,
  "llmTokenUsed": 2500,
  "metadata": {},
  "occurredAt": "2026-06-12T00:00:00.000Z",
  "reason": "admin_quota_update",
  "source": "2ndBrain.ceo",
  "userId": "supabase-profile-id",
  "version": 1
}
```

ttyproxy accepts the direct `openclaw_instance`/`llm_token_quota` shape too. For the 2ndBrain event above, set `OPENCLAW_PROFILE_ID` to the target `userId` so a shared Redis channel cannot apply another user's quota to this OpenClaw instance.

ttyproxy publishes usage deltas to `TOKEN_USAGE_REDIS_CHANNEL` after Bedrock returns usage metadata:

```json
{
  "type": "openclaw.token_usage.v1",
  "event_id": "uuid",
  "request_id": "req-000001",
  "provider": "aws_bedrock",
  "endpoint": "/api/chat",
  "model": "global.anthropic.claude-sonnet-4-6",
  "openclaw_instance": "your-openclaw-instance-name",
  "profile_id": "supabase-profile-id",
  "input_tokens": 1200,
  "output_tokens": 340,
  "cache_creation_input_tokens": 0,
  "cache_read_input_tokens": 0,
  "total_tokens": 1540,
  "llm_token_used_delta": 1540,
  "observed_llm_token_used": 4040,
  "llm_token_quota": 100000,
  "remaining_tokens": 95960,
  "is_streaming": true,
  "created_at": "2026-06-12T00:00:00Z"
}
```

If Redis is unavailable, the event stays in SQLite with `redis_published_at = null`; the background flusher retries until publish succeeds.

#### Running under systemd

systemd does not inherit your shell environment, so point the unit at the `.env` with `EnvironmentFile=` (it injects the vars straight into the process environment). The leading `-` makes a missing file non-fatal:

```ini
[Service]
ExecStart=%h/.local/bin/ttyproxy
Environment=LISTEN_ADDR=127.0.0.1:11435
Environment=RUST_LOG=ttyproxy=info
EnvironmentFile=-%h/path/to/claw-ttyproxy/.env
```

After editing the unit: `systemctl --user daemon-reload && systemctl --user restart ttyproxy`.

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
running 15 tests         # unit tests (types, helpers, config/token sanitizer)
running 23 tests         # integration tests (all endpoints, streaming, dashboard)
test result: ok. 38 passed; 0 failed
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
