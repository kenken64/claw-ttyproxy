use std::net::SocketAddr;

/// Application configuration, populated from environment variables with sensible defaults.
pub struct Config {
    /// Address to bind the HTTP server to.
    pub listen_addr: SocketAddr,
    /// The model name reported in Ollama API responses.
    pub model_name: String,

    // -- Claude CLI backend --------------------------------------------------
    /// Path to the `claude` CLI binary.
    pub claude_bin: String,
    /// Pass `--dangerously-skip-permissions` to the claude CLI.
    pub dangerously_skip_permissions: bool,

    // -- AWS Bedrock backend -------------------------------------------------
    /// Bearer token for Bedrock API (`AWS_BEARER_TOKEN_BEDROCK`).
    /// When set, the proxy routes all requests through Bedrock instead of Claude CLI.
    pub bedrock_bearer_token: Option<String>,
    /// Bedrock model ID, e.g. `global.anthropic.claude-sonnet-4-6`.
    pub bedrock_model_id: String,
    /// AWS region for the Bedrock endpoint, e.g. `ap-southeast-1`.
    pub bedrock_region: String,
    /// Maximum tokens for Bedrock responses (`BEDROCK_MAX_TOKENS`).
    pub bedrock_max_tokens: u32,
    /// Request timeout in milliseconds (`WEB_CHAT_REQUEST_TIMEOUT_MS`).
    pub bedrock_timeout_ms: u64,
}

impl Config {
    /// Load configuration from environment variables.
    ///
    /// | Env var                          | Default                               |
    /// |----------------------------------|---------------------------------------|
    /// | `LISTEN_ADDR`                    | `127.0.0.1:11435`                     |
    /// | `MODEL_NAME`                     | `claude-code:latest`                  |
    /// | `CLAUDE_BIN`                     | `claude`                              |
    /// | `DANGEROUSLY_SKIP_PERMISSIONS`   | `false`                               |
    /// | `AWS_BEARER_TOKEN_BEDROCK`       | *(unset — uses Claude CLI backend)*   |
    /// | `BEDROCK_MODEL_ID`               | `global.anthropic.claude-sonnet-4-6`  |
    /// | `AWS_REGION`                     | `us-east-1`                           |
    /// | `BEDROCK_MAX_TOKENS`             | `8192`                                |
    /// | `WEB_CHAT_REQUEST_TIMEOUT_MS`    | `180000`                              |
    pub fn from_env() -> Self {
        Self {
            listen_addr: std::env::var("LISTEN_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:11435".into())
                .parse()
                .expect("LISTEN_ADDR must be a valid socket address"),
            model_name: std::env::var("MODEL_NAME")
                .unwrap_or_else(|_| "claude-code:latest".into()),
            claude_bin: std::env::var("CLAUDE_BIN").unwrap_or_else(|_| "claude".into()),
            dangerously_skip_permissions: std::env::var("DANGEROUSLY_SKIP_PERMISSIONS")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(false),
            bedrock_bearer_token: std::env::var("AWS_BEARER_TOKEN_BEDROCK")
                .ok()
                .filter(|s| !s.is_empty()),
            bedrock_model_id: std::env::var("BEDROCK_MODEL_ID")
                .unwrap_or_else(|_| "global.anthropic.claude-sonnet-4-6".into()),
            bedrock_region: std::env::var("AWS_REGION")
                .unwrap_or_else(|_| "us-east-1".into()),
            bedrock_max_tokens: std::env::var("BEDROCK_MAX_TOKENS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8192),
            bedrock_timeout_ms: std::env::var("WEB_CHAT_REQUEST_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(180_000),
        }
    }

    /// Returns true when Bedrock env vars are configured.
    pub fn use_bedrock(&self) -> bool {
        self.bedrock_bearer_token.is_some()
    }
}
