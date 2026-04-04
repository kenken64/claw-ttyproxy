use std::net::SocketAddr;

/// Application configuration, populated from environment variables with sensible defaults.
pub struct Config {
    /// Address to bind the HTTP server to.
    pub listen_addr: SocketAddr,
    /// Path to the `claude` CLI binary.
    pub claude_bin: String,
    /// The model name reported in Ollama API responses.
    pub model_name: String,
    /// Pass `--dangerously-skip-permissions` to the claude CLI.
    pub dangerously_skip_permissions: bool,
}

impl Config {
    /// Load configuration from environment variables.
    ///
    /// | Env var                          | Default               |
    /// |----------------------------------|-----------------------|
    /// | `LISTEN_ADDR`                    | `127.0.0.1:11435`     |
    /// | `CLAUDE_BIN`                     | `claude`              |
    /// | `MODEL_NAME`                     | `claude-code:latest`  |
    /// | `DANGEROUSLY_SKIP_PERMISSIONS`   | `false`               |
    pub fn from_env() -> Self {
        Self {
            listen_addr: std::env::var("LISTEN_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:11435".into())
                .parse()
                .expect("LISTEN_ADDR must be a valid socket address"),
            claude_bin: std::env::var("CLAUDE_BIN").unwrap_or_else(|_| "claude".into()),
            model_name: std::env::var("MODEL_NAME")
                .unwrap_or_else(|_| "claude-code:latest".into()),
            dangerously_skip_permissions: std::env::var("DANGEROUSLY_SKIP_PERMISSIONS")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(true),
        }
    }
}
