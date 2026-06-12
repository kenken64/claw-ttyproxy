use std::net::SocketAddr;
use std::path::PathBuf;

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
    /// True when any Bedrock-specific env var is set (token / model / max-tokens).
    /// Used to detect a half-configured Bedrock setup so we can warn loudly
    /// instead of silently falling back to the Claude CLI backend.
    pub bedrock_env_present: bool,

    // -- Token usage tracking -----------------------------------------------
    /// Enable local SQLite token tracking for Bedrock requests.
    pub token_usage_tracking: bool,
    /// SQLite database path for the token usage ledger.
    pub token_usage_db_path: PathBuf,
    /// Redis URL used for usage publish and quota subscribe.
    pub token_usage_redis_url: Option<String>,
    /// Redis channel where usage events are published for 2ndBrain.
    pub token_usage_channel: String,
    /// Redis channel where 2ndBrain publishes quota state for this instance.
    pub token_quota_channel: String,
    /// OpenClaw instance identifier used in Redis payloads and quota matching.
    pub openclaw_instance: String,
    /// Optional 2ndBrain profile/user id to include in usage payloads.
    pub openclaw_profile_id: Option<String>,
    /// Block Bedrock requests when a known quota state is exhausted.
    pub token_usage_enforce_quota: bool,
    /// Retry interval for pending Redis usage publish attempts.
    pub token_usage_flush_interval_ms: u64,
}

/// Normalize a bearer token read from the environment.
///
/// Tolerates values that were accidentally pasted as a full shell assignment,
/// e.g. `export AWS_BEARER_TOKEN_BEDROCK=ABSK…` or `AWS_BEARER_TOKEN_BEDROCK=ABSK…`,
/// and strips surrounding whitespace and a single layer of matching quotes.
/// The real token never starts with that prefix, so this only ever repairs a
/// malformed paste — it cannot corrupt a valid token (trailing `=` padding is
/// preserved because only a leading prefix is removed).
fn sanitize_bearer_token(raw: &str) -> String {
    let mut s = raw.trim();
    s = s.strip_prefix("export ").map(str::trim_start).unwrap_or(s);
    s = s
        .strip_prefix("AWS_BEARER_TOKEN_BEDROCK=")
        .unwrap_or(s)
        .trim();
    // Strip one layer of surrounding quotes, if present.
    for q in ['"', '\''] {
        if let Some(inner) = s.strip_prefix(q).and_then(|x| x.strip_suffix(q)) {
            s = inner;
            break;
        }
    }
    s.to_string()
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
        .unwrap_or(default)
}

fn clean_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|s| s.trim().trim_matches(|c| c == '"' || c == '\'').to_string())
        .filter(|s| !s.is_empty())
}

fn default_openclaw_instance() -> String {
    clean_env("COMPUTERNAME")
        .or_else(|| clean_env("HOSTNAME"))
        .unwrap_or_else(|| "unknown-openclaw-instance".into())
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
        // Detect whether the operator intended to use Bedrock at all, regardless
        // of whether the token ends up usable.
        let bedrock_env_present = [
            "AWS_BEARER_TOKEN_BEDROCK",
            "BEDROCK_MODEL_ID",
            "BEDROCK_MAX_TOKENS",
        ]
        .iter()
        .any(|k| std::env::var_os(k).is_some());

        Self {
            listen_addr: std::env::var("LISTEN_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:11435".into())
                .parse()
                .expect("LISTEN_ADDR must be a valid socket address"),
            model_name: std::env::var("MODEL_NAME").unwrap_or_else(|_| "claude-code:latest".into()),
            claude_bin: std::env::var("CLAUDE_BIN").unwrap_or_else(|_| "claude".into()),
            dangerously_skip_permissions: std::env::var("DANGEROUSLY_SKIP_PERMISSIONS")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(false),
            bedrock_bearer_token: std::env::var("AWS_BEARER_TOKEN_BEDROCK")
                .ok()
                .map(|s| sanitize_bearer_token(&s))
                .filter(|s| !s.is_empty()),
            bedrock_model_id: std::env::var("BEDROCK_MODEL_ID")
                .unwrap_or_else(|_| "global.anthropic.claude-sonnet-4-6".into()),
            bedrock_region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
            bedrock_max_tokens: std::env::var("BEDROCK_MAX_TOKENS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8192),
            bedrock_timeout_ms: std::env::var("WEB_CHAT_REQUEST_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(180_000),
            bedrock_env_present,
            token_usage_tracking: env_bool("TOKEN_USAGE_TRACKING", true),
            token_usage_db_path: std::env::var("TOKEN_USAGE_DB_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("ttyproxy-token-usage.sqlite3")),
            token_usage_redis_url: clean_env("TOKEN_QUOTA_REDIS_URL")
                .or_else(|| clean_env("TOKEN_USAGE_REDIS_URL"))
                .or_else(|| clean_env("REDIS_URL")),
            token_usage_channel: clean_env("TOKEN_USAGE_REDIS_CHANNEL")
                .or_else(|| clean_env("TOKEN_USAGE_REDIS_USAGE_CHANNEL"))
                .unwrap_or_else(|| "openclaw:token_usage:v1".into()),
            token_quota_channel: clean_env("TOKEN_QUOTA_REDIS_CHANNEL")
                .or_else(|| clean_env("TOKEN_USAGE_REDIS_QUOTA_CHANNEL"))
                .unwrap_or_else(|| "2ndbrain:token-quota".into()),
            openclaw_instance: clean_env("OPENCLAW_INSTANCE")
                .unwrap_or_else(default_openclaw_instance),
            openclaw_profile_id: clean_env("OPENCLAW_PROFILE_ID"),
            token_usage_enforce_quota: env_bool("TOKEN_USAGE_ENFORCE_QUOTA", true),
            token_usage_flush_interval_ms: std::env::var("TOKEN_USAGE_REDIS_FLUSH_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),
        }
    }

    /// Returns true when Bedrock env vars are configured.
    pub fn use_bedrock(&self) -> bool {
        self.bedrock_bearer_token.is_some()
    }

    /// True when the operator clearly intended to use Bedrock (some Bedrock env
    /// var is set) but no usable bearer token was found — i.e. the proxy is
    /// about to silently fall back to the Claude CLI backend.
    pub fn bedrock_misconfigured(&self) -> bool {
        self.bedrock_env_present && self.bedrock_bearer_token.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::sanitize_bearer_token;

    #[test]
    fn keeps_a_plain_token_untouched() {
        assert_eq!(sanitize_bearer_token("ABSKabc123=="), "ABSKabc123==");
    }

    #[test]
    fn strips_export_assignment_prefix() {
        assert_eq!(
            sanitize_bearer_token("export AWS_BEARER_TOKEN_BEDROCK=ABSKabc123=="),
            "ABSKabc123=="
        );
    }

    #[test]
    fn strips_bare_assignment_prefix() {
        assert_eq!(
            sanitize_bearer_token("AWS_BEARER_TOKEN_BEDROCK=ABSKabc123=="),
            "ABSKabc123=="
        );
    }

    #[test]
    fn trims_whitespace_and_quotes() {
        assert_eq!(sanitize_bearer_token("  ABSKabc==  "), "ABSKabc==");
        assert_eq!(sanitize_bearer_token("\"ABSKabc==\""), "ABSKabc==");
        assert_eq!(sanitize_bearer_token("'ABSKabc=='"), "ABSKabc==");
    }

    #[test]
    fn preserves_trailing_base64_padding() {
        // Only a leading prefix is stripped, so trailing `=` padding survives.
        assert_eq!(
            sanitize_bearer_token("export AWS_BEARER_TOKEN_BEDROCK=YQ=="),
            "YQ=="
        );
    }
}
