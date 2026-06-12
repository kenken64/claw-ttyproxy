use ttyproxy::api::handlers::AppState;
use ttyproxy::config::Config;
use ttyproxy::dashboard;
use ttyproxy::dashboard::log_store::LogStore;
use ttyproxy::proxy::bedrock::BedrockRunner;
use ttyproxy::proxy::claude::ClaudeRunner;
use ttyproxy::proxy::BackendRunner;
use ttyproxy::usage::{TokenUsageConfig, TokenUsageTracker};

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// Locate and load a `.env` file, returning the path that was loaded.
///
/// Resolution order (first hit wins). Existing process env vars always take
/// precedence over file values, so explicit launcher/systemd settings still win.
///   1. `$TTYPROXY_ENV` — explicit path override.
///   2. `<dir-of-executable>/.env` — robust for daemons launched with an
///      unrelated working directory.
///   3. `.env` discovered from the current directory upward (dotenvy default).
fn load_dotenv() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("TTYPROXY_ENV") {
        let path = PathBuf::from(&p);
        match dotenvy::from_path(&path) {
            Ok(()) => return Some(path),
            Err(e) => eprintln!("warning: TTYPROXY_ENV={p} could not be loaded: {e}"),
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(candidate) = exe.parent().map(|d| d.join(".env")) {
            if candidate.is_file() && dotenvy::from_path(&candidate).is_ok() {
                return Some(candidate);
            }
        }
    }

    dotenvy::dotenv().ok()
}

#[tokio::main]
async fn main() {
    // Load variables from a `.env` file before reading config, so the selected
    // backend (Bedrock vs Claude CLI) does not depend on the launcher having
    // exported them.
    let dotenv_path = load_dotenv();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("ttyproxy=debug,tower_http=debug"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    let config = Config::from_env();

    info!("=== ttyproxy v0.1.0 starting ===");
    info!("Log levels: RUST_LOG=trace|debug|info (default: debug)");
    match dotenv_path {
        Some(path) => info!(path = %path.display(), "loaded .env file"),
        None => info!("no .env file found; using process environment only"),
    }

    // Fail loud, not silent: if the operator set Bedrock vars but we found no
    // usable token, make the fallback to Claude CLI impossible to miss.
    if config.bedrock_misconfigured() {
        warn!("Bedrock env vars are set but AWS_BEARER_TOKEN_BEDROCK is missing/empty; falling back to Claude CLI backend");
        eprintln!("========================================");
        eprintln!("  WARNING: Bedrock variables are set, but AWS_BEARER_TOKEN_BEDROCK");
        eprintln!("           is missing or empty -> FALLING BACK to the Claude CLI backend.");
        eprintln!("           Set the token (value only, no `export VAR=` prefix) to use Bedrock.");
        eprintln!("========================================");
    }

    let log_store = LogStore::new(500);
    let token_usage_tracker = if config.use_bedrock() && config.token_usage_tracking {
        match TokenUsageTracker::open(TokenUsageConfig {
            db_path: config.token_usage_db_path.clone(),
            redis_url: config.token_usage_redis_url.clone(),
            usage_channel: config.token_usage_channel.clone(),
            quota_channel: config.token_quota_channel.clone(),
            openclaw_instance: config.openclaw_instance.clone(),
            profile_id: config.openclaw_profile_id.clone(),
            enforce_quota: config.token_usage_enforce_quota,
            flush_interval_ms: config.token_usage_flush_interval_ms,
        }) {
            Ok(tracker) => {
                tracker.start_background_tasks();
                Some(tracker)
            }
            Err(error) => {
                warn!(error = %error, "token usage tracker could not start; continuing without token tracking");
                None
            }
        }
    } else {
        None
    };

    let runner: BackendRunner = if config.use_bedrock() {
        let token = config.bedrock_bearer_token.clone().unwrap();
        BackendRunner::Bedrock(BedrockRunner::new(
            token,
            config.bedrock_model_id.clone(),
            config.bedrock_region.clone(),
            config.bedrock_max_tokens,
            config.bedrock_timeout_ms,
            token_usage_tracker.clone(),
        ))
    } else {
        BackendRunner::Claude(ClaudeRunner::new(
            config.claude_bin.clone(),
            config.dangerously_skip_permissions,
        ))
    };

    let backend_label = if config.use_bedrock() {
        format!("AWS Bedrock ({})", config.bedrock_model_id)
    } else {
        format!("Claude CLI ({})", config.claude_bin)
    };

    let state = AppState {
        runner: Arc::new(Mutex::new(runner)),
        model_name: config.model_name.clone(),
        log_store: log_store.clone(),
        token_usage_tracker,
    };

    let api = ttyproxy::build_api_router(state.clone());
    let dashboard = dashboard::router(state);

    let api_addr = config.listen_addr;
    let dash_addr = {
        let mut a = api_addr;
        a.set_port(a.port() + 1);
        a
    };

    let skip_perms = config.dangerously_skip_permissions;
    eprintln!("========================================");
    eprintln!("  ttyproxy v0.1.0");
    eprintln!("  Ollama-compatible proxy -> {backend_label}");
    eprintln!("  API:       http://{api_addr}");
    eprintln!("  Dashboard: http://{dash_addr}");
    eprintln!("  Logs:      RUST_LOG=trace for max detail");
    if config.use_bedrock() {
        eprintln!("  Region:    {}", config.bedrock_region);
        eprintln!("  Model:     {}", config.bedrock_model_id);
        eprintln!("  MaxTokens: {}", config.bedrock_max_tokens);
        if config.token_usage_tracking {
            eprintln!("  Token DB:  {}", config.token_usage_db_path.display());
            if config.token_usage_redis_url.is_some() {
                eprintln!("  Redis:     enabled");
                eprintln!("  Instance:  {}", config.openclaw_instance);
            }
        }
    } else if skip_perms {
        eprintln!("  WARNING:   --dangerously-skip-permissions ENABLED");
    }
    eprintln!("========================================");

    let api_listener = tokio::net::TcpListener::bind(api_addr).await.unwrap();
    let dash_listener = tokio::net::TcpListener::bind(dash_addr).await.unwrap();

    info!(api_addr = %api_addr, dash_addr = %dash_addr, "servers starting");

    tokio::join!(
        async {
            axum::serve(
                api_listener,
                api.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap()
        },
        async {
            axum::serve(dash_listener, dashboard.into_make_service())
                .await
                .unwrap()
        },
    );
}
