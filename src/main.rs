use ttyproxy::api::handlers::AppState;
use ttyproxy::config::Config;
use ttyproxy::dashboard;
use ttyproxy::dashboard::log_store::LogStore;
use ttyproxy::proxy::claude::ClaudeRunner;

use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
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

    let log_store = LogStore::new(500);

    let state = AppState {
        claude: Arc::new(Mutex::new(ClaudeRunner::new(
            config.claude_bin,
            config.dangerously_skip_permissions,
        ))),
        model_name: config.model_name,
        log_store: log_store.clone(),
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
    eprintln!("  Ollama-compatible proxy -> Claude Code");
    eprintln!("  API:       http://{api_addr}");
    eprintln!("  Dashboard: http://{dash_addr}");
    eprintln!("  Logs:      RUST_LOG=trace for max detail");
    if skip_perms {
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
