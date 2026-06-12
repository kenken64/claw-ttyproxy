pub mod api;
pub mod config;
pub mod dashboard;
pub mod middleware;
pub mod proxy;
pub mod usage;

use api::handlers::{self, AppState};
use axum::{
    middleware as axum_mw,
    routing::{delete, get, post},
    Router,
};
use tower_http::cors::CorsLayer;

/// Build the Ollama-compatible API router with the given state.
pub fn build_api_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(handlers::health).head(handlers::health_head))
        .route("/api/version", get(handlers::version))
        .route("/api/tags", get(handlers::list_tags))
        .route("/api/show", post(handlers::show_model))
        .route("/api/chat", post(handlers::chat))
        .route("/api/generate", post(handlers::generate))
        .route("/api/embeddings", post(handlers::embeddings))
        .route("/api/embed", post(handlers::embeddings))
        .route("/api/pull", post(handlers::pull_model))
        .route("/api/delete", delete(handlers::delete_model))
        .route("/api/copy", post(handlers::copy_model))
        .layer(axum_mw::from_fn(middleware::logging::log_request))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
