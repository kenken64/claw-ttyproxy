pub mod log_store;

use crate::api::handlers::AppState;
use axum::{
    Router,
    extract::State,
    response::{Html, Sse},
    routing::get,
};
use futures::stream::Stream;
use log_store::LogEntry;
use std::convert::Infallible;
use std::time::Duration;

/// Build the dashboard router (served on a separate port).
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/logs", get(get_logs))
        .route("/api/logs/stream", get(sse_logs))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn get_logs(State(state): State<AppState>) -> axum::Json<Vec<LogEntry>> {
    let entries = state.log_store.entries();
    axum::Json(entries)
}

async fn sse_logs(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<axum::response::sse::Event, Infallible>>> {
    let mut rx = state.log_store.subscribe();

    let stream = async_stream::stream! {
        while let Ok(entry) = rx.recv().await {
            let json = serde_json::to_string(&entry).unwrap_or_default();
            yield Ok(axum::response::sse::Event::default().data(json));
        }
    };

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}
