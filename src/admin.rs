//! Admin HTTP surface.
//!
//! Disabled entirely in production. When `WORKER_ADMIN_ENABLED=true`, the
//! worker binds a small axum router to `ADMIN_BIND_ADDR` (loopback by
//! default) exposing:
//!
//! - `POST /admin/rerun/{session_id}` — force a re-run; returns `{ queued }`.
//! - `GET  /admin/status` — `{ active_sessions, last_heartbeat_at, version }`.
//!
//! Commands flow to the event loop via an mpsc channel so the event
//! loop retains single-ownership over the active-session map.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

use crate::error::{Result, WorkerError};
use crate::ids::SessionId;

/// Messages from the admin HTTP handler into the event loop.
pub enum AdminCmd {
    /// Force a rerun of a session (one-shot, clearing prior outputs).
    Rerun { session: SessionId, ack: tokio::sync::oneshot::Sender<bool> },
    /// Snapshot active-session state.
    Status { ack: tokio::sync::oneshot::Sender<StatusSnapshot> },
}

/// Shared view the admin handler exposes.
#[derive(Clone, Serialize)]
pub struct StatusSnapshot {
    pub active_sessions: Vec<Uuid>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub version: &'static str,
}

/// State shared with admin handlers.
#[derive(Clone)]
pub struct AdminState {
    pub cmd_tx: mpsc::Sender<AdminCmd>,
    pub last_heartbeat: Arc<Mutex<Option<DateTime<Utc>>>>,
}

/// Bind + serve the admin router. Returns immediately after `bind`,
/// leaving the axum server running in a spawned task. `Err` means
/// the bind itself failed.
pub async fn serve(
    bind: SocketAddr,
    state: AdminState,
) -> Result<tokio::task::JoinHandle<()>> {
    let app = Router::new()
        .route("/admin/status", get(status_handler))
        .route("/admin/rerun/{session_id}", post(rerun_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| WorkerError::Admin(format!("bind {bind}: {e}")))?;

    tracing::info!(%bind, "admin HTTP listening");

    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "admin server exited");
        }
    });
    Ok(handle)
}

async fn status_handler(State(state): State<AdminState>) -> impl IntoResponse {
    let (tx, rx) = tokio::sync::oneshot::channel();
    if state.cmd_tx.send(AdminCmd::Status { ack: tx }).await.is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, "event loop gone").into_response();
    }
    match tokio::time::timeout(Duration::from_secs(2), rx).await {
        Ok(Ok(snap)) => Json(snap).into_response(),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "timeout").into_response(),
    }
}

async fn rerun_handler(
    State(state): State<AdminState>,
    Path(session_id): Path<Uuid>,
) -> impl IntoResponse {
    let (tx, rx) = tokio::sync::oneshot::channel();
    if state
        .cmd_tx
        .send(AdminCmd::Rerun { session: SessionId(session_id), ack: tx })
        .await
        .is_err()
    {
        return (StatusCode::SERVICE_UNAVAILABLE, "event loop gone").into_response();
    }
    match tokio::time::timeout(Duration::from_secs(5), rx).await {
        Ok(Ok(queued)) => Json(serde_json::json!({ "queued": queued })).into_response(),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "timeout").into_response(),
    }
}

/// Crate version (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
