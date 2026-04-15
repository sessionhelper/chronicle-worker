//! One error enum for the worker.
//!
//! Everything funnels through `WorkerError`; callers decide whether to
//! abort a session (pipeline/data-api permanent failure), retry the
//! session (transient network), or ignore (claim race, session gone).

use thiserror::Error;

use crate::ids::SessionId;

/// Worker-wide error type.
///
/// Variants split by *what the caller should do*, not by *where the error
/// came from*. This mirrors the spec's two-tier split (pipeline permanent
/// vs transient) and keeps the event-loop's `match` exhaustive without
/// a catch-all arm.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// Data-api HTTP failed (non-2xx or wire error). Caller decides retry.
    #[error("data-api error: {0}")]
    Api(String),

    /// Data-api rejected a claim (409 on a `PATCH status=transcribing`).
    /// Another worker won the race; we skip this session.
    #[error("claim lost for session {0}")]
    ClaimLost(SessionId),

    /// Data-api returned 404. Session was deleted between events.
    #[error("session {0} not found")]
    NotFound(SessionId),

    /// Pipeline library returned a non-recoverable error.
    #[error("pipeline: {0}")]
    Pipeline(#[from] chronicle_pipeline::PipelineError),

    /// WebSocket connection lifetime issue. Event loop reconnects.
    #[error("websocket: {0}")]
    WebSocket(String),

    /// Admin-surface specific (bind failure etc.).
    #[error("admin: {0}")]
    Admin(String),

    /// Configuration invalid at startup.
    #[error("config: {0}")]
    Config(String),
}

impl WorkerError {
    /// True when the caller should schedule a session-level retry.
    ///
    /// The exhaustive match here is deliberate: every new variant forces
    /// the author to decide "does this retry?" instead of falling through
    /// a default.
    pub fn is_retryable_session_failure(&self) -> bool {
        match self {
            Self::Api(_)
            | Self::Pipeline(_)
            | Self::WebSocket(_) => true,
            Self::ClaimLost(_)
            | Self::NotFound(_)
            | Self::Admin(_)
            | Self::Config(_) => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, WorkerError>;

// ----- Conversions -------------------------------------------------------

impl From<reqwest::Error> for WorkerError {
    fn from(e: reqwest::Error) -> Self { Self::Api(e.to_string()) }
}

impl From<serde_json::Error> for WorkerError {
    fn from(e: serde_json::Error) -> Self { Self::Api(format!("json: {e}")) }
}
