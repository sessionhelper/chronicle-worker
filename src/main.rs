//! `chronicle-worker` binary entrypoint.
//!
//! Responsibilities (kept deliberately thin):
//!
//! 1. Init tracing from `LOG_LEVEL` (or `RUST_LOG`).
//! 2. Parse `Config` from env via `clap`.
//! 3. Authenticate with the Data API → `DataApiClient`.
//! 4. Build `AppState` and spawn the 30s heartbeat task.
//! 5. Hand control to `worker::run(state).await`.
//!
//! All orchestration lives in [`chronicle_worker::worker`].

use std::sync::Arc;
use std::time::Duration;

use chronicle_worker::api_client::DataApiClient;
use chronicle_worker::config::Config;
use chronicle_worker::state::AppState;
use chronicle_worker::worker;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow_lite::Result<()> {
    // ---- Config (parse before tracing so LOG_LEVEL applies) ----------
    let config = Config::from_env();

    // ---- Tracing init ------------------------------------------------
    // Prefer RUST_LOG if set, otherwise fall back to the config's
    // log_level. Mirrors the collector's init shape.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();

    tracing::info!(
        data_api = %config.data_api_url,
        poll_interval_secs = config.poll_interval_secs,
        "chronicle-worker starting"
    );

    // ---- Authenticate ------------------------------------------------
    let api = DataApiClient::authenticate(
        &config.data_api_url,
        &config.shared_secret,
        "chronicle-worker",
    )
    .await
    .map_err(|e| anyhow_lite::err(format!("data api auth failed: {e}")))?;
    let api = Arc::new(api);

    // ---- Build shared state ------------------------------------------
    let state = AppState::new(api.clone(), config);

    // ---- Spawn 30s heartbeat task ------------------------------------
    // TODO: on repeated heartbeat failures (e.g. 3 in a row), consider
    // re-authenticating instead of spamming warnings — the Data API
    // reaps sessions after 90s of silence, so a token that's already
    // been reaped will never recover on its own.
    let hb_api = api.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        // Skip the immediate first tick — authenticate already talked
        // to the server, no need to hit it again 0s later.
        tick.tick().await;
        loop {
            tick.tick().await;
            if let Err(e) = hb_api.heartbeat().await {
                tracing::warn!(error = %e, "heartbeat failed");
            }
        }
    });

    // ---- Enter the worker loop (blocks forever) ----------------------
    worker::run(state)
        .await
        .map_err(|e| anyhow_lite::err(format!("worker loop exited: {e}")))?;

    Ok(())
}

/// Tiny inline error-returning shim so we don't pull in the full
/// `anyhow` crate just for `main`'s return type. Ten lines of code
/// beats adding a dep for two uses.
mod anyhow_lite {
    pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
    pub fn err(msg: String) -> Box<dyn std::error::Error + Send + Sync> {
        msg.into()
    }
}
