//! `chronicle-worker` binary entrypoint.
//!
//! Responsibilities (thin):
//!
//! 1. Load env-driven [`Config`].
//! 2. Init tracing from `RUST_LOG` (falls back to `LOG_LEVEL` config).
//! 3. Authenticate against data-api.
//! 4. Hand control to [`event_loop::run`].

use std::sync::Arc;

use chronicle_worker::api_client::DataApiClient;
use chronicle_worker::config::Config;
use chronicle_worker::event_loop;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Config::from_env();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();

    tracing::info!(
        data_api = %config.data_api_url,
        poll_interval_secs = config.poll_interval_secs,
        admin_enabled = config.admin_enabled,
        "chronicle-worker starting"
    );

    let secret = config.resolved_shared_secret()?.to_string();
    let api = DataApiClient::authenticate(config.data_api_url.clone(), secret, "chronicle-worker").await?;
    let cfg = Arc::new(config);

    event_loop::run(api, cfg).await?;
    Ok(())
}
