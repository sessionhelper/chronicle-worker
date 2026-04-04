//! OVP Pipeline Worker
//!
//! Polls the Data API for sessions ready for transcription, downloads
//! audio, runs the ovp-pipeline, and posts results back. Never touches
//! Postgres or S3 directly — everything goes through the Data API.

mod api_client;
mod config;
mod worker;

use std::time::Duration;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = config::Config::from_env();

    tracing::info!(
        data_api = %config.data_api_url,
        whisper = %config.whisper_url,
        poll_interval = config.poll_interval_secs,
        "pipeline worker starting"
    );

    // Authenticate with Data API
    let api = match api_client::DataApiClient::authenticate(
        &config.data_api_url,
        &config.shared_secret,
        "pipeline",
    )
    .await
    {
        Ok(client) => client,
        Err(e) => {
            tracing::error!(error = %e, "failed to authenticate with Data API");
            std::process::exit(1);
        }
    };

    let api = std::sync::Arc::new(api);

    // Spawn heartbeat
    let hb_api = api.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            if let Err(e) = hb_api.heartbeat().await {
                tracing::warn!(error = %e, "heartbeat failed");
            }
        }
    });

    // Poll loop
    let poll_interval = Duration::from_secs(config.poll_interval_secs);
    loop {
        match worker::poll_and_process(&api, &config).await {
            Ok(processed) => {
                if processed > 0 {
                    tracing::info!(sessions = processed, "processed sessions");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "worker error");
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}
