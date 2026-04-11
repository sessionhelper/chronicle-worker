//! chronicle-worker: batch orchestrator that turns captured audio chunks into
//! transcripts. Polls the Data API for `uploaded` sessions, downloads
//! audio, runs `chronicle-pipeline` as a library, posts segments back.
//!
//! This crate never touches Postgres or S3 directly — every persistence
//! operation goes through the Data API over HTTP. See
//! `/home/alex/sessionhelper-hub/ARCHITECTURE.md` for the full data flow.
//!
//! Entry points:
//!
//! - [`worker::run`] — the main polling loop. `main.rs` builds an
//!   [`state::AppState`] and hands it here.
//! - [`api_client::DataApiClient`] — HTTP client for the Data API
//!   (auth, heartbeat, sessions, chunks, segments).
//! - [`config::Config`] — env-driven configuration parsed via `clap`.

pub mod api_client;
pub mod config;
pub mod decode;
pub mod state;
pub mod worker;

pub use config::Config;
pub use state::AppState;
