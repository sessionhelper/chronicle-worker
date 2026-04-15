//! chronicle-worker — glue between `chronicle-data-api` and
//! `chronicle-pipeline`.
//!
//! Entry point: [`event_loop::run`]. See `sessionhelper-hub/docs/modules/
//! chronicle-worker.md` for the authoritative Features + Behavior spec.

pub mod admin;
pub mod api_client;
pub mod config;
pub mod decode;
pub mod error;
pub mod event_loop;
pub mod ids;
pub mod retry;
pub mod session_runner;
pub mod whisper;
pub mod ws;

pub use config::Config;
pub use error::{Result, WorkerError};
