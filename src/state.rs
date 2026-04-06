//! Shared application state.
//!
//! Everything the worker needs at runtime — the Data API client and the
//! parsed config — lives in one struct, wrapped in `Arc` and passed to
//! the poll loop. Keeping state in one place avoids the "scattered
//! state across multiple HashMaps" anti-pattern (see hub CLAUDE.md).

use std::sync::Arc;

use crate::api_client::DataApiClient;
use crate::config::Config;

/// Top-level runtime state. Cheap to clone — everything inside is an
/// `Arc` or a small owned value.
pub struct AppState {
    /// Authenticated HTTP client for the Data API.
    pub api: Arc<DataApiClient>,
    /// Parsed env config.
    pub config: Config,
}

impl AppState {
    /// Construct a new `AppState`. The caller is responsible for having
    /// already authenticated the `DataApiClient`.
    pub fn new(api: Arc<DataApiClient>, config: Config) -> Arc<Self> {
        Arc::new(Self { api, config })
    }
}
