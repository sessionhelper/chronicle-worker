//! Worker configuration, env-driven.
//!
//! Matches `chronicle-worker.md` §Interfaces.Environment-variables.
//!
//! Two naming quirks kept for backward compatibility with the running
//! fleet: `DATA_API_SHARED_SECRET` is still accepted as an alias for
//! `SHARED_SECRET`, and `LOG_LEVEL` still maps to the `RUST_LOG`-style
//! filter. New code should prefer the spec-canonical names.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;

/// Fixed capture rate across the bot fleet (Discord native).
pub const CAPTURE_SAMPLE_RATE: u32 = 48_000;

/// Worker runtime configuration.
#[derive(Debug, Clone, Parser)]
#[command(name = "chronicle-worker", about = "Chronicle transcription worker")]
pub struct Config {
    /// Data API base URL (no trailing slash).
    #[arg(long, env = "DATA_API_URL", default_value = "http://127.0.0.1:8001")]
    pub data_api_url: String,

    /// Shared secret for data-api auth. Accepts either spec-canonical
    /// `SHARED_SECRET` or legacy `DATA_API_SHARED_SECRET`.
    #[arg(long, env = "SHARED_SECRET")]
    pub shared_secret: Option<String>,

    /// Legacy alias (same as `SHARED_SECRET`).
    #[arg(long = "legacy-shared-secret", env = "DATA_API_SHARED_SECRET")]
    pub legacy_shared_secret: Option<String>,

    /// REST reconciliation interval (safety net when WS is down).
    #[arg(long, env = "POLL_INTERVAL_SECS", default_value_t = 10)]
    pub poll_interval_secs: u64,

    /// Whisper HTTP endpoint.
    #[arg(long, env = "WHISPER_URL", default_value = "http://localhost:8300/v1/audio/transcriptions")]
    pub whisper_url: String,

    /// Whisper model identifier.
    #[arg(long, env = "WHISPER_MODEL", default_value = "Systran/faster-whisper-large-v3")]
    pub whisper_model: String,

    /// Silero VAD ONNX model path.
    #[arg(long, env = "VAD_MODEL_PATH", default_value = "models/silero_vad_v6.onnx")]
    pub vad_model_path: String,

    /// Silero probability threshold (0.0-1.0). Lower = more permissive.
    /// Silero's own README uses 0.5 for clean audio; 0.3 is typical for
    /// Discord's lossy DAVE-decrypted Opus.
    #[arg(long, env = "VAD_THRESHOLD", default_value_t = 0.5)]
    pub vad_threshold: f32,

    /// Minimum continuous speech in a region before emitting. Shorter =
    /// more short utterances make it to Whisper.
    #[arg(long, env = "VAD_MIN_SPEECH_MS", default_value_t = 250)]
    pub vad_min_speech_ms: u32,

    /// Silence that ends a speech region. Longer = merges short pauses.
    #[arg(long, env = "VAD_MIN_SILENCE_MS", default_value_t = 800)]
    pub vad_min_silence_ms: u32,

    /// Padding prepended/appended to each emitted region. Helps Whisper
    /// not clip the start/end consonants.
    #[arg(long, env = "VAD_PAD_MS", default_value_t = 100)]
    pub vad_pad_ms: u32,

    /// Enable the admin HTTP surface.
    #[arg(long, env = "WORKER_ADMIN_ENABLED", default_value_t = false)]
    pub admin_enabled: bool,

    /// Admin HTTP bind address (loopback by default).
    #[arg(long, env = "ADMIN_BIND_ADDR", default_value = "127.0.0.1:8020")]
    pub admin_bind_addr: SocketAddr,

    /// Session-level retry backoffs in milliseconds, CSV.
    /// Defaults to the spec's 30s / 2m / 10m.
    #[arg(long, env = "RETRY_BACKOFF_MS", default_value = "30000,120000,600000")]
    pub retry_backoff_ms: String,

    /// Tracing filter (falls back to RUST_LOG if present).
    #[arg(long, env = "LOG_LEVEL", default_value = "chronicle_worker=info,chronicle_pipeline=info")]
    pub log_level: String,

    // -- Pipeline knobs forwarded to chronicle-pipeline ------------------

    /// Optional LLM endpoint for scene detection.
    #[arg(long, env = "SCENE_LLM_URL")]
    pub scene_llm_url: Option<String>,

    /// LLM model name for scene detection.
    #[arg(long, env = "SCENE_LLM_MODEL", default_value = "qwen2.5:7b")]
    pub scene_llm_model: String,

    /// Optional GM speaker pseudo_id for scene/beat operators.
    #[arg(long, env = "GM_SPEAKER_ID")]
    pub gm_speaker_id: Option<String>,
}

impl Config {
    /// Parse from the process environment.
    pub fn from_env() -> Self { Self::parse() }

    /// The effective shared secret, preferring the canonical env var.
    pub fn resolved_shared_secret(&self) -> std::result::Result<&str, crate::error::WorkerError> {
        self.shared_secret
            .as_deref()
            .or(self.legacy_shared_secret.as_deref())
            .ok_or_else(|| {
                crate::error::WorkerError::Config(
                    "SHARED_SECRET (or DATA_API_SHARED_SECRET) must be set".into(),
                )
            })
    }

    /// Parsed retry schedule, exposed as `Duration`s so callers don't
    /// re-parse the CSV.
    pub fn retry_backoffs(&self) -> std::result::Result<Vec<Duration>, crate::error::WorkerError> {
        self.retry_backoff_ms
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<u64>()
                    .map(Duration::from_millis)
                    .map_err(|e| crate::error::WorkerError::Config(format!("RETRY_BACKOFF_MS: {e}")))
            })
            .collect()
    }

    /// REST poll interval as a `Duration`.
    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }
}
