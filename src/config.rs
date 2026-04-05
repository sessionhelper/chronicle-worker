//! Worker configuration.
//!
//! Parsed from environment variables via `clap` derive. Every env var
//! has a sensible default except `DATA_API_SHARED_SECRET`, which is
//! required for service-to-service auth.

use clap::Parser;

/// Runtime configuration for `ovp-worker`.
///
/// All fields are populated from environment variables. `clap` is used
/// purely for its env + derive machinery; the worker does not take any
/// command-line flags today.
#[derive(Debug, Clone, Parser)]
#[command(name = "ovp-worker", about = "Batch transcription worker")]
pub struct Config {
    /// Base URL of the Data API (no trailing slash).
    #[arg(long, env = "DATA_API_URL", default_value = "http://127.0.0.1:8001")]
    pub data_api_url: String,

    /// Shared secret used for service-to-service auth with the Data API.
    /// Must match the `SHARED_SECRET` env var on the Data API container.
    #[arg(long, env = "DATA_API_SHARED_SECRET")]
    pub shared_secret: String,

    /// How often to poll for new `uploaded` sessions, in seconds.
    #[arg(long, env = "POLL_INTERVAL_SECS", default_value_t = 10)]
    pub poll_interval_secs: u64,

    /// Tracing filter string (e.g. `info`, `ovp_worker=debug,warn`).
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    // ---- Pipeline knobs -----------------------------------------------
    // These belong to ovp-pipeline, not the worker's own loop. They're
    // plumbed here because the worker is the caller that constructs
    // `PipelineConfig`. If the worker ever grows a proper config file,
    // these should move into a nested `[pipeline]` section.
    /// Whisper HTTP endpoint used by `ovp-pipeline`.
    #[arg(
        long,
        env = "WHISPER_URL",
        default_value = "http://localhost:8300/v1/audio/transcriptions"
    )]
    pub whisper_url: String,

    /// Whisper model identifier.
    #[arg(
        long,
        env = "WHISPER_MODEL",
        default_value = "deepdml/faster-whisper-large-v3-turbo-ct2"
    )]
    pub whisper_model: String,

    /// Path to the Silero VAD ONNX model on disk.
    #[arg(long, env = "VAD_MODEL_PATH", default_value = "models/silero_vad_v6.onnx")]
    pub vad_model_path: String,

    /// Optional LLM endpoint for scene detection.
    #[arg(long, env = "SCENE_LLM_URL")]
    pub scene_llm_url: Option<String>,

    /// Optional LLM model name for scene detection.
    #[arg(long, env = "SCENE_LLM_MODEL", default_value = "qwen2.5:7b")]
    pub scene_llm_model: String,

    /// Optional GM speaker pseudo_id for scene/beat operators.
    #[arg(long, env = "GM_SPEAKER_ID")]
    pub gm_speaker_id: Option<String>,
}

impl Config {
    /// Parse configuration from the process environment.
    pub fn from_env() -> Self {
        Self::parse()
    }
}
