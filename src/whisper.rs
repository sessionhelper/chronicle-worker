//! Whisper HTTP client used by the pipeline's transcription operator.
//!
//! Today the chronicle-pipeline crate owns the actual Whisper transport
//! via its `TranscriberConfig`. The worker's role in Whisper-land is
//! just to construct that config from env — the retry logic and the
//! HTTP call both live in the pipeline crate.
//!
//! This module exists so future work (once the pipeline refactors to
//! an injected `WhisperClient` trait per the spec) has a clear seam.
//! Until then, `whisper_config` is the only API callers use.

use chronicle_pipeline::TranscriberConfig;

use crate::config::Config;

/// Build the pipeline-side Whisper config from worker env.
///
/// Hallucination knobs are kept here (rather than in pipeline defaults)
/// because they're tuned per deployment — the worker is the right owner.
pub fn whisper_config(cfg: &Config) -> TranscriberConfig {
    TranscriberConfig {
        endpoint: cfg.whisper_url.clone(),
        model: cfg.whisper_model.clone(),
        language: Some("en".into()),
        initial_prompt: Some(
            "TTRPG session dialogue. Multiple speakers discussing combat, exploration, and roleplay."
                .into(),
        ),
        beam_size: 5,
        temperature: vec![0.0, 0.2, 0.4],
        hallucination_logprob_threshold: -0.4,
        hallucination_no_speech_threshold: 0.5,
        hallucination_compression_ratio: 1.8,
    }
}
