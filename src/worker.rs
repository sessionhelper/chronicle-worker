//! The worker's main polling loop.
//!
//! # Shape
//!
//! ```text
//! loop {
//!     sleep(poll_interval);
//!     match process_next_session(&state).await {
//!         Ok(Some(id)) => info!("session_processed"),
//!         Ok(None)     => continue,         // queue empty
//!         Err(e)       => error!("process_failed"),
//!     }
//! }
//! ```
//!
//! Errors never kill the loop — they log and fall through to the next
//! tick. The worker is expected to run as a long-lived daemon and
//! recover from transient Data API / Whisper outages on its own.
//!
//! This module is **scaffolding**. The heavy lifting inside
//! [`process_next_session`] is behind `TODO:` markers; search for
//! those to find the A1 implementation checklist.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::api_client::{ApiError, Participant, Segment, SessionSummary};
use crate::state::AppState;

#[derive(thiserror::Error, Debug)]
pub enum WorkerError {
    #[error("API error: {0}")]
    Api(#[from] ApiError),
    #[error("Pipeline error: {0}")]
    Pipeline(#[from] ovp_pipeline::PipelineError),
}

pub type Result<T> = std::result::Result<T, WorkerError>;

/// Main entrypoint called from `main.rs`. Runs forever.
///
/// Returns `Result` only so `main` can `?` it — in practice this
/// function never returns `Ok` and only surfaces an error if the
/// loop itself becomes unrecoverable (which it currently cannot).
pub async fn run(state: Arc<AppState>) -> Result<()> {
    let poll_interval = Duration::from_secs(state.config.poll_interval_secs);
    tracing::info!(
        poll_interval_secs = state.config.poll_interval_secs,
        "worker loop started"
    );

    loop {
        tokio::time::sleep(poll_interval).await;

        match process_next_session(&state).await {
            Ok(Some(id)) => {
                tracing::info!(session_id = %id, "session_processed");
            }
            Ok(None) => {
                // Nothing to do this tick — fall through and sleep again.
                tracing::trace!("no uploaded sessions; idling");
            }
            Err(e) => {
                tracing::error!(error = %e, "process_failed");
                // Intentionally do not bail — keep polling.
            }
        }
    }
}

/// Pull one `uploaded` session off the queue and run it end-to-end.
///
/// Returns `Ok(Some(id))` on success, `Ok(None)` when the queue is
/// empty, `Err(_)` on any fatal-for-this-session error. The caller
/// logs and keeps going.
pub async fn process_next_session(state: &AppState) -> Result<Option<Uuid>> {
    // ---- 1. Discover work --------------------------------------------
    let uploaded: Vec<SessionSummary> = state.api.list_uploaded_sessions().await?;
    let Some(session) = uploaded.into_iter().next() else {
        return Ok(None);
    };
    let session_id = session.id;

    tracing::info!(session_id = %session_id, "claiming session");

    // Mark transcribing so a second worker won't pick it up. Best-effort
    // — if this fails we still try to process, because the alternative
    // (bailing) leaves the session stuck forever.
    //
    // TODO: once multiple workers run in parallel, switch to an
    // atomic claim endpoint (`POST /internal/sessions/{id}/claim`)
    // that returns 409 if someone else got there first. Status-based
    // claiming is racy.
    if let Err(e) = state
        .api
        .update_session_state(session_id, "transcribing")
        .await
    {
        tracing::warn!(session_id = %session_id, error = %e, "failed to mark transcribing");
    }

    // ---- 2. List consented participants ------------------------------
    let participants: Vec<Participant> = state.api.list_participants(session_id).await?;
    let consented: Vec<Participant> = participants
        .into_iter()
        .filter(|p| p.consent_scope.as_deref() == Some("full"))
        .collect();

    if consented.is_empty() {
        tracing::warn!(session_id = %session_id, "no consented participants; marking transcribed with 0 segments");
        state.api.update_session_state(session_id, "transcribed").await?;
        return Ok(Some(session_id));
    }

    // ---- 3. Download audio per speaker -------------------------------
    //
    // TODO: for each consented participant, download every chunk and
    // build a `SpeakerTrack`. There is no "list chunks" call stubbed
    // on `DataApiClient` yet — either add one back (the endpoint
    // exists: `GET /internal/sessions/{id}/audio/{pseudo}/chunks`)
    // or grow `download_chunk` into a streaming "download all chunks"
    // variant. See `api_client.rs` TODOs.
    //
    // TODO: decode raw bytes to mono f32. The collector uploads
    // stereo s16le at 48kHz (two bytes per sample, two channels per
    // frame). The decode is:
    //     let samples: Vec<f32> = raw.chunks_exact(2)
    //         .map(|p| i16::from_le_bytes([p[0], p[1]]) as f32 / i16::MAX as f32)
    //         .collect();
    //     let mono: Vec<f32> = samples.chunks_exact(2)
    //         .map(|f| (f[0] + f[1]) / 2.0)
    //         .collect();
    // Keep this in a dedicated `fn decode_stereo_to_mono(&[u8]) ->
    // Vec<f32>` so it's unit-testable in isolation.
    //
    // TODO: verify the sample rate assumption. The collector hardcodes
    // 48000 today (Discord's native rate), but if that ever changes
    // the worker must read it from session metadata, not assume.
    let tracks: Vec<ovp_pipeline::SpeakerTrack> = {
        let _ = &consented; // silence unused-var warning while stubbed
        Vec::new()
    };

    // ---- 4. Invoke ovp-pipeline --------------------------------------
    //
    // TODO: build a `PipelineConfig` from `state.config` (whisper_url,
    // whisper_model, vad_model_path, optional scene/beat LLM). See the
    // pre-scaffold commit for a working construction — it was ripped
    // out here to keep the skeleton tight.
    //
    // TODO: call `ovp_pipeline::process_session(&pipeline_config,
    // SessionInput { session_id, tracks }, &mut operators).await?`
    // and capture the `PipelineResult`.
    //
    // TODO: choose operators via `default_operators()` or
    // `operators_with_llm_scene(beat_cfg, scene_cfg)` depending on
    // whether `config.scene_llm_url` is Some.
    let segments_out: Vec<Segment> = {
        let _ = &tracks;
        Vec::new()
    };

    // ---- 5. Post segments back ---------------------------------------
    //
    // TODO: once the pipeline produces `Vec<TranscriptSegment>`, filter
    // to `!excluded` and map into `api_client::Segment`. The mapping is
    // a straightforward field-by-field (see Segment in api_client.rs).
    state.api.post_segments(session_id, segments_out).await?;

    // ---- 6. Finalize state -------------------------------------------
    state
        .api
        .update_session_state(session_id, "transcribed")
        .await?;

    Ok(Some(session_id))
}
