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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::api_client::{ApiError, Participant, Segment, SessionSummary};
use crate::decode::decode_stereo_to_mono;
use crate::state::AppState;

use ovp_pipeline::{
    default_operators, operators_with_llm_scene, process_session, PipelineConfig, PipelineResult,
    SessionInput, SpeakerTrack, TranscriberConfig, VadConfig,
};
use ovp_pipeline::operators::{beat::BeatConfig, scene::SceneConfig};

/// Collector-side invariant: all PCM chunks are uploaded as s16le stereo
/// at Discord's native 48kHz mix rate. The pipeline resamples to 16kHz
/// internally; the worker only has to tell it the input rate. If the
/// collector ever exposes session-scoped capture rates, this constant
/// becomes a per-session lookup against session metadata — nothing else
/// in the worker should care.
const CAPTURE_SAMPLE_RATE: u32 = 48_000;

#[derive(thiserror::Error, Debug)]
pub enum WorkerError {
    #[error("API error: {0}")]
    Api(#[from] ApiError),
    #[error("Pipeline error: {0}")]
    Pipeline(#[from] ovp_pipeline::PipelineError),
}

pub type Result<T> = std::result::Result<T, WorkerError>;

/// Abstraction over the pipeline call so integration tests can swap in a
/// stub without a real Whisper/VAD stack. Production wires
/// [`RealPipelineRunner`] which calls [`ovp_pipeline::process_session`].
///
/// Hand-rolled boxed-future return (instead of `async_trait`) keeps the
/// dependency list tight. Only one trait, one impl, one call site — not
/// worth a proc-macro.
pub trait PipelineRunner: Send + Sync {
    fn run<'a>(
        &'a self,
        config: &'a PipelineConfig,
        input: SessionInput,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = ovp_pipeline::Result<PipelineResult>> + Send + 'a>,
    >;
}

/// Real production pipeline runner. Delegates to `ovp_pipeline::process_session`
/// with the operator chain chosen from config.
pub struct RealPipelineRunner {
    /// GM speaker id for LLM beat/scene operators, if configured.
    pub gm_speaker_id: Option<String>,
    /// Optional LLM endpoint for scene detection. When `None`, the
    /// default (non-LLM) operator chain is used.
    pub scene_llm_url: Option<String>,
    /// LLM model name for scene detection.
    pub scene_llm_model: String,
}

impl PipelineRunner for RealPipelineRunner {
    fn run<'a>(
        &'a self,
        config: &'a PipelineConfig,
        input: SessionInput,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = ovp_pipeline::Result<PipelineResult>> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut operators = match &self.scene_llm_url {
                Some(url) => {
                    let beat_cfg = BeatConfig {
                        endpoint: url.clone(),
                        model: self.scene_llm_model.clone(),
                        gm_speaker_id: self.gm_speaker_id.clone(),
                        ..Default::default()
                    };
                    let scene_cfg = SceneConfig {
                        endpoint: url.clone(),
                        model: self.scene_llm_model.clone(),
                        gm_speaker_id: self.gm_speaker_id.clone(),
                        ..Default::default()
                    };
                    operators_with_llm_scene(beat_cfg, scene_cfg)
                }
                None => default_operators(),
            };
            process_session(config, input, &mut operators).await
        })
    }
}

/// Main entrypoint called from `main.rs`. Runs forever.
///
/// Returns `Result` only so `main` can `?` it — in practice this
/// function never returns `Ok` and only surfaces an error if the
/// loop itself becomes unrecoverable (which it currently cannot).
pub async fn run(state: Arc<AppState>) -> Result<()> {
    let runner: Arc<dyn PipelineRunner> = Arc::new(RealPipelineRunner {
        gm_speaker_id: state.config.gm_speaker_id.clone(),
        scene_llm_url: state.config.scene_llm_url.clone(),
        scene_llm_model: state.config.scene_llm_model.clone(),
    });
    let poll_interval = Duration::from_secs(state.config.poll_interval_secs);
    tracing::info!(
        poll_interval_secs = state.config.poll_interval_secs,
        "worker loop started"
    );

    loop {
        tokio::time::sleep(poll_interval).await;

        match process_next_session(&state, runner.as_ref()).await {
            Ok(Some(id)) => {
                tracing::info!(session_id = %id, "session_processed");
            }
            Ok(None) => {
                tracing::trace!("no uploaded sessions; idling");
            }
            Err(e) => {
                tracing::error!(error = %e, "process_failed");
            }
        }
    }
}

/// Pull one `uploaded` session off the queue and run it end-to-end.
///
/// Returns `Ok(Some(id))` on success, `Ok(None)` when the queue is
/// empty, `Err(_)` on any fatal-for-this-session error. The caller
/// logs and keeps going.
pub async fn process_next_session(
    state: &AppState,
    runner: &dyn PipelineRunner,
) -> Result<Option<Uuid>> {
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
    // TODO: once multiple workers run in parallel, switch to an atomic
    // claim endpoint that returns 409 if someone else got there first.
    if let Err(e) = state
        .api
        .update_session_state(session_id, "transcribing")
        .await
    {
        tracing::warn!(session_id = %session_id, error = %e, "failed to mark transcribing");
    }

    // ---- 2. List consented participants ------------------------------
    //
    // Consent rule: transcribe every participant who actively consented
    // to recording. We do NOT filter on the license flags
    // (`no_llm_training`, `no_public_release`) here — those gate
    // *downstream publication* (dataset release, LLM training corpus),
    // not transcription. A user whose flags are both set still gets
    // their own transcript in the participant portal for personal
    // review; the worker's job is to produce that transcript.
    //
    // Future wire-model note: the spec is migrating to a two-flag model
    // (binary `consent_given` + license flags), but the data-api and
    // collector still speak the `consent_scope` string today
    // (`"full"`/`"decline"`). We accept `Some("full")` as "consented"
    // here and will revisit when the schema rolls forward.
    let participants: Vec<Participant> = state.api.list_participants(session_id).await?;
    let consented: Vec<Participant> = participants
        .into_iter()
        .filter(|p| {
            p.consent_scope.as_deref() == Some("full") && p.user_pseudo_id.is_some()
        })
        .collect();

    if consented.is_empty() {
        tracing::warn!(
            session_id = %session_id,
            "no consented participants; marking transcribed with 0 segments"
        );
        state
            .api
            .update_session_state(session_id, "transcribed")
            .await?;
        return Ok(Some(session_id));
    }

    // ---- 3. Download audio per speaker -------------------------------
    let mut tracks: Vec<SpeakerTrack> = Vec::with_capacity(consented.len());
    for p in &consented {
        // Safe: filtered above.
        let pseudo_id = p.user_pseudo_id.as_deref().expect("filtered to Some");
        let chunks = state.api.list_chunks(session_id, pseudo_id).await?;
        if chunks.is_empty() {
            tracing::warn!(
                session_id = %session_id,
                pseudo_id,
                "consented participant has no audio chunks; skipping"
            );
            continue;
        }

        // Concatenate raw bytes in seq order. `list_chunks` already sorts
        // by seq on the server side but we re-sort defensively — a wire
        // reorder (proxies, retries) would silently desync timestamps.
        let mut ordered = chunks;
        ordered.sort_by_key(|c| c.seq);

        let mut raw: Vec<u8> = Vec::new();
        for chunk in ordered {
            let bytes = state
                .api
                .download_chunk(session_id, pseudo_id, chunk.seq)
                .await?;
            raw.extend_from_slice(&bytes);
        }

        let samples = decode_stereo_to_mono(&raw);
        tracing::debug!(
            session_id = %session_id,
            pseudo_id,
            raw_bytes = raw.len(),
            samples = samples.len(),
            "decoded speaker audio"
        );

        tracks.push(SpeakerTrack {
            pseudo_id: pseudo_id.to_string(),
            samples,
            sample_rate: CAPTURE_SAMPLE_RATE,
        });
    }

    if tracks.is_empty() {
        tracing::warn!(session_id = %session_id, "no audio to transcribe; marking transcribed");
        state
            .api
            .update_session_state(session_id, "transcribed")
            .await?;
        return Ok(Some(session_id));
    }

    // ---- 4. Invoke ovp-pipeline --------------------------------------
    let pipeline_config = PipelineConfig {
        vad: VadConfig {
            model_path: PathBuf::from(&state.config.vad_model_path),
            ..Default::default()
        },
        whisper: TranscriberConfig {
            endpoint: state.config.whisper_url.clone(),
            model: state.config.whisper_model.clone(),
            language: Some("en".into()),
        },
        ..Default::default()
    };

    let result = runner
        .run(&pipeline_config, SessionInput { session_id, tracks })
        .await?;

    // ---- 5. Post segments back ---------------------------------------
    let segments_out: Vec<Segment> = result
        .segments
        .into_iter()
        .filter(|s| !s.excluded)
        .map(|s| Segment {
            segment_index: s.segment_index as i32,
            speaker_pseudo_id: s.speaker_pseudo_id,
            start_time: s.start_time as f64,
            end_time: s.end_time as f64,
            text: s.text,
            original_text: s.original_text,
            confidence: s.confidence.map(|c| c as f64),
        })
        .collect();

    tracing::info!(
        session_id = %session_id,
        segment_count = segments_out.len(),
        "posting segments"
    );
    state.api.post_segments(session_id, segments_out).await?;

    // ---- 6. Finalize state -------------------------------------------
    state
        .api
        .update_session_state(session_id, "transcribed")
        .await?;

    Ok(Some(session_id))
}
