//! Per-session processing task.
//!
//! The event loop creates one runner per active session. Each runner owns
//! its `Pipeline` (chronicle-pipeline's unified streaming/one-shot handle)
//! and runs in its own tokio task, reading `RunnerCmd`s from an mpsc
//! channel.
//!
//! # State machine
//!
//! ```text
//!     [Streaming] --Finalize--> write outputs + PATCH transcribed --> [Done]
//!        |   \---- Abort -----> discard pipeline state              --> [Done]
//!
//!     [OneShot]                 download all chunks, run_one_shot,
//!                               write outputs + PATCH transcribed   --> [Done]
//! ```
//!
//! The runner never decides state from an if-ladder — the initial mode
//! selects a dedicated async fn, and the streaming fn's per-iteration
//! decision is a `match` on `RunnerCmd`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use chronicle_pipeline::{
    AudioChunk, Beat, OperatorKind, Pipeline, PipelineConfig, PipelineOutput, Scene, Segment,
    SessionAudio, SessionTrack, Timestamp, VadConfig,
};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::api_client::{CreateInputWire, DataApiClient};
use crate::config::{Config, CAPTURE_SAMPLE_RATE};
use crate::decode::decode_stereo_to_mono_i16;
use crate::error::{Result, WorkerError};
use crate::ids::{PseudoId, Seq, SessionId};
use crate::whisper::HttpWhisperClient;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Control messages the event loop sends to a running `SessionRunner`.
pub enum RunnerCmd {
    /// A new chunk is ready to be ingested. The runner fetches + decodes
    /// it on its own task to keep the event loop free.
    IngestChunk { pseudo: PseudoId, seq: Seq },
    /// Session has moved to `uploaded`. Finalize the streaming pipeline
    /// and write outputs, then shut down.
    Finalize,
    /// Forceful shutdown (drop pipeline state without writing).
    Abort,
}

/// Runner mode chosen at spawn time. Spec §"Pipeline lifecycle per session".
#[derive(Debug, Clone, Copy)]
pub enum RunnerMode {
    /// Streaming — spec normal case. Chunks arrive via `IngestChunk`;
    /// `Finalize` flushes remaining state and writes outputs.
    Streaming,
    /// One-shot — spec rerun / orphan-recovery case. The runner
    /// downloads every chunk up front and runs `Pipeline::run_one_shot`.
    OneShot,
}

/// Handle returned to the event loop. `join` is observed by the loop's
/// `JoinSet`; `tx` is retained for `IngestChunk` / `Finalize` / `Abort`.
pub struct SessionHandle {
    pub session: SessionId,
    pub tx: mpsc::Sender<RunnerCmd>,
    pub join: tokio::task::JoinHandle<Result<()>>,
}

pub struct SessionInputs {
    pub api: Arc<DataApiClient>,
    pub cfg: Arc<Config>,
    pub session: SessionId,
    pub mode: RunnerMode,
}

/// Spawn a new session runner task.
pub fn spawn(inputs: SessionInputs) -> SessionHandle {
    let (tx, rx) = mpsc::channel::<RunnerCmd>(64);
    let session = inputs.session;
    let join = tokio::spawn(run(inputs, rx));
    SessionHandle { session, tx, join }
}

async fn run(inputs: SessionInputs, rx: mpsc::Receiver<RunnerCmd>) -> Result<()> {
    match inputs.mode {
        RunnerMode::Streaming => run_streaming(inputs, rx).await,
        RunnerMode::OneShot => run_one_shot(inputs).await,
    }
}

// ---------------------------------------------------------------------------
// Streaming path
// ---------------------------------------------------------------------------

async fn run_streaming(
    inputs: SessionInputs,
    mut rx: mpsc::Receiver<RunnerCmd>,
) -> Result<()> {
    let SessionInputs { api, cfg, session, .. } = inputs;

    let mut pipeline = build_pipeline(&cfg)?;

    // Per-speaker bookkeeping. WS ordering within a single speaker is
    // guaranteed by data-api; across speakers we still sort by seq.
    // `processed` filters duplicate WS events; `pending` holds
    // out-of-order buffer; `next_expected` tracks the next seq to hand
    // to the pipeline.
    let mut processed: HashMap<String, HashSet<u32>> = HashMap::new();
    let mut pending: HashMap<String, BTreeMap<u32, Vec<u8>>> = HashMap::new();
    let mut next_expected: HashMap<String, u32> = HashMap::new();

    // Segment IDs we've already POSTed during the streaming phase, so
    // finalize doesn't re-send them.
    let mut posted_segment_ids: HashSet<Uuid> = HashSet::new();

    tracing::info!(%session, "session_runner: streaming started");

    loop {
        let Some(cmd) = rx.recv().await else { break };
        match cmd {
            RunnerCmd::IngestChunk { pseudo, seq } => {
                if let Err(e) = ingest_one(
                    &api,
                    session,
                    &pseudo,
                    seq,
                    &mut pipeline,
                    &mut processed,
                    &mut pending,
                    &mut next_expected,
                    &mut posted_segment_ids,
                )
                .await
                {
                    tracing::warn!(%session, error = %e, "streaming ingest failed");
                }
            }
            RunnerCmd::Finalize => {
                drop(rx); // stop accepting new chunks
                return finalize_streaming(api, session, pipeline, posted_segment_ids).await;
            }
            RunnerCmd::Abort => {
                tracing::info!(%session, "session_runner: aborted");
                return Ok(());
            }
        }
    }
    tracing::info!(%session, "session_runner: command channel closed before finalize");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn ingest_one(
    api: &DataApiClient,
    session: SessionId,
    pseudo: &PseudoId,
    seq: Seq,
    pipeline: &mut Pipeline,
    processed: &mut HashMap<String, HashSet<u32>>,
    pending: &mut HashMap<String, BTreeMap<u32, Vec<u8>>>,
    next_expected: &mut HashMap<String, u32>,
    posted_segment_ids: &mut HashSet<Uuid>,
) -> Result<()> {
    let pkey = pseudo.as_str().to_string();
    let seen = processed.entry(pkey.clone()).or_default();
    if seen.contains(&seq.as_u32()) {
        return Ok(());
    }

    let bytes = api.download_chunk(session, pseudo, seq).await?;

    let next = next_expected.entry(pkey.clone()).or_insert(0);
    let buf = pending.entry(pkey.clone()).or_default();

    if seq.as_u32() != *next {
        buf.insert(seq.as_u32(), bytes);
        tracing::debug!(
            %session, pseudo = %pseudo, seq = %seq, expected = *next,
            "buffered out-of-order chunk"
        );
        return Ok(());
    }

    // Feed this chunk + any consecutive buffered ones.
    let mut ordered: Vec<(u32, Vec<u8>)> = vec![(seq.as_u32(), bytes)];
    *next = seq.as_u32() + 1;
    while let Some(b) = buf.remove(next) {
        ordered.push((*next, b));
        *next += 1;
    }

    for (s, raw) in ordered {
        let pcm = decode_stereo_to_mono_i16(&raw);
        seen.insert(s);
        if pcm.is_empty() {
            continue;
        }
        let duration_ms =
            ((pcm.len() as u64) * 1000 / CAPTURE_SAMPLE_RATE as u64) as u32;
        let chunk = AudioChunk {
            session_id: session.as_uuid(),
            pseudo_id: pseudo.as_str().to_string(),
            seq: s,
            capture_started_at: 0 as Timestamp,
            duration_ms,
            pcm: Arc::from(pcm),
        };
        if let Err(e) = pipeline.ingest_chunk(chunk).await {
            tracing::warn!(%session, pseudo = %pseudo, seq = s, error = %e, "ingest_chunk failed");
        }
    }

    // Drain whatever is ready mid-stream.
    let drained = pipeline.emit();
    write_new_outputs(api, session, &drained, posted_segment_ids).await?;

    Ok(())
}

async fn finalize_streaming(
    api: Arc<DataApiClient>,
    session: SessionId,
    pipeline: Pipeline,
    mut posted_segment_ids: HashSet<Uuid>,
) -> Result<()> {
    let start = Instant::now();

    // Claim before finalizing. Atomic: 409 → ClaimLost → drop silently.
    match api.claim_session(session).await {
        Ok(()) => {}
        Err(WorkerError::ClaimLost(_)) => {
            tracing::info!(%session, "claim lost, peer worker owns it");
            return Ok(());
        }
        Err(e) => return Err(e),
    }

    let output = pipeline.finalize().await?;
    let segs_total = output.segments.len();
    let beats_total = output.beats.len();
    let scenes_total = output.scenes.len();

    write_new_outputs(&api, session, &output, &mut posted_segment_ids).await?;
    api.mark_transcribed(session).await?;

    tracing::info!(
        %session,
        segs_total, beats_total, scenes_total,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "session finalized (streaming)"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// One-shot path
// ---------------------------------------------------------------------------

async fn run_one_shot(inputs: SessionInputs) -> Result<()> {
    let SessionInputs { api, cfg, session, .. } = inputs;
    let start = Instant::now();

    // Claim — idempotent for orphan recovery (transcribing → transcribing
    // is an accepted no-op per the data-api state machine).
    match api.claim_session(session).await {
        Ok(()) => {}
        Err(WorkerError::ClaimLost(_)) => {
            tracing::info!(%session, "one-shot: claim lost, peer owns it");
            return Ok(());
        }
        Err(e) => return Err(e),
    }

    let tracks = collect_speaker_tracks(&api, session).await?;
    if tracks.is_empty() {
        tracing::warn!(%session, "one-shot: no tracks; marking transcribed");
        api.mark_transcribed(session).await?;
        return Ok(());
    }

    let pipeline = build_pipeline(&cfg)?;
    let audio = SessionAudio {
        session_id: session.as_uuid(),
        tracks,
    };
    let output = pipeline.run_one_shot(audio).await?;

    let mut posted: HashSet<Uuid> = HashSet::new();
    write_new_outputs(&api, session, &output, &mut posted).await?;
    api.mark_transcribed(session).await?;

    tracing::info!(
        %session,
        segs = output.segments.len(),
        beats = output.beats.len(),
        scenes = output.scenes.len(),
        elapsed_ms = start.elapsed().as_millis() as u64,
        "session finalized (one-shot)"
    );
    Ok(())
}

/// Clear any prior pipeline outputs for a session. Used on admin rerun
/// to guarantee idempotency of the subsequent one-shot run.
pub async fn clear_prior_outputs(api: &DataApiClient, session: SessionId) -> Result<()> {
    let (segs, beats, scenes) = tokio::join!(
        api.list_segment_ids(session),
        api.list_beat_ids(session),
        api.list_scene_ids(session),
    );
    let segs = segs.unwrap_or_default();
    let beats = beats.unwrap_or_default();
    let scenes = scenes.unwrap_or_default();

    for id in segs {
        let _ = api.delete_segment(id).await;
    }
    for id in beats {
        let _ = api.delete_beat(id).await;
    }
    for id in scenes {
        let _ = api.delete_scene(id).await;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_pipeline(cfg: &Config) -> Result<Pipeline> {
    let whisper = HttpWhisperClient::from_config(cfg)
        .map_err(|e| WorkerError::Config(format!("whisper: {e}")))?;

    let pcfg = PipelineConfig {
        operators: operator_chain(cfg),
        vad: VadConfig {
            model_path: Some(std::path::PathBuf::from(&cfg.vad_model_path)),
            ..Default::default()
        },
        ..Default::default()
    };

    Pipeline::builder(pcfg)
        .whisper(Arc::new(whisper))
        .build()
        .map_err(WorkerError::from)
}

/// The worker always runs VAD → Transcription → Filter → Segment → MetaTalk.
/// Beats + Scenes are enabled only when a scene-LLM endpoint is configured,
/// matching the spec's "LLM is optional" stance.
fn operator_chain(cfg: &Config) -> Vec<OperatorKind> {
    let mut ops = vec![
        OperatorKind::Vad,
        OperatorKind::Transcription,
        OperatorKind::Filter,
        OperatorKind::Segment,
        OperatorKind::MetaTalk,
    ];
    if cfg.scene_llm_url.is_some() {
        ops.push(OperatorKind::Beats);
        ops.push(OperatorKind::Scenes);
    }
    ops
}

async fn collect_speaker_tracks(
    api: &DataApiClient,
    session: SessionId,
) -> Result<Vec<SessionTrack>> {
    let participants = api.list_participants(session).await?;
    let consented: Vec<String> = participants
        .into_iter()
        .filter_map(|p| {
            let pid = p.pseudo_id?;
            let scope = p.consent_scope?;
            (scope == "full").then_some(pid)
        })
        .collect();

    let mut tracks = Vec::with_capacity(consented.len());
    for pid in consented {
        let pseudo = PseudoId::new(pid.clone());
        let mut chunks = api.list_chunks(session, &pseudo).await?;
        if chunks.is_empty() {
            continue;
        }
        chunks.sort_by_key(|c| c.seq);

        let mut raw = Vec::new();
        for c in &chunks {
            match api.download_chunk(session, &pseudo, Seq::new(c.seq)).await {
                Ok(b) => raw.extend_from_slice(&b),
                Err(e) => {
                    tracing::warn!(%session, pseudo = %pseudo, seq = c.seq, error = %e, "skip chunk");
                }
            }
        }
        let pcm = decode_stereo_to_mono_i16(&raw);
        tracks.push(SessionTrack {
            pseudo_id: pid,
            capture_started_at: 0 as Timestamp,
            pcm: Arc::from(pcm),
        });
    }
    Ok(tracks)
}

/// Post only the outputs we haven't posted before. Segment dedup is via
/// the segment's pipeline-assigned UUID (stable across `emit()` +
/// `finalize()`); beats and scenes only show up at finalize so we post
/// them unconditionally when the streaming fn's last call drains them.
async fn write_new_outputs(
    api: &DataApiClient,
    session: SessionId,
    out: &PipelineOutput,
    posted_segment_ids: &mut HashSet<Uuid>,
) -> Result<()> {
    let new_segments: Vec<&Segment> = out
        .segments
        .iter()
        .filter(|s| !posted_segment_ids.contains(&s.id))
        .collect();
    if !new_segments.is_empty() {
        let wires: Vec<CreateInputWire> = new_segments.iter().map(|s| segment_wire(s)).collect();
        post_segments_chunked(api, session, &wires).await?;
        for s in &new_segments {
            posted_segment_ids.insert(s.id);
        }
    }

    if !out.beats.is_empty() {
        let wires: Vec<CreateInputWire> = out.beats.iter().map(beat_wire).collect();
        api.post_beats(session, &wires).await?;
    }
    if !out.scenes.is_empty() {
        let wires: Vec<CreateInputWire> = out.scenes.iter().map(scene_wire).collect();
        api.post_scenes(session, &wires).await?;
    }
    Ok(())
}

async fn post_segments_chunked(
    api: &DataApiClient,
    session: SessionId,
    segs: &[CreateInputWire],
) -> Result<()> {
    for batch in segs.chunks(5) {
        api.post_segments(session, batch).await?;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Wire conversions
// ---------------------------------------------------------------------------

fn segment_wire(s: &Segment) -> CreateInputWire {
    let flags = s
        .flags
        .meta_talk
        .as_ref()
        .map(|mt| serde_json::json!({ "meta_talk": mt }));
    CreateInputWire {
        client_id: s.id.to_string(),
        start_ms: s.start_ms as i64,
        end_ms: s.end_ms as i64,
        pseudo_id: Some(s.pseudo_id.clone()),
        text: Some(s.text.clone()),
        title: None,
        summary: None,
        confidence: Some(s.confidence as f64),
        flags,
        original: Some(serde_json::json!({
            "text": s.original,
            "confidence": s.confidence,
            "language": s.language,
        })),
    }
}

fn beat_wire(b: &Beat) -> CreateInputWire {
    CreateInputWire {
        client_id: b.id.to_string(),
        start_ms: b.t_ms as i64,
        end_ms: b.t_ms as i64,
        pseudo_id: None,
        text: None,
        title: Some(b.label.clone()),
        summary: Some(format!("{:?}", b.kind)),
        confidence: Some(b.confidence as f64),
        flags: None,
        original: None,
    }
}

fn scene_wire(s: &Scene) -> CreateInputWire {
    CreateInputWire {
        client_id: s.id.to_string(),
        start_ms: s.start_ms as i64,
        end_ms: s.end_ms as i64,
        pseudo_id: None,
        text: None,
        title: Some(s.label.clone()),
        summary: Some(String::new()),
        confidence: Some(s.confidence as f64),
        flags: None,
        original: None,
    }
}
