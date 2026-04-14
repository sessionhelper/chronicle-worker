//! Per-session processing task.
//!
//! The event loop creates one `SessionRunner` per active session. Each
//! runner owns its `StreamingPipeline` and runs in its own tokio task,
//! reading commands (feed chunk / finalize) from an mpsc channel.
//!
//! # State machine
//!
//! ```text
//!     [Streaming] --finalize()--> [Finalizing] --done--> [Done]
//!          |                              |
//!          +----- one-shot path -----> [OneShotRunning] -> [Done]
//! ```
//!
//! The runner never "decides" state from an if-ladder; the public API
//! takes the initial mode (`Streaming` or `OneShot`) and the task loop
//! is a `match` on a small enum in each iteration.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use chronicle_pipeline::operators::{beat::BeatConfig, scene::SceneConfig};
use chronicle_pipeline::{
    default_operators, operators_with_llm_scene, process_session, Operator, PipelineConfig,
    PipelineResult, SessionInput, SpeakerTrack, StreamingConfig, StreamingPipeline, VadConfig,
};
use tokio::sync::mpsc;

use crate::api_client::{BeatWire, DataApiClient, SceneWire, SegmentWire};
use crate::config::{Config, CAPTURE_SAMPLE_RATE};
use crate::decode::decode_stereo_to_mono;
use crate::error::{Result, WorkerError};
use crate::ids::{PseudoId, Seq, SessionId};
use crate::whisper::whisper_config;

/// Control messages the event loop sends to a running `SessionRunner`.
pub enum RunnerCmd {
    /// A new chunk is ready to be ingested. The runner downloads +
    /// decodes it on its own thread to keep the event loop free.
    IngestChunk { pseudo: PseudoId, seq: Seq },
    /// Finalize the streaming pipeline and write outputs. Shuts the
    /// runner down on completion.
    Finalize,
    /// Forceful shutdown (drop pipeline state without writing).
    Abort,
}

/// The mode the runner was constructed in. Spec §"Pipeline lifecycle per session".
pub enum RunnerMode {
    /// Streaming — spec's normal case. Chunks arrive via `IngestChunk`;
    /// `Finalize` flushes remaining state and writes outputs.
    Streaming,
    /// One-shot — spec's rerun / orphan-recovery case. The runner
    /// downloads every chunk up front and runs `process_session`.
    OneShot,
}

/// Handle returned to the event loop.
///
/// The `join` field is the raw `JoinHandle` that the event loop moves
/// into its `JoinSet` to observe completion. `tx` is retained for
/// command dispatch (ingest, finalize, abort).
pub struct SessionHandle {
    pub session: SessionId,
    pub tx: mpsc::Sender<RunnerCmd>,
    pub join: tokio::task::JoinHandle<Result<()>>,
}

/// Inputs needed to spawn a runner.
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

// --- Streaming path ------------------------------------------------------

async fn run_streaming(
    inputs: SessionInputs,
    mut rx: mpsc::Receiver<RunnerCmd>,
) -> Result<()> {
    let SessionInputs { api, cfg, session, .. } = inputs;

    let stream_cfg = StreamingConfig {
        rms: Default::default(),
        vad: VadConfig {
            model_path: PathBuf::from(&cfg.vad_model_path),
            ..Default::default()
        },
        whisper: whisper_config(&cfg),
        input_sample_rate: CAPTURE_SAMPLE_RATE,
        session_id: session.as_uuid(),
    };
    let mut pipeline = StreamingPipeline::new(stream_cfg);

    // Ordered buffer of already-seen chunks per speaker. Guards against
    // duplicate events and out-of-order arrivals (WS does not guarantee
    // cross-speaker ordering, though the spec's intra-session ordering
    // is claimed).
    let mut processed: HashMap<String, HashSet<u32>> = HashMap::new();
    let mut pending: HashMap<String, BTreeMap<u32, Vec<u8>>> = HashMap::new();
    let mut next_expected: HashMap<String, u32> = HashMap::new();

    // Segments already POSTed during streaming — so finalize doesn't
    // double-post them.
    let mut posted_count: u32 = 0;

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
                    &mut posted_count,
                )
                .await
                {
                    tracing::warn!(%session, error = %e, "streaming ingest failed");
                }
            }
            RunnerCmd::Finalize => {
                drop(rx); // stop accepting new chunks
                return finalize_streaming(api, cfg, session, pipeline, posted_count).await;
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
    pipeline: &mut StreamingPipeline,
    processed: &mut HashMap<String, HashSet<u32>>,
    pending: &mut HashMap<String, BTreeMap<u32, Vec<u8>>>,
    next_expected: &mut HashMap<String, u32>,
    posted_count: &mut u32,
) -> Result<()> {
    let seen = processed.entry(pseudo.as_str().to_string()).or_default();
    if seen.contains(&seq.as_u32()) {
        return Ok(());
    }

    let bytes = api.download_chunk(session, pseudo, seq).await?;
    let samples = decode_stereo_to_mono(&bytes);

    // Order buffer.
    let next = next_expected.entry(pseudo.as_str().to_string()).or_insert(0);
    let buf = pending.entry(pseudo.as_str().to_string()).or_default();

    if seq.as_u32() != *next {
        buf.insert(seq.as_u32(), bytes);
        tracing::debug!(%session, pseudo = %pseudo, seq = %seq, expected = *next, "buffered out-of-order chunk");
        return Ok(());
    }

    // Feed this and any consecutive buffered chunks.
    let mut to_feed: Vec<(u32, Vec<f32>)> = vec![(seq.as_u32(), samples)];
    *next = seq.as_u32() + 1;
    while let Some(b) = buf.remove(next) {
        let s = decode_stereo_to_mono(&b);
        to_feed.push((*next, s));
        *next += 1;
    }

    let mut new_segments = Vec::new();
    for (s, samples) in to_feed {
        if samples.is_empty() {
            seen.insert(s);
            continue;
        }
        match pipeline.feed_chunk(pseudo.as_str(), samples).await {
            Ok(mut segs) => {
                seen.insert(s);
                new_segments.append(&mut segs);
            }
            Err(e) => {
                tracing::warn!(%session, pseudo = %pseudo, seq = s, error = %e, "feed_chunk failed");
                seen.insert(s);
            }
        }
    }

    if !new_segments.is_empty() {
        let wire = to_segment_wires(&new_segments);
        post_segments_chunked(api, session, &wire).await?;
        *posted_count += wire.len() as u32;
    }
    Ok(())
}

async fn finalize_streaming(
    api: Arc<DataApiClient>,
    cfg: Arc<Config>,
    session: SessionId,
    pipeline: StreamingPipeline,
    already_posted: u32,
) -> Result<()> {
    let start = Instant::now();

    // Claim before finalizing. The atomic claim is the contract: first
    // worker that PATCHes to `transcribing` wins. A 409 surfaces as
    // `ClaimLost` — we drop the session silently.
    match api.claim_session(session).await {
        Ok(()) => {}
        Err(WorkerError::ClaimLost(_)) => {
            tracing::info!(%session, "claim lost, peer worker owns it");
            return Ok(());
        }
        Err(e) => return Err(e),
    }

    let mut operators = build_operators(&cfg);
    let result = pipeline.finalize(&mut operators).await?;

    // Only POST the segments that weren't emitted during streaming
    // (by segment_index). The final `result.segments` vec is re-indexed
    // chronologically after sort; we rely on the count diff to know how
    // many are "new".
    let all_segs = to_segment_wires(&result.segments);
    let new_segs: Vec<_> = all_segs
        .into_iter()
        .filter(|s| s.segment_index >= already_posted as i32)
        .collect();
    post_segments_chunked(&api, session, &new_segs).await?;

    let beats = to_beat_wires(&result.beats);
    let scenes = to_scene_wires(&result.scenes);
    api.post_beats(session, &beats).await?;
    api.post_scenes(session, &scenes).await?;

    api.mark_transcribed(session).await?;

    tracing::info!(
        %session,
        segs_total = result.segments.len(),
        new_segs = new_segs.len(),
        beats = beats.len(),
        scenes = scenes.len(),
        elapsed_ms = start.elapsed().as_millis() as u64,
        "session finalized (streaming)"
    );
    Ok(())
}

// --- One-shot path -------------------------------------------------------

async fn run_one_shot(inputs: SessionInputs) -> Result<()> {
    let SessionInputs { api, cfg, session, .. } = inputs;
    let start = Instant::now();

    // Claim (idempotent for orphan recovery: transcribing -> transcribing
    // is accepted as a no-op transition by the data-api state machine).
    match api.claim_session(session).await {
        Ok(()) => {}
        Err(WorkerError::ClaimLost(_)) => {
            tracing::info!(%session, "one-shot: claim lost, peer owns it");
            return Ok(());
        }
        Err(e) => return Err(e),
    }

    // Build the one-shot input by downloading every chunk per consented speaker.
    let tracks = collect_speaker_tracks(&api, session).await?;
    if tracks.is_empty() {
        tracing::warn!(%session, "one-shot: no tracks, marking transcribed");
        api.mark_transcribed(session).await?;
        return Ok(());
    }

    let pipeline_cfg = PipelineConfig {
        vad: VadConfig {
            model_path: PathBuf::from(&cfg.vad_model_path),
            ..Default::default()
        },
        whisper: whisper_config(&cfg),
        ..Default::default()
    };

    let mut operators = build_operators(&cfg);
    let result = process_session(
        &pipeline_cfg,
        SessionInput { session_id: session.as_uuid(), tracks },
        &mut operators,
    )
    .await?;

    write_outputs(&api, session, &result).await?;
    api.mark_transcribed(session).await?;

    tracing::info!(
        %session,
        segs = result.segments.len(),
        beats = result.beats.len(),
        scenes = result.scenes.len(),
        elapsed_ms = start.elapsed().as_millis() as u64,
        "session finalized (one-shot)"
    );
    Ok(())
}

/// Clear any prior pipeline outputs for a session (used on admin rerun
/// to guarantee idempotency).
pub async fn clear_prior_outputs(api: &DataApiClient, session: SessionId) -> Result<()> {
    let (segs, beats, scenes) = tokio::join!(
        api.list_segment_ids(session),
        api.list_beat_ids(session),
        api.list_scene_ids(session),
    );
    let segs = segs.unwrap_or_default();
    let beats = beats.unwrap_or_default();
    let scenes = scenes.unwrap_or_default();

    for id in segs { let _ = api.delete_segment(id).await; }
    for id in beats { let _ = api.delete_beat(id).await; }
    for id in scenes { let _ = api.delete_scene(id).await; }
    Ok(())
}

/// Download + decode all consented speakers' chunks into `SpeakerTrack`s.
async fn collect_speaker_tracks(
    api: &DataApiClient,
    session: SessionId,
) -> Result<Vec<SpeakerTrack>> {
    let participants = api.list_participants(session).await?;
    let consented: Vec<_> = participants
        .into_iter()
        .filter_map(|p| {
            let pid = p.user_pseudo_id?;
            let scope = p.consent_scope?;
            (scope == "full").then_some(pid)
        })
        .collect();

    let mut tracks = Vec::with_capacity(consented.len());
    for pid in consented {
        let pseudo = PseudoId::new(pid.clone());
        let mut chunks = api.list_chunks(session, &pseudo).await?;
        if chunks.is_empty() { continue; }
        chunks.sort_by_key(|c| c.seq);

        let mut raw = Vec::new();
        for c in &chunks {
            match api.download_chunk(session, &pseudo, Seq::new(c.seq)).await {
                Ok(b) => raw.extend_from_slice(&b),
                Err(e) => tracing::warn!(%session, pseudo = %pseudo, seq = c.seq, error = %e, "skip chunk"),
            }
        }
        let samples = decode_stereo_to_mono(&raw);
        tracks.push(SpeakerTrack {
            pseudo_id: pid,
            samples,
            sample_rate: CAPTURE_SAMPLE_RATE,
        });
    }
    Ok(tracks)
}

async fn write_outputs(
    api: &DataApiClient,
    session: SessionId,
    result: &PipelineResult,
) -> Result<()> {
    let segs = to_segment_wires(&result.segments);
    let beats = to_beat_wires(&result.beats);
    let scenes = to_scene_wires(&result.scenes);
    post_segments_chunked(api, session, &segs).await?;
    api.post_beats(session, &beats).await?;
    api.post_scenes(session, &scenes).await?;
    Ok(())
}

async fn post_segments_chunked(
    api: &DataApiClient,
    session: SessionId,
    segs: &[SegmentWire],
) -> Result<()> {
    for batch in segs.chunks(5) {
        api.post_segments(session, batch).await?;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Ok(())
}

// --- Conversions ---------------------------------------------------------

fn to_segment_wires(segs: &[chronicle_pipeline::TranscriptSegment]) -> Vec<SegmentWire> {
    segs.iter()
        .map(|s| SegmentWire {
            segment_index: s.segment_index as i32,
            speaker_pseudo_id: s.speaker_pseudo_id.clone(),
            start_time: s.start_time as f64,
            end_time: s.end_time as f64,
            text: s.text.clone(),
            original_text: s.original_text.clone(),
            confidence: s.confidence.map(|c| c as f64),
            beat_id: s.beat_id.map(|b| b as i32),
            chunk_group: s.chunk_group.map(|c| c as i32),
            excluded: s.excluded,
            exclude_reason: s.exclude_reason.clone(),
        })
        .collect()
}

fn to_beat_wires(beats: &[chronicle_pipeline::PipelineBeat]) -> Vec<BeatWire> {
    beats
        .iter()
        .map(|b| BeatWire {
            beat_index: b.beat_index as i32,
            start_time: b.start_time as f64,
            end_time: b.end_time as f64,
            title: b.title.clone(),
            summary: b.summary.clone(),
        })
        .collect()
}

fn to_scene_wires(scenes: &[chronicle_pipeline::PipelineScene]) -> Vec<SceneWire> {
    scenes
        .iter()
        .map(|s| SceneWire {
            scene_index: s.scene_index as i32,
            start_time: s.start_time as f64,
            end_time: s.end_time as f64,
            title: s.title.clone(),
            summary: s.summary.clone(),
            beat_start: s.beat_start as i32,
            beat_end: s.beat_end as i32,
        })
        .collect()
}

// --- Operator chain ------------------------------------------------------

fn build_operators(cfg: &Config) -> Vec<Box<dyn Operator>> {
    match &cfg.scene_llm_url {
        Some(url) => {
            let beat_cfg = BeatConfig {
                endpoint: url.clone(),
                model: cfg.scene_llm_model.clone(),
                gm_speaker_id: cfg.gm_speaker_id.clone(),
                ..Default::default()
            };
            let scene_cfg = SceneConfig {
                endpoint: url.clone(),
                model: cfg.scene_llm_model.clone(),
                gm_speaker_id: cfg.gm_speaker_id.clone(),
                ..Default::default()
            };
            operators_with_llm_scene(beat_cfg, scene_cfg)
        }
        None => default_operators(),
    }
}

