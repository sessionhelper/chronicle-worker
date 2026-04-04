//! Worker: watches for new audio chunks, processes them through the pipeline
//! in real-time, pushes transcript segments to the Data API as they're produced.
//!
//! Two modes:
//! - **Live**: Polls for new chunks during an active recording session.
//!   Processes each chunk as it arrives — transcript builds while recording.
//! - **Batch**: Picks up completed sessions (status=uploaded) and processes
//!   all chunks at once. Fallback for sessions that weren't live-processed.

use std::sync::Arc;
use std::time::Instant;

use ovp_pipeline::{
    default_operators, operators_with_llm_scene, process_session,
    PipelineConfig, SessionInput, SpeakerTrack, TranscriberConfig, VadConfig,
};
use uuid::Uuid;

use crate::api_client::{CreateSegment, DataApiClient};
use crate::config::Config;

#[derive(thiserror::Error, Debug)]
pub enum WorkerError {
    #[error("API error: {0}")]
    Api(#[from] crate::api_client::ApiError),
    #[error("Pipeline error: {0}")]
    Pipeline(#[from] ovp_pipeline::PipelineError),
}

/// Decode raw s16le stereo PCM bytes to mono f32 samples.
fn decode_stereo_to_mono(raw: &[u8]) -> Vec<f32> {
    let all_samples: Vec<f32> = raw
        .chunks_exact(2)
        .map(|pair| {
            let sample = i16::from_le_bytes([pair[0], pair[1]]);
            sample as f32 / i16::MAX as f32
        })
        .collect();
    all_samples
        .chunks_exact(2)
        .map(|frame| (frame[0] + frame[1]) / 2.0)
        .collect()
}

/// Poll for sessions to process — both live (recording) and batch (uploaded).
pub async fn poll_and_process(
    api: &Arc<DataApiClient>,
    config: &Config,
) -> Result<u32, WorkerError> {
    let mut processed = 0;

    // Batch mode: pick up completed sessions
    let uploaded = api.list_sessions_by_status("uploaded").await?;
    for session in &uploaded {
        tracing::info!(session_id = %session.id, "batch processing uploaded session");
        api.update_session_status(session.id, "transcribing").await.ok();

        match process_session_batch(api, config, session.id).await {
            Ok(count) => {
                api.update_session_status(session.id, "transcribed").await.ok();
                tracing::info!(session_id = %session.id, segments = count, "batch transcription complete");
                processed += 1;
            }
            Err(e) => {
                tracing::error!(session_id = %session.id, error = %e, "batch transcription failed");
                api.update_session_status(session.id, "transcription_failed").await.ok();
            }
        }
    }

    // Live mode: check recording sessions for new chunks
    let recording = api.list_sessions_by_status("recording").await?;
    for session in &recording {
        // TODO: Implement live chunk-by-chunk processing.
        // For now, live sessions wait until they're uploaded (batch mode).
        // The architecture supports it — each new chunk gets decoded,
        // fed through RMS → VAD → Whisper, and segments emitted immediately.
        // Requires the pipeline to support incremental input (push-based).
        let _ = session;
    }

    Ok(processed)
}

/// Batch mode: download all audio for a session, run the full pipeline.
async fn process_session_batch(
    api: &Arc<DataApiClient>,
    config: &Config,
    session_id: Uuid,
) -> Result<u32, WorkerError> {
    let start = Instant::now();

    let participants = api.list_participants(session_id).await?;
    let consented: Vec<_> = participants
        .iter()
        .filter(|p| p.consent_scope.as_deref() == Some("full"))
        .collect();

    if consented.is_empty() {
        tracing::warn!(session_id = %session_id, "no consented participants");
        return Ok(0);
    }

    let mut tracks = Vec::new();

    for participant in &consented {
        let pseudo_id = match &participant.pseudo_id {
            Some(id) => id.clone(),
            None => continue,
        };

        let chunks = api.list_chunks(session_id, &pseudo_id).await?;
        if chunks.is_empty() {
            continue;
        }

        // Download all chunks and decode to mono f32
        let mut raw_bytes = Vec::new();
        for chunk in &chunks {
            let data = api.download_chunk(session_id, &pseudo_id, chunk.seq).await?;
            raw_bytes.extend_from_slice(&data);
        }

        let mono = decode_stereo_to_mono(&raw_bytes);
        let duration = mono.len() as f32 / 48000.0;

        tracing::info!(
            speaker = %pseudo_id,
            chunks = chunks.len(),
            duration_secs = format_args!("{:.0}", duration),
            "downloaded speaker audio"
        );

        tracks.push(SpeakerTrack {
            pseudo_id,
            samples: mono,
            sample_rate: 48000,
        });
    }

    if tracks.is_empty() {
        return Ok(0);
    }

    // Pipeline config
    let pipeline_config = PipelineConfig {
        rms: ovp_pipeline::ad::RmsConfig::default(),
        vad: VadConfig {
            model_path: std::path::PathBuf::from(&config.vad_model_path),
            ..VadConfig::default()
        },
        whisper: TranscriberConfig {
            endpoint: config.whisper_url.clone(),
            model: config.whisper_model.clone(),
            language: Some("en".into()),
        },
        min_chunk_duration: 0.8,
    };

    let input = SessionInput {
        session_id,
        tracks,
    };

    let mut operators = build_operators(config);

    let result = process_session(&pipeline_config, input, &mut operators).await?;
    let kept: Vec<_> = result.segments.iter().filter(|s| !s.excluded).collect();

    tracing::info!(
        session_id = %session_id,
        segments = kept.len(),
        excluded = result.segments_excluded,
        scenes = result.scenes_detected,
        pipeline_ms = start.elapsed().as_millis(),
        "pipeline complete"
    );

    // Upload segments
    let api_segments: Vec<CreateSegment> = kept
        .iter()
        .map(|s| CreateSegment {
            segment_index: s.segment_index as i32,
            speaker_pseudo_id: s.speaker_pseudo_id.clone(),
            start_time: s.start_time as f64,
            end_time: s.end_time as f64,
            text: s.text.clone(),
            original_text: s.original_text.clone(),
            confidence: s.confidence.map(|c| c as f64),
        })
        .collect();

    api.create_segments(session_id, &api_segments).await?;

    Ok(api_segments.len() as u32)
}

fn build_operators(config: &Config) -> Vec<Box<dyn ovp_pipeline::Operator>> {
    if let Some(ref llm_url) = config.scene_llm_url {
        let beat_config = ovp_pipeline::operators::beat::BeatConfig {
            endpoint: llm_url.clone(),
            model: config.scene_llm_model.clone(),
            gm_speaker_id: config.gm_speaker_id.clone(),
            ..Default::default()
        };
        let scene_config = ovp_pipeline::operators::scene::SceneConfig {
            endpoint: llm_url.clone(),
            model: config.scene_llm_model.clone(),
            gm_speaker_id: config.gm_speaker_id.clone(),
            ..Default::default()
        };
        operators_with_llm_scene(beat_config, scene_config)
    } else {
        default_operators()
    }
}
