//! End-to-end integration test for `worker::process_next_session`.
//!
//! Stands up a `wiremock` server that impersonates the Data API, wires a
//! stub `PipelineRunner` that returns canned segments, and asserts the
//! worker walks through one full session:
//!
//!   auth -> list uploaded -> mark transcribing -> list participants ->
//!   list chunks (per speaker) -> download chunks -> (stub pipeline) ->
//!   post segments -> mark transcribed
//!
//! Pipeline is stubbed because spinning up Whisper + Silero VAD in a
//! unit test is out of scope for A1. The stub runner receives the real
//! `SessionInput` the worker constructs so we can sanity-check the
//! decoded sample count and speaker set.

use std::sync::{Arc, Mutex};

use chronicle_pipeline::{PipelineConfig, PipelineResult, SessionInput, TranscriptSegment};
use chronicle_worker::api_client::DataApiClient;
use chronicle_worker::config::Config;
use chronicle_worker::state::AppState;
use chronicle_worker::worker::{process_next_session, PipelineRunner};
use serde_json::json;
use uuid::Uuid;
use wiremock::matchers::{method, path, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Records every `SessionInput` the worker hands to the pipeline. One
/// shot per test is enough — we're checking the happy path, not looping.
struct CapturingStubRunner {
    captured: Arc<Mutex<Option<SessionInput>>>,
    canned: TranscriptSegment,
}

impl PipelineRunner for CapturingStubRunner {
    fn run<'a>(
        &'a self,
        _config: &'a PipelineConfig,
        input: SessionInput,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = chronicle_pipeline::Result<PipelineResult>> + Send + 'a,
        >,
    > {
        let captured = self.captured.clone();
        let canned = self.canned.clone();
        Box::pin(async move {
            *captured.lock().unwrap() = Some(input);
            Ok(PipelineResult {
                segments: vec![canned],
                beats: Vec::new(),
                scenes: Vec::new(),
                segments_produced: 1,
                segments_excluded: 0,
                scenes_detected: 1,
                duration_processed: 1.0,
            })
        })
    }
}

fn test_config(base_url: String) -> Config {
    // Config is clap-derived; construct a minimal instance by parsing a
    // fake argv. The only fields the worker actually reads in this test
    // are the pipeline knobs (passed straight to the stub runner's
    // `PipelineConfig`, which the stub ignores) and `poll_interval_secs`
    // which we don't exercise because we call `process_next_session`
    // directly.
    Config {
        data_api_url: base_url,
        shared_secret: "test-secret".into(),
        poll_interval_secs: 10,
        log_level: "info".into(),
        whisper_url: "http://stub/whisper".into(),
        whisper_model: "stub".into(),
        vad_model_path: "models/silero_vad_v6.onnx".into(),
        scene_llm_url: None,
        scene_llm_model: "stub".into(),
        gm_speaker_id: None,
    }
}

#[tokio::test]
async fn happy_path_end_to_end() {
    let server = MockServer::start().await;
    let session_id = Uuid::new_v4();
    let pseudo_id = "aabbccdd";

    // ---- Auth ----
    Mock::given(method("POST"))
        .and(path("/internal/auth"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "session_token": "test-token" })),
        )
        .mount(&server)
        .await;

    // ---- List uploaded sessions ----
    Mock::given(method("GET"))
        .and(path("/internal/sessions"))
        .and(query_param("status", "uploaded"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "id": session_id, "status": "uploaded" }
        ])))
        .mount(&server)
        .await;

    // ---- Update status (transcribing and transcribed both go through
    // PATCH /internal/sessions/{id}) ----
    Mock::given(method("PATCH"))
        .and(path(format!("/internal/sessions/{session_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": session_id,
            "guild_id": 1,
            "started_at": "2025-01-01T00:00:00Z",
            "ended_at": null,
            "game_system": null,
            "campaign_name": null,
            "participant_count": 1,
            "s3_prefix": "sessions/x",
            "status": "transcribing",
            "collaborative_editing": false,
            "created_at": "2025-01-01T00:00:00Z"
        })))
        .mount(&server)
        .await;

    // ---- List participants ----
    let participant_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    Mock::given(method("GET"))
        .and(path(format!("/internal/sessions/{session_id}/participants")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {
                "id": participant_id,
                "session_id": session_id,
                "user_id": user_id,
                "user_pseudo_id": pseudo_id,
                "consent_scope": "full",
                "consented_at": null,
                "withdrawn_at": null,
                "mid_session_join": false,
                "no_llm_training": false,
                "no_public_release": false
            },
            // A declined participant to exercise the filter.
            {
                "id": Uuid::new_v4(),
                "session_id": session_id,
                "user_id": Uuid::new_v4(),
                "user_pseudo_id": "deadbeef",
                "consent_scope": "decline",
                "consented_at": null,
                "withdrawn_at": null,
                "mid_session_join": false,
                "no_llm_training": false,
                "no_public_release": false
            }
        ])))
        .mount(&server)
        .await;

    // ---- List chunks ----
    Mock::given(method("GET"))
        .and(path(format!(
            "/internal/sessions/{session_id}/audio/{pseudo_id}/chunks"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "key": "k0", "seq": 0, "size": 8 },
            { "key": "k1", "seq": 1, "size": 8 }
        ])))
        .mount(&server)
        .await;

    // ---- Download chunk (both seqs). Each chunk is 8 bytes = 2 stereo
    // frames = 2 mono samples after decode. Two chunks = 4 samples total. ----
    let chunk_bytes: Vec<u8> = vec![
        0x00, 0x40, 0x00, 0x40, // frame 1: L=0x4000, R=0x4000 -> 0.5
        0x00, 0xC0, 0x00, 0xC0, // frame 2: L=0xC000, R=0xC000 -> -0.5
    ];
    Mock::given(method("GET"))
        .and(path_regex(format!(
            r"^/internal/sessions/{session_id}/audio/{pseudo_id}/chunk/\d+$"
        )))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "audio/pcm")
                .set_body_bytes(chunk_bytes),
        )
        .mount(&server)
        .await;

    // ---- Post segments ----
    Mock::given(method("POST"))
        .and(path(format!("/internal/sessions/{session_id}/segments")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&server)
        .await;

    // ---- Heartbeat (not exercised here but the client won't call it
    // during process_next_session) ----

    // Build the real client against the mock server.
    let api = DataApiClient::authenticate(&server.uri(), "test-secret", "chronicle-worker-test")
        .await
        .expect("auth");
    let state = AppState::new(Arc::new(api), test_config(server.uri()));

    // Wire the capturing stub runner.
    let captured = Arc::new(Mutex::new(None));
    let runner = CapturingStubRunner {
        captured: captured.clone(),
        canned: TranscriptSegment {
            id: Uuid::new_v4(),
            session_id,
            segment_index: 0,
            speaker_pseudo_id: pseudo_id.to_string(),
            start_time: 0.0,
            end_time: 1.0,
            text: "hello world".into(),
            original_text: "hello world".into(),
            confidence: Some(0.9),
            beat_id: None,
            chunk_group: Some(0),
            excluded: false,
            exclude_reason: None,
        },
    };

    let result = process_next_session(&state, &runner)
        .await
        .expect("process succeeds");
    assert_eq!(result, Some(session_id));

    // Assert the stub saw exactly one track with the expected speaker
    // and sample count. Two chunks * 2 stereo frames = 4 mono samples.
    let captured = captured.lock().unwrap();
    let input = captured.as_ref().expect("pipeline was invoked");
    assert_eq!(input.session_id, session_id);
    assert_eq!(input.tracks.len(), 1);
    assert_eq!(input.tracks[0].pseudo_id, pseudo_id);
    assert_eq!(input.tracks[0].sample_rate, 48_000);
    assert_eq!(input.tracks[0].samples.len(), 4);
}
