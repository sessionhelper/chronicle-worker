//! Admin `/rerun` endpoint clears prior outputs (DELETEs segments,
//! beats, scenes) before re-running. This test exercises
//! `session_runner::clear_prior_outputs` directly — the HTTP surface
//! is tested separately in `admin_disabled.rs`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chronicle_worker::api_client::DataApiClient;
use chronicle_worker::ids::SessionId;
use chronicle_worker::session_runner;
use serde_json::json;
use uuid::Uuid;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn clear_prior_outputs_deletes_segments_beats_scenes() {
    let server = MockServer::start().await;
    let session = Uuid::new_v4();

    // Auth.
    Mock::given(method("POST"))
        .and(path("/internal/auth"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "session_token": "tok" })),
        )
        .mount(&server)
        .await;

    let seg1 = Uuid::new_v4();
    let seg2 = Uuid::new_v4();
    let beat1 = Uuid::new_v4();
    let scene1 = Uuid::new_v4();

    // GET lists.
    Mock::given(method("GET"))
        .and(path(format!("/internal/sessions/{session}/segments")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "id": seg1 },
            { "id": seg2 }
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/internal/sessions/{session}/beats")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "id": beat1 }
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/internal/sessions/{session}/scenes")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "id": scene1 }
        ])))
        .mount(&server)
        .await;

    // Count DELETEs across the three resource types.
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    Mock::given(method("DELETE"))
        .and(path_regex(r"^/internal/(segments|beats|scenes)/[0-9a-f-]+$"))
        .respond_with(move |_req: &wiremock::Request| {
            c.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(204)
        })
        .mount(&server)
        .await;

    let api: Arc<DataApiClient> = DataApiClient::authenticate(server.uri(), "x", "test")
        .await
        .expect("auth");

    session_runner::clear_prior_outputs(&api, SessionId(session))
        .await
        .expect("clear prior outputs");

    // 2 segments + 1 beat + 1 scene = 4 deletes.
    assert_eq!(counter.load(Ordering::SeqCst), 4);
}
