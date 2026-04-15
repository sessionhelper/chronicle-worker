//! Orphan recovery: on startup, the worker lists
//! `GET /internal/sessions?status=transcribing` and re-claims + reruns
//! each one. The claim is idempotent (`transcribing -> transcribing` is
//! a valid no-op transition per spec).
//!
//! We drive this through the lower-level api_client surface so we can
//! assert the HTTP ordering without standing up a live pipeline.

use std::sync::Arc;

use chronicle_worker::api_client::DataApiClient;
use chronicle_worker::ids::SessionId;
use serde_json::json;
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn startup_lists_transcribing_and_reclaims() {
    let server = MockServer::start().await;
    let orphan = Uuid::new_v4();

    // Auth
    Mock::given(method("POST"))
        .and(path("/internal/auth"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "session_token": "tok" })),
        )
        .mount(&server)
        .await;

    // GET /internal/sessions?status=transcribing -> returns the orphan
    Mock::given(method("GET"))
        .and(path("/internal/sessions"))
        .and(query_param("status", "transcribing"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "id": orphan, "status": "transcribing" }
        ])))
        .mount(&server)
        .await;

    // PATCH transcribing -> 200 (the data-api accepts the self-transition
    // per spec §"Orphan claim relies on transcribing -> transcribing
    // being a valid no-op transition")
    Mock::given(method("PATCH"))
        .and(path(format!("/internal/sessions/{orphan}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
        .expect(1..) // at least once
        .mount(&server)
        .await;

    let api: Arc<DataApiClient> = DataApiClient::authenticate(server.uri(), "x", "test")
        .await
        .expect("auth");

    // Reproduce the startup step the event loop performs first:
    //   1) list transcribing  2) PATCH each back to transcribing.
    let orphans = api
        .list_sessions_by_status("transcribing")
        .await
        .expect("list");
    assert_eq!(orphans.len(), 1);
    assert_eq!(orphans[0].id, orphan);

    api.claim_session(SessionId(orphan))
        .await
        .expect("idempotent reclaim");

    // Wiremock verifies the expectations on drop.
}
