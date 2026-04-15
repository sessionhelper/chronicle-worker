//! Claim race: second worker to PATCH `transcribing` sees 409 and
//! surfaces `WorkerError::ClaimLost` — the worker skips the session
//! without retrying.

use chronicle_worker::api_client::DataApiClient;
use chronicle_worker::error::WorkerError;
use chronicle_worker::ids::SessionId;
use serde_json::json;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn claim_returns_claim_lost_on_409() {
    let server = MockServer::start().await;
    let session = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/internal/auth"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "session_token": "tok" })),
        )
        .mount(&server)
        .await;

    Mock::given(method("PATCH"))
        .and(path(format!("/internal/sessions/{session}")))
        .respond_with(
            ResponseTemplate::new(409).set_body_json(json!({
                "error": "invalid transition: transcribing already claimed"
            })),
        )
        .mount(&server)
        .await;

    let api = DataApiClient::authenticate(server.uri(), "secret", "test")
        .await
        .expect("auth");
    let result = api.claim_session(SessionId(session)).await;

    match result {
        Err(WorkerError::ClaimLost(id)) => assert_eq!(id.as_uuid(), session),
        other => panic!("expected ClaimLost, got {other:?}"),
    }
}

#[tokio::test]
async fn claim_succeeds_on_200() {
    let server = MockServer::start().await;
    let session = Uuid::new_v4();

    Mock::given(method("POST"))
        .and(path("/internal/auth"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "session_token": "tok" })),
        )
        .mount(&server)
        .await;

    Mock::given(method("PATCH"))
        .and(path(format!("/internal/sessions/{session}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
        .mount(&server)
        .await;

    let api = DataApiClient::authenticate(server.uri(), "secret", "test")
        .await
        .expect("auth");
    api.claim_session(SessionId(session)).await.expect("claim wins");
}
