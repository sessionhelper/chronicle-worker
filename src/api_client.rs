//! HTTP client for `ovp-data-api`.
//!
//! Mirrors the idioms in `ttrpg-collector/voice-capture/src/api_client.rs`
//! (shared-secret auth → session token → Bearer on every request, 30s
//! heartbeat, `check_status` helper). Code is not shared across crates
//! — each service owns its own slimmed-down copy with just the methods
//! it needs.
//!
//! Auth protocol: see `sessionhelper-hub/CLAUDE.md` → "Shared-secret
//! service auth". One `POST /internal/auth` at startup, then a token
//! the server reaps after 90s of heartbeat silence.

use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API returned {status}: {body}")]
    Status { status: u16, body: String },
    #[error("Auth failed: {0}")]
    Auth(String),
}

pub type Result<T> = std::result::Result<T, ApiError>;

/// Turn a non-2xx response into an `ApiError::Status`, otherwise hand
/// the response back unchanged so the caller can chain `.json()`/`.bytes()`.
async fn check_status(resp: reqwest::Response) -> Result<reqwest::Response> {
    if resp.status().is_success() {
        Ok(resp)
    } else {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Err(ApiError::Status { status, body })
    }
}

// ---------------------------------------------------------------------------
// Response / request types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct AuthResponse {
    session_token: String,
}

#[derive(Serialize)]
struct AuthRequest<'a> {
    shared_secret: &'a str,
    service_name: &'a str,
}

/// Minimal session row returned by `list_uploaded_sessions`.
///
/// The Data API's `db::Session` row has ~10 fields; the worker only
/// cares about `id` and `status`. If more fields become needed, grow
/// this struct rather than pulling in the whole DB row type.
#[derive(Deserialize, Debug, Clone)]
pub struct SessionSummary {
    pub id: Uuid,
    pub status: String,
}

/// Full session detail for a single session fetch.
///
/// TODO: expand as `process_next_session` grows — it will probably
/// want `started_at`, `ended_at`, `guild_id`, `s3_prefix`. For now
/// keep it narrow so the scaffolding compiles.
#[derive(Deserialize, Debug, Clone)]
pub struct SessionDetail {
    pub id: Uuid,
    pub status: String,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub ended_at: Option<DateTime<Utc>>,
}

/// A session participant as returned by `GET /internal/sessions/{id}/participants`.
///
/// The Data API joins `users.pseudo_id` onto the participant row so the
/// worker has everything it needs to address audio chunks in S3 without
/// a second round trip per participant. `user_pseudo_id` is `None` for
/// participant rows that were never linked to a concrete user (shouldn't
/// happen in production — the collector always upserts a user first —
/// but the JOIN is LEFT so the field is still Optional on the wire).
///
/// `consent_scope` is the collector's current wire-level consent field
/// (today: `"full"` or `"decline"`). License flags (`no_llm_training`,
/// `no_public_release`) gate *downstream publication*, not transcription,
/// so the worker does not read them. See `worker::process_next_session`
/// for the filter rule.
#[derive(Deserialize, Debug, Clone)]
pub struct Participant {
    pub id: Uuid,
    pub user_id: Option<Uuid>,
    #[serde(default)]
    pub user_pseudo_id: Option<String>,
    #[serde(default)]
    pub consent_scope: Option<String>,
}

/// Metadata about a single audio chunk, as returned by the
/// `list_chunks` endpoint. Only `seq` is load-bearing for the worker —
/// `size` is kept for diagnostics and metrics, `key` for log correlation.
#[derive(Deserialize, Debug, Clone)]
pub struct ChunkInfo {
    #[allow(dead_code)]
    pub key: String,
    pub seq: u32,
    #[allow(dead_code)]
    pub size: i64,
}

/// Transcript segment posted back to the Data API after pipeline processing.
///
/// Wire format matches `ovp-data-api/src/routes/segments.rs` ->
/// `bulk_create_segments` (a bare JSON array of `CreateSegment`). Field
/// types are `i32`/`f64` to match the DB column types — not `usize`/`f32`
/// which would force conversions on both sides.
#[derive(Serialize, Debug, Clone)]
pub struct Segment {
    pub segment_index: i32,
    pub speaker_pseudo_id: String,
    pub start_time: f64,
    pub end_time: f64,
    pub text: String,
    pub original_text: String,
    pub confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub beat_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_group: Option<i32>,
    #[serde(default)]
    pub excluded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_reason: Option<String>,
}

/// Narrative beat posted to the Data API after pipeline processing.
#[derive(Serialize, Debug, Clone)]
pub struct Beat {
    pub beat_index: i32,
    pub start_time: f64,
    pub end_time: f64,
    pub title: String,
    pub summary: String,
}

/// Scene grouping posted to the Data API after pipeline processing.
#[derive(Serialize, Debug, Clone)]
pub struct Scene {
    pub scene_index: i32,
    pub start_time: f64,
    pub end_time: f64,
    pub title: String,
    pub summary: String,
    pub beat_start: i32,
    pub beat_end: i32,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct DataApiClient {
    client: Client,
    base_url: String,
    session_token: RwLock<String>,
    /// Stored for re-authentication on 401.
    shared_secret: String,
    /// Stored for re-authentication on 401.
    service_name: String,
}

impl DataApiClient {
    /// The bearer token for this authenticated session. Needed by the WS
    /// client which passes it as a query parameter on the upgrade request.
    pub async fn session_token(&self) -> String {
        self.session_token.read().await.clone()
    }

    /// Build the WebSocket URL for the data-api event bus, including the
    /// auth token as a query parameter. Converts `http(s)://` to `ws(s)://`.
    pub async fn ws_url(&self) -> String {
        let token = self.session_token.read().await;
        let ws_base = if self.base_url.starts_with("https") {
            self.base_url.replacen("https", "wss", 1)
        } else {
            self.base_url.replacen("http", "ws", 1)
        };
        format!("{ws_base}/ws?token={}", *token)
    }
}

impl DataApiClient {
    /// Authenticate with the Data API via shared secret and return a
    /// client ready to make authenticated requests.
    pub async fn authenticate(
        base_url: &str,
        shared_secret: &str,
        service_name: &str,
    ) -> Result<Self> {
        let client = Client::new();
        let token = Self::do_auth(&client, base_url, shared_secret, service_name).await?;
        tracing::info!(service = service_name, "authenticated with Data API");

        Ok(Self {
            client,
            base_url: base_url.to_string(),
            session_token: RwLock::new(token),
            shared_secret: shared_secret.to_string(),
            service_name: service_name.to_string(),
        })
    }

    /// Perform the auth handshake and return the session token.
    async fn do_auth(
        client: &Client,
        base_url: &str,
        shared_secret: &str,
        service_name: &str,
    ) -> Result<String> {
        let resp = client
            .post(format!("{base_url}/internal/auth"))
            .json(&AuthRequest { shared_secret, service_name })
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ApiError::Auth(format!("auth failed ({status}): {body}")));
        }

        let auth: AuthResponse = resp.json().await?;
        Ok(auth.session_token)
    }

    /// Re-authenticate with the Data API, replacing the stored session
    /// token. Called when a request returns 401 (token expired).
    pub async fn re_authenticate(&self) -> Result<()> {
        let token =
            Self::do_auth(&self.client, &self.base_url, &self.shared_secret, &self.service_name)
                .await?;
        *self.session_token.write().await = token;
        tracing::info!(service = %self.service_name, "re-authenticated with Data API");
        Ok(())
    }

    async fn auth_header(&self) -> String {
        format!("Bearer {}", self.session_token.read().await)
    }

    /// 30s heartbeat. Server reaps sessions inactive >90s.
    pub async fn heartbeat(&self) -> Result<()> {
        let resp = self
            .client
            .post(format!("{}/internal/heartbeat", self.base_url))
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        check_status(resp).await?;
        Ok(())
    }

    // ----- Session discovery -------------------------------------------

    /// List sessions in the `uploaded` state, oldest first.
    pub async fn list_uploaded_sessions(&self) -> Result<Vec<SessionSummary>> {
        let resp = self
            .client
            .get(format!("{}/internal/sessions?status=uploaded", self.base_url))
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        Ok(check_status(resp).await?.json().await?)
    }

    /// Fetch one session's full detail row.
    pub async fn get_session(&self, session_id: Uuid) -> Result<SessionDetail> {
        let resp = self
            .client
            .get(format!("{}/internal/sessions/{session_id}", self.base_url))
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        Ok(check_status(resp).await?.json().await?)
    }

    /// List all participants for a session. The API returns everyone,
    /// including withdrawn and declined rows — the worker is responsible
    /// for filtering to consented participants before downloading audio.
    pub async fn list_participants(&self, session_id: Uuid) -> Result<Vec<Participant>> {
        let resp = self
            .client
            .get(format!(
                "{}/internal/sessions/{session_id}/participants",
                self.base_url
            ))
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        Ok(check_status(resp).await?.json().await?)
    }

    // ----- Audio chunk download ----------------------------------------

    /// Enumerate all audio chunks for one speaker in one session.
    ///
    /// Returned chunks are sorted oldest-first by `seq` (the Data API
    /// guarantees this in `storage::audio::list_chunks`). Callers must
    /// preserve that order when concatenating raw bytes — PCM is not
    /// self-synchronizing, so a reorder would corrupt timestamps.
    pub async fn list_chunks(
        &self,
        session_id: Uuid,
        pseudo_id: &str,
    ) -> Result<Vec<ChunkInfo>> {
        let resp = self
            .client
            .get(format!(
                "{}/internal/sessions/{session_id}/audio/{pseudo_id}/chunks",
                self.base_url
            ))
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        Ok(check_status(resp).await?.json().await?)
    }

    /// Download one raw-PCM audio chunk by sequence number (no retry).
    ///
    /// The Data API streams these back as s16le stereo 48kHz bytes with
    /// `Content-Type: audio/pcm`. The worker handles decoding; see
    /// `crate::decode::decode_stereo_to_mono`.
    ///
    /// Prefer [`download_chunk_with_retry`] in production code paths —
    /// this bare version exists for internal use and tests.
    pub async fn download_chunk(
        &self,
        session_id: Uuid,
        pseudo_id: &str,
        chunk_seq: u32,
    ) -> Result<Vec<u8>> {
        let resp = self
            .client
            .get(format!(
                "{}/internal/sessions/{session_id}/audio/{pseudo_id}/chunk/{chunk_seq}",
                self.base_url
            ))
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        Ok(check_status(resp).await?.bytes().await?.to_vec())
    }

    /// Download one audio chunk with retry on transient errors.
    ///
    /// Retry policy:
    /// - **5xx** (server error): retry up to 3 times with 1s, 2s, 4s backoff.
    /// - **401** (unauthorized): re-authenticate once and retry. If re-auth
    ///   itself fails, propagate the error.
    /// - **404**: returned as-is (signals end-of-chunks to some callers).
    /// - **4xx** (other client errors): fail immediately, no retry.
    pub async fn download_chunk_with_retry(
        &self,
        session_id: Uuid,
        pseudo_id: &str,
        chunk_seq: u32,
    ) -> Result<Vec<u8>> {
        const MAX_RETRIES: u32 = 3;
        let backoff_durations = [
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(4),
        ];

        let mut attempt = 0u32;
        loop {
            let url = format!(
                "{}/internal/sessions/{session_id}/audio/{pseudo_id}/chunk/{chunk_seq}",
                self.base_url
            );
            let resp = self
                .client
                .get(&url)
                .header("authorization", self.auth_header().await)
                .send()
                .await?;

            let status = resp.status().as_u16();

            if resp.status().is_success() {
                return Ok(resp.bytes().await?.to_vec());
            }

            // 401 — token expired, re-auth once and retry immediately.
            if status == 401 {
                tracing::warn!(
                    session_id = %session_id,
                    pseudo_id,
                    chunk_seq,
                    "chunk download got 401, re-authenticating"
                );
                self.re_authenticate().await?;
                // Retry once after re-auth; if it fails again we fall
                // through to the normal retry/error path below.
                let retry_resp = self
                    .client
                    .get(&url)
                    .header("authorization", self.auth_header().await)
                    .send()
                    .await?;
                return if retry_resp.status().is_success() {
                    Ok(retry_resp.bytes().await?.to_vec())
                } else {
                    let s = retry_resp.status().as_u16();
                    let body = retry_resp.text().await.unwrap_or_default();
                    Err(ApiError::Status { status: s, body })
                };
            }

            // 5xx — transient server error, retry with backoff.
            if status >= 500 {
                attempt += 1;
                if attempt <= MAX_RETRIES {
                    let delay = backoff_durations[(attempt - 1) as usize];
                    tracing::warn!(
                        session_id = %session_id,
                        pseudo_id,
                        chunk_seq,
                        status,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        "chunk download failed (5xx), retrying"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                // Exhausted retries.
                let body = resp.text().await.unwrap_or_default();
                return Err(ApiError::Status { status, body });
            }

            // 4xx (not 401) — client error, fail immediately.
            let body = resp.text().await.unwrap_or_default();
            return Err(ApiError::Status { status, body });
        }
    }

    // ----- Segment upload ----------------------------------------------

    /// Bulk-post transcript segments for a session.
    pub async fn post_segments(
        &self,
        session_id: Uuid,
        segments: Vec<Segment>,
    ) -> Result<()> {
        let resp = self
            .client
            .post(format!(
                "{}/internal/sessions/{session_id}/segments",
                self.base_url
            ))
            .header("authorization", self.auth_header().await)
            .json(&segments)
            .send()
            .await?;
        check_status(resp).await?;
        Ok(())
    }

    /// Bulk-post narrative beats for a session.
    pub async fn post_beats(
        &self,
        session_id: Uuid,
        beats: Vec<Beat>,
    ) -> Result<()> {
        let resp = self
            .client
            .post(format!(
                "{}/internal/sessions/{session_id}/beats",
                self.base_url
            ))
            .header("authorization", self.auth_header().await)
            .json(&beats)
            .send()
            .await?;
        check_status(resp).await?;
        Ok(())
    }

    /// Bulk-post scene groupings for a session.
    pub async fn post_scenes(
        &self,
        session_id: Uuid,
        scenes: Vec<Scene>,
    ) -> Result<()> {
        let resp = self
            .client
            .post(format!(
                "{}/internal/sessions/{session_id}/scenes",
                self.base_url
            ))
            .header("authorization", self.auth_header().await)
            .json(&scenes)
            .send()
            .await?;
        check_status(resp).await?;
        Ok(())
    }

    // ----- Session state update ----------------------------------------

    /// Transition a session to a new status (`transcribing`, `transcribed`,
    /// `transcription_failed`, ...). Thin wrapper over `PATCH
    /// /internal/sessions/{id}` with a `{"status": "..."}` body.
    pub async fn update_session_state(
        &self,
        session_id: Uuid,
        status: &str,
    ) -> Result<()> {
        let resp = self
            .client
            .patch(format!("{}/internal/sessions/{session_id}", self.base_url))
            .header("authorization", self.auth_header().await)
            .json(&serde_json::json!({ "status": status }))
            .send()
            .await?;
        check_status(resp).await?;
        Ok(())
    }
}
