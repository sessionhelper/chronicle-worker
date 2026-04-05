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

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
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
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct DataApiClient {
    client: Client,
    base_url: String,
    session_token: String,
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
        tracing::info!(service = service_name, "authenticated with Data API");

        Ok(Self {
            client,
            base_url: base_url.to_string(),
            session_token: auth.session_token,
        })
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.session_token)
    }

    /// 30s heartbeat. Server reaps sessions inactive >90s.
    pub async fn heartbeat(&self) -> Result<()> {
        let resp = self
            .client
            .post(format!("{}/internal/heartbeat", self.base_url))
            .header("authorization", self.auth_header())
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
            .header("authorization", self.auth_header())
            .send()
            .await?;
        Ok(check_status(resp).await?.json().await?)
    }

    /// Fetch one session's full detail row.
    pub async fn get_session(&self, session_id: Uuid) -> Result<SessionDetail> {
        let resp = self
            .client
            .get(format!("{}/internal/sessions/{session_id}", self.base_url))
            .header("authorization", self.auth_header())
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
            .header("authorization", self.auth_header())
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
            .header("authorization", self.auth_header())
            .send()
            .await?;
        Ok(check_status(resp).await?.json().await?)
    }

    /// Download one raw-PCM audio chunk by sequence number.
    ///
    /// The Data API streams these back as s16le stereo 48kHz bytes with
    /// `Content-Type: audio/pcm`. The worker handles decoding; see
    /// `crate::decode::decode_stereo_to_mono`.
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
            .header("authorization", self.auth_header())
            .send()
            .await?;
        Ok(check_status(resp).await?.bytes().await?.to_vec())
    }

    // ----- Segment upload ----------------------------------------------

    /// Bulk-post transcript segments for a session.
    ///
    /// The Data API route (`bulk_create_segments`) deserializes a bare
    /// JSON array of `CreateSegment`, so we serialize `Vec<Segment>`
    /// directly — no envelope.
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
            .header("authorization", self.auth_header())
            .json(&segments)
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
            .header("authorization", self.auth_header())
            .json(&serde_json::json!({ "status": status }))
            .send()
            .await?;
        check_status(resp).await?;
        Ok(())
    }
}
