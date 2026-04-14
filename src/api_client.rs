//! HTTP client for `chronicle-data-api`.
//!
//! One client instance per worker, cheap to clone via `Arc`. Owns the
//! bearer token with interior mutability so the heartbeat / re-auth
//! paths don't poison request sites.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::error::{Result, WorkerError};
use crate::ids::{PseudoId, Seq, SessionId};

// --- Wire types ----------------------------------------------------------

#[derive(Deserialize)]
struct AuthResponse {
    session_token: String,
}

#[derive(Serialize)]
struct AuthRequest<'a> {
    shared_secret: &'a str,
    service_name: &'a str,
}

/// Narrow session row the worker cares about.
#[derive(Deserialize, Debug, Clone)]
pub struct SessionSummary {
    pub id: Uuid,
    pub status: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SessionDetail {
    pub id: Uuid,
    pub status: String,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Participant {
    pub id: Uuid,
    #[serde(default)]
    pub user_pseudo_id: Option<String>,
    #[serde(default)]
    pub consent_scope: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ChunkInfo {
    #[allow(dead_code)]
    pub key: String,
    pub seq: u32,
    #[allow(dead_code)]
    #[serde(default)]
    pub size: i64,
}

/// Bulk-insert row for segments/beats/scenes. Data-api's
/// `uniform::CreateInput`: client_id-keyed, dedup'd on `(session_id,
/// client_id)`. Segments set `text`; beats/scenes set `title` + `summary`.
#[derive(Serialize, Debug, Clone)]
pub struct CreateInputWire {
    pub client_id: String,
    pub start_ms: i64,
    pub end_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pseudo_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flags: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original: Option<serde_json::Value>,
}

/// Per-session IDs returned by list-segments (used for DELETE on rerun).
#[derive(Deserialize, Debug, Clone)]
pub struct ResourceRow {
    pub id: Uuid,
}

// --- Client --------------------------------------------------------------

pub struct DataApiClient {
    http: Client,
    base_url: String,
    session_token: RwLock<String>,
    shared_secret: String,
    service_name: String,
}

impl DataApiClient {
    /// Construct an authenticated client. Performs the `POST /internal/auth`
    /// handshake and stores the session token.
    pub async fn authenticate(
        base_url: impl Into<String>,
        shared_secret: impl Into<String>,
        service_name: impl Into<String>,
    ) -> Result<Arc<Self>> {
        let base_url = base_url.into();
        let shared_secret = shared_secret.into();
        let service_name = service_name.into();

        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| WorkerError::Api(format!("reqwest build: {e}")))?;

        let token = Self::do_auth(&http, &base_url, &shared_secret, &service_name).await?;
        tracing::info!(service = %service_name, "authenticated with data-api");

        Ok(Arc::new(Self {
            http,
            base_url,
            session_token: RwLock::new(token),
            shared_secret,
            service_name,
        }))
    }

    async fn do_auth(
        http: &Client,
        base_url: &str,
        shared_secret: &str,
        service_name: &str,
    ) -> Result<String> {
        let resp = http
            .post(format!("{base_url}/internal/auth"))
            .json(&AuthRequest { shared_secret, service_name })
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(WorkerError::Api(format!("auth {status}: {body}")));
        }
        let a: AuthResponse = resp.json().await?;
        Ok(a.session_token)
    }

    /// Re-auth and update the stored token.
    pub async fn re_authenticate(&self) -> Result<()> {
        let token =
            Self::do_auth(&self.http, &self.base_url, &self.shared_secret, &self.service_name)
                .await?;
        *self.session_token.write().await = token;
        tracing::info!(service = %self.service_name, "re-authenticated");
        Ok(())
    }

    async fn token(&self) -> String { self.session_token.read().await.clone() }

    async fn auth_header(&self) -> String { format!("Bearer {}", self.token().await) }

    /// Build the WebSocket URL with the bearer token embedded as a query
    /// param. Returns `ws://` / `wss://` depending on the base URL scheme.
    pub async fn ws_url(&self) -> String {
        let token = self.token().await;
        let ws_base = if self.base_url.starts_with("https") {
            self.base_url.replacen("https", "wss", 1)
        } else {
            self.base_url.replacen("http", "ws", 1)
        };
        format!("{ws_base}/ws?token={token}")
    }

    /// Unauthenticated base URL accessor (used by admin status).
    pub fn base_url(&self) -> &str { &self.base_url }

    // ----- Heartbeat -----------------------------------------------------

    pub async fn heartbeat(&self) -> Result<()> {
        let resp = self
            .http
            .post(format!("{}/internal/heartbeat", self.base_url))
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        check_ok(resp).await?;
        Ok(())
    }

    // ----- Session state -------------------------------------------------

    pub async fn list_sessions_by_status(&self, status: &str) -> Result<Vec<SessionSummary>> {
        let url = format!("{}/internal/sessions?status={status}", self.base_url);
        let resp = self
            .http
            .get(url)
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        let resp = check_ok(resp).await?;
        Ok(resp.json().await?)
    }

    pub async fn get_session(&self, id: SessionId) -> Result<SessionDetail> {
        let url = format!("{}/internal/sessions/{}", self.base_url, id);
        let resp = self
            .http
            .get(url)
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Err(WorkerError::NotFound(id));
        }
        let resp = check_ok(resp).await?;
        Ok(resp.json().await?)
    }

    /// Atomic claim: PATCH status. A 409 surfaces as `ClaimLost` so the
    /// caller can skip cleanly without inspecting error strings.
    pub async fn claim_session(&self, id: SessionId) -> Result<()> {
        self.patch_status(id, "transcribing").await
    }

    pub async fn mark_transcribed(&self, id: SessionId) -> Result<()> {
        self.patch_status(id, "transcribed").await
    }

    pub async fn mark_failed(&self, id: SessionId) -> Result<()> {
        self.patch_status(id, "transcribing_failed").await
    }

    pub async fn patch_status(&self, id: SessionId, status: &str) -> Result<()> {
        let url = format!("{}/internal/sessions/{}", self.base_url, id);
        let resp = self
            .http
            .patch(url)
            .header("authorization", self.auth_header().await)
            .json(&serde_json::json!({ "status": status }))
            .send()
            .await?;
        let code = resp.status();
        if code == StatusCode::CONFLICT {
            return Err(WorkerError::ClaimLost(id));
        }
        if code == StatusCode::NOT_FOUND {
            return Err(WorkerError::NotFound(id));
        }
        check_ok(resp).await?;
        Ok(())
    }

    // ----- Participants + chunks ----------------------------------------

    pub async fn list_participants(&self, id: SessionId) -> Result<Vec<Participant>> {
        let url = format!("{}/internal/sessions/{}/participants", self.base_url, id);
        let resp = self
            .http
            .get(url)
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        Ok(check_ok(resp).await?.json().await?)
    }

    pub async fn list_chunks(
        &self,
        id: SessionId,
        pseudo: &PseudoId,
    ) -> Result<Vec<ChunkInfo>> {
        let url = format!(
            "{}/internal/sessions/{}/audio/{}/chunks",
            self.base_url, id, pseudo
        );
        let resp = self
            .http
            .get(url)
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        Ok(check_ok(resp).await?.json().await?)
    }

    /// Download one chunk. Handles 401 via one-shot re-auth, 5xx via
    /// exponential backoff (1s, 2s, 4s). 4xx (other than 401) fails fast.
    pub async fn download_chunk(
        &self,
        id: SessionId,
        pseudo: &PseudoId,
        seq: Seq,
    ) -> Result<Vec<u8>> {
        const MAX_5XX_RETRIES: u32 = 3;
        let backoffs = [Duration::from_secs(1), Duration::from_secs(2), Duration::from_secs(4)];

        let url = format!(
            "{}/internal/sessions/{}/audio/{}/chunk/{}",
            self.base_url, id, pseudo, seq
        );
        let mut attempt = 0u32;
        loop {
            let resp = self
                .http
                .get(&url)
                .header("authorization", self.auth_header().await)
                .send()
                .await?;
            let code = resp.status();

            if code.is_success() {
                return Ok(resp.bytes().await?.to_vec());
            }
            if code == StatusCode::UNAUTHORIZED {
                self.re_authenticate().await?;
                continue;
            }
            if code.is_server_error() && attempt < MAX_5XX_RETRIES {
                let delay = backoffs[attempt as usize];
                attempt += 1;
                tokio::time::sleep(delay).await;
                continue;
            }
            let body = resp.text().await.unwrap_or_default();
            return Err(WorkerError::Api(format!("chunk {code}: {body}")));
        }
    }

    // ----- Output writes -------------------------------------------------

    pub async fn post_segments(&self, id: SessionId, segs: &[CreateInputWire]) -> Result<()> {
        self.bulk_insert(id, "segments", segs).await
    }

    pub async fn post_beats(&self, id: SessionId, beats: &[CreateInputWire]) -> Result<()> {
        self.bulk_insert(id, "beats", beats).await
    }

    pub async fn post_scenes(&self, id: SessionId, scenes: &[CreateInputWire]) -> Result<()> {
        self.bulk_insert(id, "scenes", scenes).await
    }

    /// Shared bulk-insert handler. Data-api expects the body to be wrapped
    /// under the resource kind (`{ "segments": [...] }`); each entry must
    /// carry a `client_id` for idempotent dedup.
    async fn bulk_insert(
        &self,
        id: SessionId,
        kind: &str,
        rows: &[CreateInputWire],
    ) -> Result<()> {
        if rows.is_empty() { return Ok(()); }
        let url = format!("{}/internal/sessions/{}/{}", self.base_url, id, kind);
        let body = serde_json::json!({ kind: rows });
        let resp = self
            .http
            .post(url)
            .header("authorization", self.auth_header().await)
            .json(&body)
            .send()
            .await?;
        check_ok(resp).await?;
        Ok(())
    }

    // ----- Clear outputs on rerun ---------------------------------------

    /// List existing segment IDs for a session (used by rerun to DELETE).
    pub async fn list_segment_ids(&self, id: SessionId) -> Result<Vec<Uuid>> {
        self.list_resource_ids(id, "segments").await
    }

    pub async fn list_beat_ids(&self, id: SessionId) -> Result<Vec<Uuid>> {
        self.list_resource_ids(id, "beats").await
    }

    pub async fn list_scene_ids(&self, id: SessionId) -> Result<Vec<Uuid>> {
        self.list_resource_ids(id, "scenes").await
    }

    async fn list_resource_ids(&self, id: SessionId, resource: &str) -> Result<Vec<Uuid>> {
        let url = format!("{}/internal/sessions/{}/{}", self.base_url, id, resource);
        let resp = self
            .http
            .get(url)
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        let resp = check_ok(resp).await?;
        // Tolerate two shapes: `[{id:..}, ...]` and `{items:[{id:..}]}`.
        let v: serde_json::Value = resp.json().await?;
        let rows: Vec<ResourceRow> = match v {
            serde_json::Value::Array(_) => serde_json::from_value(v)?,
            serde_json::Value::Object(mut o) => {
                let items = o.remove("items").unwrap_or(serde_json::Value::Array(vec![]));
                serde_json::from_value(items)?
            }
            _ => Vec::new(),
        };
        Ok(rows.into_iter().map(|r| r.id).collect())
    }

    pub async fn delete_segment(&self, id: Uuid) -> Result<()> {
        self.delete_resource("segments", id).await
    }

    pub async fn delete_beat(&self, id: Uuid) -> Result<()> {
        self.delete_resource("beats", id).await
    }

    pub async fn delete_scene(&self, id: Uuid) -> Result<()> {
        self.delete_resource("scenes", id).await
    }

    async fn delete_resource(&self, kind: &str, id: Uuid) -> Result<()> {
        let url = format!("{}/internal/{}/{}", self.base_url, kind, id);
        let resp = self
            .http
            .delete(url)
            .header("authorization", self.auth_header().await)
            .send()
            .await?;
        // 404 means someone already deleted it — idempotent.
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        check_ok(resp).await?;
        Ok(())
    }
}

/// Convert a non-2xx response into a `WorkerError::Api` carrying the status+body.
async fn check_ok(resp: Response) -> Result<Response> {
    if resp.status().is_success() {
        Ok(resp)
    } else {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Err(WorkerError::Api(format!("{status}: {body}")))
    }
}
