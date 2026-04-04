//! HTTP client for the Data API.
//!
//! The worker never touches Postgres or S3 directly. All storage
//! operations go through this client.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error {status}: {body}")]
    Api { status: u16, body: String },
    #[error("Auth failed: {0}")]
    Auth(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ApiError>;

#[derive(Deserialize)]
struct AuthResponse {
    session_token: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Session {
    pub id: Uuid,
    pub guild_id: Option<i64>,
    pub status: Option<String>,
    pub s3_prefix: Option<String>,
    pub participant_count: Option<i32>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Participant {
    pub id: Uuid,
    pub user_id: Uuid,
    pub pseudo_id: Option<String>,
    pub consent_scope: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ChunkInfo {
    pub key: String,
    pub size: u64,
    pub seq: u32,
}

#[derive(Serialize)]
pub struct CreateSegment {
    pub segment_index: i32,
    pub speaker_pseudo_id: String,
    pub start_time: f64,
    pub end_time: f64,
    pub text: String,
    pub original_text: String,
    pub confidence: Option<f64>,
}

pub struct DataApiClient {
    client: Client,
    base_url: String,
    session_token: String,
}

impl DataApiClient {
    /// Authenticate with the Data API using the admission token file.
    pub async fn authenticate(
        base_url: &str,
        admission_token_path: &str,
        service_name: &str,
    ) -> Result<Self> {
        let admission_token = std::fs::read_to_string(admission_token_path)
            .map_err(|e| ApiError::Auth(format!("failed to read admission token: {}", e)))?;

        let client = Client::new();
        let resp = client
            .post(format!("{}/internal/auth", base_url))
            .json(&serde_json::json!({
                "admission_token": admission_token.trim(),
                "service_name": service_name,
            }))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ApiError::Auth(format!("auth failed ({}): {}", status, body)));
        }

        let auth: AuthResponse = resp.json().await?;

        tracing::info!(service = service_name, "authenticated with Data API");

        Ok(Self {
            client,
            base_url: base_url.to_string(),
            session_token: auth.session_token,
        })
    }

    /// Send heartbeat.
    pub async fn heartbeat(&self) -> Result<()> {
        let resp = self
            .client
            .post(format!("{}/internal/heartbeat", self.base_url))
            .bearer_auth(&self.session_token)
            .send()
            .await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ApiError::Api {
                status: 500,
                body,
            });
        }
        Ok(())
    }

    /// List sessions with a given status.
    pub async fn list_sessions_by_status(&self, status: &str) -> Result<Vec<Session>> {
        // The Data API's list_sessions requires user_pseudo_id, but for internal
        // service use we need a different query. For now, get all sessions and filter.
        // TODO: Add a /internal/sessions?status=X endpoint to the Data API.
        let resp = self
            .client
            .get(format!("{}/internal/sessions?status={}", self.base_url, status))
            .bearer_auth(&self.session_token)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status_code = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ApiError::Api {
                status: status_code,
                body,
            });
        }
        Ok(resp.json().await?)
    }

    /// Get session participants.
    pub async fn list_participants(&self, session_id: Uuid) -> Result<Vec<Participant>> {
        let resp = self
            .client
            .get(format!(
                "{}/internal/sessions/{}/participants",
                self.base_url, session_id
            ))
            .bearer_auth(&self.session_token)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ApiError::Api { status, body });
        }
        Ok(resp.json().await?)
    }

    /// List audio chunks for a speaker.
    pub async fn list_chunks(
        &self,
        session_id: Uuid,
        pseudo_id: &str,
    ) -> Result<Vec<ChunkInfo>> {
        let resp = self
            .client
            .get(format!(
                "{}/internal/sessions/{}/audio/{}/chunks",
                self.base_url, session_id, pseudo_id
            ))
            .bearer_auth(&self.session_token)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ApiError::Api { status, body });
        }
        Ok(resp.json().await?)
    }

    /// Download a specific audio chunk (raw PCM bytes).
    pub async fn download_chunk(
        &self,
        session_id: Uuid,
        pseudo_id: &str,
        seq: u32,
    ) -> Result<Vec<u8>> {
        let resp = self
            .client
            .get(format!(
                "{}/internal/sessions/{}/audio/{}/chunk/{}",
                self.base_url, session_id, pseudo_id, seq
            ))
            .bearer_auth(&self.session_token)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ApiError::Api { status, body });
        }
        Ok(resp.bytes().await?.to_vec())
    }

    /// Post transcript segments for a session.
    pub async fn create_segments(
        &self,
        session_id: Uuid,
        segments: &[CreateSegment],
    ) -> Result<()> {
        let resp = self
            .client
            .post(format!(
                "{}/internal/sessions/{}/segments",
                self.base_url, session_id
            ))
            .bearer_auth(&self.session_token)
            .json(segments)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ApiError::Api { status, body });
        }
        Ok(())
    }

    /// Update session status.
    pub async fn update_session_status(
        &self,
        session_id: Uuid,
        status: &str,
    ) -> Result<()> {
        let resp = self
            .client
            .patch(format!(
                "{}/internal/sessions/{}",
                self.base_url, session_id
            ))
            .bearer_auth(&self.session_token)
            .json(&serde_json::json!({ "status": status }))
            .send()
            .await?;
        if !resp.status().is_success() {
            let status_code = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ApiError::Api {
                status: status_code,
                body,
            });
        }
        Ok(())
    }
}
