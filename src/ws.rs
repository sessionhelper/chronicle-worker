//! WebSocket subscription to the data-api event bus.
//!
//! Responsibilities:
//!
//! - Connect and subscribe (with exponential reconnect backoff).
//! - Parse incoming JSON frames into a strongly-typed `BusEvent` enum
//!   exposed to the event loop. Unknown frames are logged and dropped.
//!
//! The spec locks event names `session_state_changed` and `chunk_uploaded`;
//! the running data-api today emits `session_status_changed` (historical
//! drift). Both names deserialize into the same variant via serde aliases.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;
use uuid::Uuid;

use crate::api_client::DataApiClient;
use crate::ids::{PseudoId, Seq, SessionId};

/// Strongly-typed events the event loop cares about.
#[derive(Debug, Clone)]
pub enum BusEvent {
    /// A session moved to `uploaded`. Worker should claim + process.
    SessionUploaded(SessionId),
    /// Any other state transition — worth logging but no action.
    SessionStatus { session: SessionId, status: String },
    /// A new chunk was uploaded for a session.
    ChunkUploaded { session: SessionId, pseudo: PseudoId, seq: Seq },
    /// WS told us the connection dropped; event loop triggers reconnect.
    Disconnected,
}

// Raw wire shape — accepts both spec-locked `session_state_changed`
// and the current data-api's `session_status_changed`.
#[derive(Deserialize, Debug)]
#[serde(tag = "event")]
enum RawEvent {
    #[serde(alias = "session_state_changed", rename = "session_status_changed")]
    SessionStateChanged {
        session_id: Uuid,
        #[serde(alias = "new")]
        status: String,
    },
    #[serde(rename = "chunk_uploaded")]
    ChunkUploaded {
        session_id: Uuid,
        pseudo_id: String,
        /// Data-api wire says `seq`; spec says `chunk_seq`. Accept either.
        #[serde(alias = "chunk_seq")]
        seq: u32,
    },
}

/// Spawn a WS read loop that pipes parsed events to `tx`.
///
/// One spawned task keeps the WS alive with reconnect/backoff; the event
/// loop only sees `BusEvent`s on the channel. Returns the receiver.
pub fn start(api: Arc<DataApiClient>) -> mpsc::Receiver<BusEvent> {
    // Generous buffer — worst case an entire session's chunks stack up
    // before the event loop drains one. Dropping would cost us a rerun.
    let (tx, rx) = mpsc::channel::<BusEvent>(256);
    tokio::spawn(ws_task(api, tx));
    rx
}

async fn ws_task(api: Arc<DataApiClient>, tx: mpsc::Sender<BusEvent>) {
    let mut backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    loop {
        let ws_url = api.ws_url().await;
        tracing::info!(url = %redact_token(&ws_url), "connecting to data-api WS");
        match tokio_tungstenite::connect_async(&ws_url).await {
            Ok((stream, _)) => {
                backoff = Duration::from_secs(1);
                if !run_session(stream, &tx).await { break; }
            }
            Err(e) => {
                tracing::warn!(error = %e, backoff_secs = backoff.as_secs(), "WS connect failed");
            }
        }
        let _ = tx.send(BusEvent::Disconnected).await;
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Read one live WS connection to EOF. Returns `false` iff the output
/// channel was closed (event loop gone → shut down the WS task).
async fn run_session(
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    tx: &mpsc::Sender<BusEvent>,
) -> bool {
    let (mut sink, mut source) = stream.split();

    // Subscribe to all session events. The current data-api uses
    // topic-based subscription; spec uses events-list. Both the worker
    // and data-api can evolve later — today we send the topic form.
    let sub = serde_json::json!({ "subscribe": "sessions" }).to_string();
    if let Err(e) = sink.send(tungstenite::Message::Text(sub.into())).await {
        tracing::warn!(error = %e, "WS subscribe failed");
        return true;
    }
    tracing::info!("WS subscribed (sessions)");

    loop {
        let Some(msg) = source.next().await else { break };
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "WS recv error");
                break;
            }
        };

        match msg {
            tungstenite::Message::Text(text) => {
                match serde_json::from_str::<RawEvent>(&text) {
                    Ok(raw) => {
                        let event = match raw {
                            RawEvent::SessionStateChanged { session_id, status } => {
                                if status == "uploaded" {
                                    BusEvent::SessionUploaded(SessionId(session_id))
                                } else {
                                    BusEvent::SessionStatus { session: SessionId(session_id), status }
                                }
                            }
                            RawEvent::ChunkUploaded { session_id, pseudo_id, seq } => {
                                BusEvent::ChunkUploaded {
                                    session: SessionId(session_id),
                                    pseudo: PseudoId(pseudo_id),
                                    seq: Seq(seq),
                                }
                            }
                        };
                        if tx.send(event).await.is_err() { return false; }
                    }
                    Err(e) => {
                        tracing::trace!(error = %e, "WS ignoring unparsed event");
                    }
                }
            }
            tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => {
                // tungstenite handles pong automatically.
            }
            tungstenite::Message::Close(_) => {
                tracing::info!("WS closed by server");
                break;
            }
            _ => {}
        }
    }
    true
}

fn redact_token(url: &str) -> String {
    match url.split_once("token=") {
        Some((head, _)) => format!("{head}token=REDACTED"),
        None => url.to_string(),
    }
}
