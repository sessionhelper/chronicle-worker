//! The worker's single event loop.
//!
//! Drives everything by `tokio::select!` across four sources:
//!
//! 1. WebSocket events from the data-api bus.
//! 2. Periodic REST reconciliation tick (safety net for WS blackouts).
//! 3. Retry-queue entries whose backoff has elapsed.
//! 4. Admin HTTP commands (when the admin surface is enabled).
//!
//! Completed session-runner tasks feed in via a `JoinSet` so failures
//! translate into retry-queue pushes without a separate bookkeeping layer.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::{interval, MissedTickBehavior};
use uuid::Uuid;

use crate::admin::{self, AdminCmd, AdminState, StatusSnapshot};
use crate::api_client::{DataApiClient, SessionSummary};
use crate::config::Config;
use crate::error::{Result, WorkerError};
use crate::ids::SessionId;
use crate::retry::{RetryEntry, RetryQueue};
use crate::session_runner::{self, RunnerCmd, RunnerMode, SessionInputs};
use crate::ws::{self, BusEvent};

/// In-flight runner state tracked by the event loop.
struct ActiveRunner {
    tx: mpsc::Sender<RunnerCmd>,
    abort: AbortHandle,
}

/// Wrapper carrying the result of a finished runner back into the loop.
struct RunnerResult {
    session: SessionId,
    attempt: u32,
    outcome: Result<()>,
}

/// Run the event loop forever. `main.rs` calls this.
pub async fn run(api: Arc<DataApiClient>, cfg: Arc<Config>) -> Result<()> {
    let retry = RetryQueue::new(cfg.retry_backoffs()?);

    let mut ws_rx = ws::start(api.clone());

    let mut poll_timer = interval(cfg.poll_interval());
    poll_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let active: Arc<Mutex<HashMap<SessionId, ActiveRunner>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut runner_joins: JoinSet<RunnerResult> = JoinSet::new();
    let last_heartbeat = Arc::new(Mutex::new(None));

    spawn_heartbeat(api.clone(), last_heartbeat.clone());

    let mut admin_rx = if cfg.admin_enabled {
        let (tx, rx) = mpsc::channel::<AdminCmd>(32);
        let state = AdminState { cmd_tx: tx, last_heartbeat: last_heartbeat.clone() };
        admin::serve(cfg.admin_bind_addr, state).await?;
        Some(rx)
    } else {
        None
    };

    // Startup reconciliation per spec §Startup flow.
    startup_reconcile(&api, &cfg, &active, &mut runner_joins).await;

    tracing::info!("worker event loop entering main select");

    loop {
        tokio::select! {
            ws_event = ws_rx.recv() => {
                let Some(ev) = ws_event else {
                    tracing::warn!("WS channel closed permanently; exiting event loop");
                    break;
                };
                handle_ws_event(ev, &api, &cfg, &active, &mut runner_joins).await;
            }
            _ = poll_timer.tick() => {
                poll_reconcile(&api, &cfg, &active, &mut runner_joins).await;
            }
            retry_entry = retry.next_ready() => {
                handle_retry(retry_entry, &api, &cfg, &active, &mut runner_joins).await;
            }
            Some(join) = runner_joins.join_next() => {
                match join {
                    Ok(res) => on_runner_complete(res, &retry, &active, &api).await,
                    Err(e) => tracing::warn!(error = %e, "runner task panicked"),
                }
            }
            admin_cmd = recv_admin(admin_rx.as_mut()) => {
                if let Some(cmd) = admin_cmd {
                    handle_admin(cmd, &api, &cfg, &active, &mut runner_joins).await;
                }
            }
        }
    }

    Ok(())
}

async fn recv_admin(rx: Option<&mut mpsc::Receiver<AdminCmd>>) -> Option<AdminCmd> {
    match rx {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

// --- Startup reconciliation ---------------------------------------------

async fn startup_reconcile(
    api: &Arc<DataApiClient>,
    cfg: &Arc<Config>,
    active: &Arc<Mutex<HashMap<SessionId, ActiveRunner>>>,
    joins: &mut JoinSet<RunnerResult>,
) {
    match api.list_sessions_by_status("transcribing").await {
        Ok(sessions) => {
            for s in sessions {
                let sid = SessionId(s.id);
                tracing::info!(%sid, "startup: orphaned transcribing session → one-shot rerun");
                spawn_runner(api, cfg, sid, 1, RunnerMode::OneShot, active, joins).await;
            }
        }
        Err(e) => tracing::warn!(error = %e, "startup: list transcribing failed"),
    }
    match api.list_sessions_by_status("uploaded").await {
        Ok(sessions) => {
            for s in sessions {
                let sid = SessionId(s.id);
                tracing::info!(%sid, "startup: uploaded session → one-shot");
                spawn_runner(api, cfg, sid, 1, RunnerMode::OneShot, active, joins).await;
            }
        }
        Err(e) => tracing::warn!(error = %e, "startup: list uploaded failed"),
    }
}

// --- Poll (safety net) ---------------------------------------------------

async fn poll_reconcile(
    api: &Arc<DataApiClient>,
    cfg: &Arc<Config>,
    active: &Arc<Mutex<HashMap<SessionId, ActiveRunner>>>,
    joins: &mut JoinSet<RunnerResult>,
) {
    let uploaded: Vec<SessionSummary> = match api.list_sessions_by_status("uploaded").await {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!(error = %e, "poll: list uploaded failed");
            return;
        }
    };
    let pending: Vec<SessionId> = {
        let locked = active.lock().await;
        uploaded
            .into_iter()
            .map(|s| SessionId(s.id))
            .filter(|id| !locked.contains_key(id))
            .collect()
    };
    for sid in pending {
        tracing::info!(%sid, "poll reconcile: spawning one-shot");
        spawn_runner(api, cfg, sid, 1, RunnerMode::OneShot, active, joins).await;
    }
}

// --- WS event dispatch ---------------------------------------------------

async fn handle_ws_event(
    ev: BusEvent,
    api: &Arc<DataApiClient>,
    cfg: &Arc<Config>,
    active: &Arc<Mutex<HashMap<SessionId, ActiveRunner>>>,
    joins: &mut JoinSet<RunnerResult>,
) {
    match ev {
        BusEvent::SessionUploaded(session) => {
            let tx_opt = active.lock().await.get(&session).map(|h| h.tx.clone());
            match tx_opt {
                Some(tx) => {
                    if tx.send(RunnerCmd::Finalize).await.is_err() {
                        tracing::warn!(%session, "finalize cmd lost, falling back to one-shot");
                        spawn_runner(api, cfg, session, 1, RunnerMode::OneShot, active, joins).await;
                    }
                }
                None => {
                    spawn_runner(api, cfg, session, 1, RunnerMode::OneShot, active, joins).await;
                }
            }
        }
        BusEvent::ChunkUploaded { session, pseudo, seq } => {
            let tx = {
                let locked = active.lock().await;
                if let Some(h) = locked.get(&session) {
                    h.tx.clone()
                } else {
                    drop(locked);
                    spawn_runner_return_tx(
                        api, cfg, session, 1, RunnerMode::Streaming, active, joins,
                    )
                    .await
                }
            };
            if tx.send(RunnerCmd::IngestChunk { pseudo, seq }).await.is_err() {
                tracing::debug!(%session, "ingest cmd dropped (runner finished)");
            }
        }
        BusEvent::SessionStatus { session, status } => {
            tracing::debug!(%session, %status, "status changed (no action)");
        }
        BusEvent::Disconnected => {
            tracing::info!("WS disconnected; poll timer will reconcile");
        }
    }
}

// --- Retry dispatch ------------------------------------------------------

async fn handle_retry(
    entry: RetryEntry,
    api: &Arc<DataApiClient>,
    cfg: &Arc<Config>,
    active: &Arc<Mutex<HashMap<SessionId, ActiveRunner>>>,
    joins: &mut JoinSet<RunnerResult>,
) {
    tracing::info!(session = %entry.session, attempt = entry.attempt, "retry: one-shot rerun");
    spawn_runner(api, cfg, entry.session, entry.attempt, RunnerMode::OneShot, active, joins).await;
}

// --- Admin dispatch ------------------------------------------------------

async fn handle_admin(
    cmd: AdminCmd,
    api: &Arc<DataApiClient>,
    cfg: &Arc<Config>,
    active: &Arc<Mutex<HashMap<SessionId, ActiveRunner>>>,
    joins: &mut JoinSet<RunnerResult>,
) {
    match cmd {
        AdminCmd::Status { ack } => {
            let ids: Vec<Uuid> = active.lock().await.keys().map(|s| s.as_uuid()).collect();
            let _ = ack.send(StatusSnapshot {
                active_sessions: ids,
                last_heartbeat_at: Some(Utc::now()),
                version: admin::VERSION,
            });
        }
        AdminCmd::Rerun { session, ack } => {
            // Abort any existing runner first so we don't race outputs.
            if let Some(h) = active.lock().await.remove(&session) {
                let _ = h.tx.send(RunnerCmd::Abort).await;
                h.abort.abort();
            }
            if let Err(e) = session_runner::clear_prior_outputs(api, session).await {
                tracing::warn!(%session, error = %e, "admin rerun: clear outputs failed");
                let _ = ack.send(false);
                return;
            }
            spawn_runner(api, cfg, session, 1, RunnerMode::OneShot, active, joins).await;
            let _ = ack.send(true);
        }
    }
}

// --- Spawning helpers ----------------------------------------------------

async fn spawn_runner(
    api: &Arc<DataApiClient>,
    cfg: &Arc<Config>,
    session: SessionId,
    attempt: u32,
    mode: RunnerMode,
    active: &Arc<Mutex<HashMap<SessionId, ActiveRunner>>>,
    joins: &mut JoinSet<RunnerResult>,
) {
    let _ = spawn_runner_return_tx(api, cfg, session, attempt, mode, active, joins).await;
}

async fn spawn_runner_return_tx(
    api: &Arc<DataApiClient>,
    cfg: &Arc<Config>,
    session: SessionId,
    attempt: u32,
    mode: RunnerMode,
    active: &Arc<Mutex<HashMap<SessionId, ActiveRunner>>>,
    joins: &mut JoinSet<RunnerResult>,
) -> mpsc::Sender<RunnerCmd> {
    let handle = session_runner::spawn(SessionInputs {
        api: api.clone(),
        cfg: cfg.clone(),
        session,
        mode,
    });
    let tx_for_map = handle.tx.clone();
    let tx_for_caller = handle.tx;
    let inner_join = handle.join;

    // Drive completion through JoinSet, keep an AbortHandle for the map.
    let abort = joins
        .spawn(async move {
            let outcome = inner_join.await.unwrap_or_else(|e| {
                Err(WorkerError::Api(format!("runner task join: {e}")))
            });
            RunnerResult { session, attempt, outcome }
        });

    active.lock().await.insert(
        session,
        ActiveRunner { tx: tx_for_map, abort },
    );
    tx_for_caller
}

async fn on_runner_complete(
    res: RunnerResult,
    retry: &RetryQueue,
    active: &Arc<Mutex<HashMap<SessionId, ActiveRunner>>>,
    api: &Arc<DataApiClient>,
) {
    active.lock().await.remove(&res.session);

    match res.outcome {
        Ok(()) => {
            tracing::info!(session = %res.session, attempt = res.attempt, "runner done");
        }
        Err(e) if !e.is_retryable_session_failure() => {
            tracing::info!(session = %res.session, error = %e, "runner finished (non-retryable)");
        }
        Err(e) => {
            tracing::warn!(
                session = %res.session,
                attempt = res.attempt,
                error = %e,
                "runner failed"
            );
            // Best-effort mark failed (a later retry will PATCH anyway).
            if let Err(e2) = api.mark_failed(res.session).await {
                tracing::debug!(session = %res.session, error = %e2, "mark_failed failed");
            }
            match retry.schedule(res.session, res.attempt + 1).await {
                Some(entry) => tracing::info!(
                    session = %res.session,
                    next_attempt = entry.attempt,
                    "scheduled retry"
                ),
                None => tracing::info!(
                    session = %res.session,
                    "retry schedule exhausted; session stays transcribing_failed"
                ),
            }
        }
    }
}

// --- Heartbeat -----------------------------------------------------------

fn spawn_heartbeat(
    api: Arc<DataApiClient>,
    last_heartbeat: Arc<Mutex<Option<chrono::DateTime<chrono::Utc>>>>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
        tick.tick().await;
        loop {
            tick.tick().await;
            match api.heartbeat().await {
                Ok(()) => *last_heartbeat.lock().await = Some(Utc::now()),
                Err(e) => tracing::warn!(error = %e, "heartbeat failed"),
            }
        }
    });
}
