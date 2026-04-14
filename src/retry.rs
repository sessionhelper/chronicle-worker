//! Session-level retry queue.
//!
//! When a session-level failure occurs, the runner PATCHes the session
//! to `transcribing_failed` and pushes a `RetryEntry` onto the queue
//! with the first backoff. The event-loop's `select!` waits on
//! `next_ready` — a future that resolves to the session due for retry.
//!
//! The schedule is read from config (`RETRY_BACKOFF_MS`), default
//! `30s / 2m / 10m`. Attempts exceeding the schedule length are dropped
//! from the queue; the session stays in `transcribing_failed` until an
//! admin rerun is issued.

use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::ids::SessionId;

/// One pending retry entry: which session, which attempt number,
/// when it's due.
#[derive(Debug, Clone, Copy)]
pub struct RetryEntry {
    pub session: SessionId,
    pub attempt: u32, // 1-based
    pub due_at: Instant,
}

// Reverse ordering so `BinaryHeap` becomes a min-heap on `due_at`.
impl Ord for RetryEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.due_at.cmp(&self.due_at)
    }
}
impl PartialOrd for RetryEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for RetryEntry {
    fn eq(&self, other: &Self) -> bool { self.due_at == other.due_at }
}
impl Eq for RetryEntry {}

/// Thread-safe priority queue of retry entries.
#[derive(Clone)]
pub struct RetryQueue {
    inner: Arc<Mutex<BinaryHeap<RetryEntry>>>,
    schedule: Arc<Vec<Duration>>,
    notify: Arc<tokio::sync::Notify>,
}

impl RetryQueue {
    pub fn new(schedule: Vec<Duration>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(BinaryHeap::new())),
            schedule: Arc::new(schedule),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Number of configured retry attempts (3 by default).
    pub fn max_attempts(&self) -> u32 { self.schedule.len() as u32 }

    /// Schedule the next retry for a session that just failed its
    /// `attempt`-th run. Returns `Some(entry)` if queued, `None` if
    /// the schedule is exhausted.
    pub async fn schedule(&self, session: SessionId, attempt: u32) -> Option<RetryEntry> {
        // 1-based attempt; look up next index = attempt
        let idx = attempt as usize;
        if idx > self.schedule.len() || self.schedule.is_empty() {
            return None;
        }
        let delay = self.schedule[idx - 1];
        let entry = RetryEntry {
            session,
            attempt,
            due_at: Instant::now() + delay,
        };
        self.inner.lock().await.push(entry);
        self.notify.notify_waiters();
        Some(entry)
    }

    /// Await the next ready entry. Resolves when an entry's `due_at`
    /// passes. Returns the entry, removing it from the queue.
    pub async fn next_ready(&self) -> RetryEntry {
        loop {
            let sleep_for = {
                let heap = self.inner.lock().await;
                match heap.peek() {
                    Some(e) => e.due_at.saturating_duration_since(Instant::now()),
                    None => Duration::from_secs(3600 * 24), // effectively "wait forever"
                }
            };
            let notified = self.notify.notified();
            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {}
                _ = notified => {}
            }
            let mut heap = self.inner.lock().await;
            if let Some(top) = heap.peek().copied() {
                if top.due_at <= Instant::now() {
                    heap.pop();
                    return top;
                }
            }
        }
    }

    /// For tests / introspection.
    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test(start_paused = true)]
    async fn schedule_spacing_matches_config() {
        let schedule = vec![
            Duration::from_secs(30),
            Duration::from_secs(120),
            Duration::from_secs(600),
        ];
        let q = RetryQueue::new(schedule);
        let session = SessionId(Uuid::new_v4());

        // Attempt 1 -> 30s
        let e1 = q.schedule(session, 1).await.expect("attempt 1 queues");
        assert!(e1.due_at.saturating_duration_since(Instant::now()) <= Duration::from_secs(30));

        let start = Instant::now();
        let ready = q.next_ready().await;
        assert_eq!(ready.session, session);
        assert_eq!(ready.attempt, 1);
        let waited = ready.due_at.saturating_duration_since(start);
        assert!(waited >= Duration::from_secs(29) && waited <= Duration::from_secs(31));

        // Attempt 2 -> 2m
        let _ = q.schedule(session, 2).await.expect("attempt 2 queues");
        let before = Instant::now();
        let ready2 = q.next_ready().await;
        assert_eq!(ready2.attempt, 2);
        assert!(ready2.due_at.saturating_duration_since(before) >= Duration::from_secs(119));
        assert!(ready2.due_at.saturating_duration_since(before) <= Duration::from_secs(121));

        // Attempt 3 -> 10m
        let _ = q.schedule(session, 3).await.expect("attempt 3 queues");
        let before = Instant::now();
        let ready3 = q.next_ready().await;
        assert_eq!(ready3.attempt, 3);
        assert!(ready3.due_at.saturating_duration_since(before) >= Duration::from_secs(599));
        assert!(ready3.due_at.saturating_duration_since(before) <= Duration::from_secs(601));

        // Attempt 4 -> nothing (schedule exhausted)
        assert!(q.schedule(session, 4).await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn empty_queue_waits_forever_until_pushed() {
        let q = RetryQueue::new(vec![Duration::from_secs(30)]);
        let q2 = q.clone();
        let session = SessionId(Uuid::new_v4());

        let h = tokio::spawn(async move { q2.next_ready().await });

        // Advance a minute with nothing in the queue — should still be pending.
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert!(!h.is_finished(), "next_ready returned with an empty queue");

        // Now schedule — should fire in 30s.
        q.schedule(session, 1).await.unwrap();
        tokio::time::sleep(Duration::from_secs(31)).await;
        let e = h.await.unwrap();
        assert_eq!(e.session, session);
    }
}
