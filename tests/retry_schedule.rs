//! Retry schedule: 30s / 2m / 10m spacing verified with `tokio::time::pause`.
//!
//! Attempt 1 -> 30s later, attempt 2 -> 2m, attempt 3 -> 10m, attempt 4
//! -> None (schedule exhausted).

use std::time::Duration;

use chronicle_worker::ids::SessionId;
use chronicle_worker::retry::RetryQueue;
use tokio::time::Instant;
use uuid::Uuid;

#[tokio::test(start_paused = true)]
async fn spacing_is_30s_2m_10m() {
    let schedule = vec![
        Duration::from_secs(30),
        Duration::from_secs(120),
        Duration::from_secs(600),
    ];
    let q = RetryQueue::new(schedule);
    let session = SessionId(Uuid::new_v4());

    // --- Attempt 1: 30s ---
    q.schedule(session, 1).await.expect("attempt 1 queues");
    let before = Instant::now();
    let e1 = q.next_ready().await;
    let elapsed = e1.due_at - before;
    assert_eq!(e1.attempt, 1);
    assert!(
        elapsed >= Duration::from_secs(29) && elapsed <= Duration::from_secs(31),
        "attempt 1 spacing: expected ~30s, got {elapsed:?}"
    );

    // --- Attempt 2: 2 minutes ---
    q.schedule(session, 2).await.expect("attempt 2 queues");
    let before = Instant::now();
    let e2 = q.next_ready().await;
    let elapsed = e2.due_at - before;
    assert_eq!(e2.attempt, 2);
    assert!(
        elapsed >= Duration::from_secs(119) && elapsed <= Duration::from_secs(121),
        "attempt 2 spacing: expected ~2min, got {elapsed:?}"
    );

    // --- Attempt 3: 10 minutes ---
    q.schedule(session, 3).await.expect("attempt 3 queues");
    let before = Instant::now();
    let e3 = q.next_ready().await;
    let elapsed = e3.due_at - before;
    assert_eq!(e3.attempt, 3);
    assert!(
        elapsed >= Duration::from_secs(599) && elapsed <= Duration::from_secs(601),
        "attempt 3 spacing: expected ~10min, got {elapsed:?}"
    );

    // --- Attempt 4: schedule exhausted ---
    assert!(
        q.schedule(session, 4).await.is_none(),
        "attempt 4 should return None (schedule exhausted)"
    );
}

#[tokio::test]
async fn custom_csv_schedule_via_config_parses() {
    // Not a time test — just parse validation. Uses Config::retry_backoffs.
    use chronicle_worker::config::Config;
    use clap::Parser;

    // clap Parser::try_parse_from consumes an argv-like vec.
    let cfg = Config::try_parse_from([
        "chronicle-worker",
        "--shared-secret", "x",
        "--retry-backoff-ms", "1000,2000,3000,4000",
    ])
    .expect("parse config");
    let schedule = cfg.retry_backoffs().expect("valid schedule");
    assert_eq!(schedule, vec![
        Duration::from_millis(1000),
        Duration::from_millis(2000),
        Duration::from_millis(3000),
        Duration::from_millis(4000),
    ]);
}
