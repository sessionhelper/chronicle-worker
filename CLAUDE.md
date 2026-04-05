# ovp-worker

> Org-wide conventions (Rust style, git workflow, shared-secret auth, cross-service architecture) live in `/home/alex/sessionhelper-hub/CLAUDE.md`. Read that first for anything cross-cutting. Data flow diagram: `sessionhelper-hub/ARCHITECTURE.md`.

Batch orchestrator: polls Data API for `uploaded` sessions, downloads audio, runs `ovp-pipeline` as a library, posts transcript segments back. Never touches Postgres or S3 directly — everything goes through the Data API.

## Main loop shape

```
main.rs  ──► authenticate ──► AppState ──► spawn heartbeat (30s)
                                       └─► worker::run(state)

worker::run:
    loop {
        sleep(poll_interval);
        match process_next_session(&state).await {
            Ok(Some(id)) => info!("session_processed"),
            Ok(None)     => continue,           // queue empty
            Err(e)       => error!("process_failed"),  // log, keep going
        }
    }
```

`process_next_session` pulls one uploaded session, marks it `transcribing`, filters participants to `consent_scope=full`, downloads all chunks per speaker, decodes s16le stereo -> mono f32, invokes `ovp_pipeline::process_session`, posts segments, marks `transcribed`.

## Project layout

```
src/
  main.rs        — binary entrypoint: tracing, config, auth, heartbeat, run
  lib.rs         — re-exports (the binary consumes the library)
  config.rs      — clap-derived env config
  state.rs       — AppState: Arc<DataApiClient> + Config
  api_client.rs  — HTTP client: auth, heartbeat, sessions, chunks, segments
  worker.rs      — run() + process_next_session(), the polling loop
```

## Scaffolding status

This tree is a skeleton. Real implementation lives behind `TODO:` comments — `rg 'TODO' src/` gives an ordered checklist. Known missing pieces:

- `GET /internal/sessions?status=uploaded` does not exist in `ovp-data-api` yet (`src/routes/sessions.rs::list_sessions` only filters by user).
- PCM decode, pipeline invocation, and segment mapping are stubbed as empty `Vec`s.
- Segment POST wire-format unconfirmed against `ovp-data-api/src/routes/segments.rs::bulk_create_segments`.

Do not modify `ovp-pipeline` — it's a fixed dependency. If a new endpoint is needed on `ovp-data-api`, add it there and update the worker's client to match.
