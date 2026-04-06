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
  decode.rs      — PCM decode: stereo s16le bytes → mono f32 samples
  worker.rs      — run() + process_next_session(), the polling loop
```

## Env vars

| Var | Required | Default |
|---|---|---|
| `DATA_API_URL` | yes | — |
| `DATA_API_SHARED_SECRET` | yes | — |
| `POLL_INTERVAL_SECS` | no | `10` |
| `WHISPER_URL` | yes | — |
| `WHISPER_MODEL` | no | `deepdml/faster-whisper-large-v3-turbo-ct2` |
| `VAD_MODEL_PATH` | yes | — |
| `LOG_LEVEL` | no | `info` |

## Build

```bash
cargo build --release

# Docker (build context must include both ovp-worker/ and ovp-pipeline/)
cd /home/alex
docker build -f ovp-worker/Dockerfile -t ovp-worker:dev .
```
