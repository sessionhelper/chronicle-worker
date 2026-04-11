# chronicle-worker

Rust event-driven orchestrator that turns captured audio chunks into
structured transcripts. Subscribes to `chronicle-data-api` via
WebSocket for `chunk_uploaded` events, pulls the incoming per-speaker
PCM, feeds it into `chronicle-pipeline` as a library, and posts the
resulting transcript segments, beats, and scenes back to the data-api.

Nothing else in the Chronicle stack writes transcript data. If a session
finalizes without the worker having processed its chunks, the transcript
is empty.

## Role in the stack

```
┌──────────────────┐
│ chronicle-data-  │
│     api          │───► emits chunk_uploaded, session_finalized events
└────────┬─────────┘
         │ WebSocket subscribe
         ▼
┌──────────────────────────┐
│    chronicle-worker      │
│    (this service)        │
│                          │
│  ┌────────────────────┐  │
│  │  ActiveSession     │  │   one per in-progress session
│  │  ├ StreamingPipe   │  │   chronicle-pipeline streaming mode
│  │  ├ per-speaker     │  │
│  │  │   VadSession    │  │
│  │  └ operator state  │  │
│  └────────────────────┘  │
└──────────┬───────────────┘
           │ POST segments, beats, scenes
           ▼
┌──────────────────┐
│ chronicle-data-  │
│     api          │───► writes to Postgres, emits transcript_ready
└──────────────────┘
```

## What it does

- **Subscribes** to the data-api WebSocket at `ws://.../ws` using a
  shared-secret bearer token (re-authenticates on 401)
- **Per session**: maintains an `ActiveSession` that holds a
  `StreamingPipeline` from `chronicle-pipeline`, plus per-speaker state
  for VAD and the operator chain
- **On `chunk_uploaded` event**: downloads the chunk via
  `GET /internal/sessions/{id}/audio/{pseudo}/chunk/{seq}` (with retry
  on 5xx: 1s/2s/4s backoff, one-shot re-auth on 401, fail-fast on 4xx),
  decodes s16le stereo to mono f32, and calls
  `StreamingPipeline::feed_chunk(pseudo_id, samples)`
- **On `session_finalized` event**: drains the pipeline via
  `finalize()`, posts any final segments, marks the session as
  `transcribed` in the data-api
- **Fallback polling loop**: if the WebSocket drops, a polling
  fallback at `POLL_INTERVAL_SECS` catches up on any sessions marked
  `uploaded` that weren't processed via events
- **Retry tolerant**: transient data-api failures trigger the same
  exponential backoff pattern the collector uses for chunk uploads

## Quick start

```bash
# Build context must include BOTH chronicle-worker/ AND chronicle-pipeline/
# side by side, because chronicle-worker depends on chronicle-pipeline via
# a relative path dep (../chronicle-pipeline in Cargo.toml).
#
# If you're in /home/alex/sessionhelper:
cargo build --release --manifest-path chronicle-worker/Cargo.toml

# Or run standalone:
cd chronicle-worker
cp .env.example .env
# Edit .env — add DATA_API_URL, DATA_API_SHARED_SECRET, WHISPER_URL
cargo run --release
```

A healthy start looks like:

```
chronicle-worker starting data_api=http://localhost:8001 poll_interval_secs=10
authenticated with Data API service="chronicle-worker"
worker event loop starting
connecting to data-api WS url=ws://localhost:8001/ws
WS connected, subscribing to sessions/*
```

## Env vars

| Var | Purpose |
|---|---|
| `DATA_API_URL` | Base URL for chronicle-data-api (default `http://localhost:8001`) |
| `DATA_API_SHARED_SECRET` | Cross-service auth token (see `sessionhelper-hub/CLAUDE.md`) |
| `WHISPER_URL` | Whisper HTTP service for transcription |
| `LLM_URL` | Optional LLM endpoint for beat detection |
| `POLL_INTERVAL_SECS` | Fallback polling interval in seconds (default `10`) |
| `RUST_LOG` | tracing filter (e.g. `chronicle_worker=info,chronicle_pipeline=info`) |

## Docker build

The Dockerfile uses a multi-stage build and REQUIRES the parent directory
as the build context (so the path dep on chronicle-pipeline resolves):

```bash
cd /home/alex/sessionhelper   # or wherever the repos live side by side
docker build -f chronicle-worker/Dockerfile -t chronicle-worker:dev .
```

This image is currently built locally and loaded onto the dev VPS via
`docker save | ssh docker load` (there's no GHA deploy workflow yet —
see the dangling issues list in `sessionhelper-hub/SPEC.md` §10).

## Related

- [`chronicle-pipeline`](https://github.com/sessionhelper/chronicle-pipeline) — the library this worker invokes
- [`chronicle-data-api`](https://github.com/sessionhelper/chronicle-data-api) — the service this worker subscribes to and posts back to
- [`sessionhelper-hub/ARCHITECTURE.md`](https://github.com/sessionhelper/sessionhelper-hub/blob/main/ARCHITECTURE.md) — cross-service data flow
- [`sessionhelper-hub/SPEC.md`](https://github.com/sessionhelper/sessionhelper-hub/blob/main/SPEC.md) — OVP program spec
