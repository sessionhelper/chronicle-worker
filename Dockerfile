# Multi-stage Dockerfile for ovp-worker.
#
# Build context MUST be the parent directory containing both ovp-worker/
# and ovp-pipeline/, because ovp-worker depends on ovp-pipeline via a
# relative path (../ovp-pipeline in Cargo.toml).
#
# Build:
#   cd /home/alex  # or wherever both dirs live side by side
#   docker build -f ovp-worker/Dockerfile -t ovp-worker:dev .
#
# The Silero VAD ONNX model (1.2 MB) is baked into the image at
# /app/models/silero_vad_v6.onnx. The worker's default VAD_MODEL_PATH
# points there.

FROM rust:1.94-bookworm AS builder

RUN apt-get update && apt-get install -y cmake && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy both crates preserving the relative path structure that
# Cargo.toml's `path = "../ovp-pipeline"` expects.
COPY ovp-pipeline/ ovp-pipeline/
COPY ovp-worker/ ovp-worker/

WORKDIR /build/ovp-worker
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# ONNX Runtime shared library — required by the `ort` crate's `load-dynamic`
# feature for Silero VAD inference. Downloaded once at image build time.
# ort-sys 2.0.0-rc.12 targets ONNX Runtime 1.24. Using an older version
# (e.g. 1.19 or 1.23) causes a silent deadlock during model loading.
RUN curl -sL https://github.com/microsoft/onnxruntime/releases/download/v1.24.4/onnxruntime-linux-x64-1.24.4.tgz \
    | tar xz -C /opt/ \
    && rm -rf /opt/onnxruntime-linux-x64-1.24.4/include
ENV ORT_DYLIB_PATH=/opt/onnxruntime-linux-x64-1.24.4/lib/libonnxruntime.so

COPY --from=builder /build/ovp-worker/target/release/ovp-worker /usr/local/bin/ovp-worker

# Bake in the Silero VAD model so the container is self-contained.
COPY ovp-pipeline/models/silero_vad_v6.onnx /app/models/silero_vad_v6.onnx

WORKDIR /app

CMD ["ovp-worker"]
