# Multi-stage Dockerfile for chronicle-worker.
#
# Build context MUST be the parent directory containing both chronicle-worker/
# and chronicle-pipeline/, because chronicle-worker depends on chronicle-pipeline via a
# relative path (../chronicle-pipeline in Cargo.toml).
#
# Build:
#   cd /home/alex  # or wherever both dirs live side by side
#   docker build -f chronicle-worker/Dockerfile -t chronicle-worker:dev .
#
# The Silero VAD ONNX model (1.2 MB) is baked into the image at
# /app/models/silero_vad_v6.onnx. The worker's default VAD_MODEL_PATH
# points there.

FROM rust:1.94-bookworm AS builder

RUN apt-get update && apt-get install -y cmake && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy both crates preserving the relative path structure that
# Cargo.toml's `path = "../chronicle-pipeline"` expects.
COPY chronicle-pipeline/ chronicle-pipeline/
COPY chronicle-worker/ chronicle-worker/

WORKDIR /build/chronicle-worker
RUN cargo build --release

FROM debian:bookworm-slim

ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# ONNX Runtime shared library — required by the `ort` crate's `load-dynamic`
# feature for Silero VAD inference. Downloaded once at image build time.
# ort-sys 2.0.0-rc.12 targets ONNX Runtime 1.24. Using an older version
# (e.g. 1.19 or 1.23) causes a silent deadlock during model loading.
#
# Architecture must match the runtime host. Loading the x64 .so on an
# aarch64 host ALSO produces a silent deadlock (no dlopen error surfaces
# up to the ort crate).
RUN set -e; \
    case "${TARGETARCH:-amd64}" in \
      amd64) ORT_ARCH=x64 ;; \
      arm64) ORT_ARCH=aarch64 ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -sL "https://github.com/microsoft/onnxruntime/releases/download/v1.24.4/onnxruntime-linux-${ORT_ARCH}-1.24.4.tgz" \
      | tar xz -C /opt/ \
    && rm -rf "/opt/onnxruntime-linux-${ORT_ARCH}-1.24.4/include" \
    && ln -s "/opt/onnxruntime-linux-${ORT_ARCH}-1.24.4" /opt/onnxruntime
ENV ORT_DYLIB_PATH=/opt/onnxruntime/lib/libonnxruntime.so

COPY --from=builder /build/chronicle-worker/target/release/chronicle-worker /usr/local/bin/chronicle-worker

# Bake in the Silero VAD model so the container is self-contained.
COPY chronicle-pipeline/models/silero_vad_v6.onnx /app/models/silero_vad_v6.onnx

WORKDIR /app

CMD ["chronicle-worker"]
