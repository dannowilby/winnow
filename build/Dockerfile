# syntax=docker/dockerfile:1
#
# Build context is the repository root (see build/docker-compose.yaml), so all
# COPY paths below are relative to the repo root.

# ---- build stage ----
FROM rust:1.96-bookworm AS builder

WORKDIR /src

# Only copy the sources needed to build the server binary. The whole workspace
# manifest must be present (cargo resolves every member), but the wasm
# components are uploaded to the server at runtime, not baked in.
COPY Cargo.toml Cargo.lock ./
COPY server ./server
COPY components ./components
COPY wit ./wit

RUN cargo build -p server --bin mapreduce_bin

# ---- runtime stage ----
FROM debian:bookworm-20260518-slim

WORKDIR /app

COPY --from=builder /src/target/debug/mapreduce_bin /usr/local/bin/mapreduce_bin
COPY build/cluster.json ./cluster.json

CMD ["mapreduce_bin"]
