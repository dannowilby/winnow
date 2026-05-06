#!/usr/bin/env just --justfile

# build the server and wasm binaries in release mode
build:
    cargo build -p server --release

# build the server and wasm binaries
dev-build:
    cargo build -p server

cli: dev-build
    cargo run -p server --bin mapreduce_cli

build-components:
    cargo build -p map --target=wasm32-wasip2 --release

server: dev-build
    cargo run -p server --bin mapreduce_bin

test: dev-build
    cargo test -p server -- --no-capture

check:
    cargo check --workspace