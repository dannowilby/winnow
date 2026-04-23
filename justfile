#!/usr/bin/env just --justfile

# build the server and wasm binaries in release mode
build:
    cargo build -p server --release
    cargo build -p mapper --target=wasm32-wasip2 --release

# build the server and wasm binaries
dev-build:
    cargo build -p server
    cargo build -p read --target=wasm32-wasip2 --release
    cargo build -p map --target=wasm32-wasip2 --release
    cargo build -p partition --target=wasm32-wasip2 --release
    cargo build -p reduce --target=wasm32-wasip2 --release

test: dev-build
    cargo test -p server -- --no-capture

check:
    cargo check --workspace