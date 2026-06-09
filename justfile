#!/usr/bin/env just --justfile

# build the server and wasm binaries in release mode
build:
    cargo build -p server --release

# build the server and wasm binaries
dev-build:
    cargo build -p server

cli: dev-build
    cargo run -p server --bin mapreduce_cli

coverage:
    cargo llvm-cov -p server

build-components:
    cargo build -p read --target=wasm32-wasip2 --release
    cargo build -p map --target=wasm32-wasip2 --release
    cargo build -p reduce --target=wasm32-wasip2 --release
    cargo build -p partition --target=wasm32-wasip2 --release

server: dev-build
    cargo run -p server --bin mapreduce_bin

# build the host binary, then bake it into the cluster image (no in-container compile)
build-local-cluster: dev-build
    cd build && docker compose build && cd ../

run-local-cluster: build-local-cluster
    cd build && docker compose up && cd ../

test: dev-build
    cargo test -p server -- --no-capture

check:
    cargo check --workspace