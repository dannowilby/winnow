#!/usr/bin/env just --justfile

# build the server
build-server *FLAGS:
    cargo build -p server {{FLAGS}}

# build wasm components in release
build-components:
    cargo build -p read --target=wasm32-wasip2 --release
    cargo build -p map --target=wasm32-wasip2 --release
    cargo build -p reduce --target=wasm32-wasip2 --release
    cargo build -p partition --target=wasm32-wasip2 --release

# build the server and wasm binaries
build *FLAGS: (build-server FLAGS) build-components

# run the cli to start a job
cli *FLAGS:
    cargo run -p server --bin mapreduce_cli -- {{FLAGS}}

coverage:
    cargo llvm-cov nextest -p server


run-local-cluster: build
    docker compose -f build/docker-compose.yaml build
    docker compose -f build/docker-compose.yaml up

fmt:
    cargo fmt --all -- --check

clippy:
    cargo clippy -- -D warnings