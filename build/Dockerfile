FROM debian:bookworm-20260518-slim

ARG BIN_PATH=target/debug/winnow

WORKDIR /app

COPY ${BIN_PATH} /usr/local/bin/winnow
COPY build/cluster.json ./cluster.json

CMD ["winnow"]
