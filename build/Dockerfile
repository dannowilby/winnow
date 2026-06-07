FROM debian:bookworm-20260518-slim

ARG BIN_PATH=target/debug/mapreduce_bin

WORKDIR /app

COPY ${BIN_PATH} /usr/local/bin/mapreduce_bin
COPY build/cluster.json ./cluster.json

CMD ["mapreduce_bin"]
