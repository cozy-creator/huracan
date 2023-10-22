FROM rust:1.67 as builder
WORKDIR /usr/src/huracan-indexer
run apt install git -y
COPY ./Cargo.toml .
COPY ./src ./src
run ls /usr/src/macros/
run cargo build

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y ca-certificates build-essential pkg-config cmake tzdata && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/huracan-indexer /usr/local/bin/huracan-indexer
CMD ["huracan-indexer"]
