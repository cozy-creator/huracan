FROM rust:1.67 as builder
WORKDIR /usr/src/huracan-graphql
COPY ./Cargo.toml .
COPY ./src ./src
run cargo install --path .

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y ca-certificates build-essential pkg-config cmake tzdata && rm -rf /var/lib/apt/lists/*
EXPOSE 8000
COPY --from=builder /usr/local/cargo/bin/huracan-graphql /usr/local/bin/huracan-graphql
CMD ["huracan-graphql"]
