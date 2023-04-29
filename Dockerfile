# set to "--release" to produce release builds
# set to "" to produce debug builds
ARG BUILD_TYPE="--release"

FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app
RUN rustup toolchain install nightly

# caches info about our deps in recipe.json
FROM chef AS planner
COPY . .
RUN cargo +nightly chef prepare --recipe-path recipe.json

FROM chef AS builder
# install system deps we need to be able to build our deps and/or binary
RUN apt-get update && apt-get -q -y install clang protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
# build just the deps
RUN cargo +nightly chef cook ${BUILD_TYPE} --recipe-path recipe.json
# copy and build main application
COPY . .
RUN cargo +nightly build ${BUILD_TYPE} --bin sui-indexer

# any base image with libc works (not alpine and others that would require a static binary)
FROM debian:buster-slim AS runtime
WORKDIR /app
COPY --from=builder /app/target/*/sui-indexer /usr/local/bin
ENTRYPOINT ["/usr/local/bin/sui-indexer"]