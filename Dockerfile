# set to "--release" to produce release builds
# set to "" to produce debug builds
ARG BUILD_TYPE="--release"

FROM clux/muslrust:stable AS chef
USER root
WORKDIR /app
ENV CARGO_BUILD_TARGET=x86_64-unknown-linux-musl
RUN rustup toolchain install nightly && rustup default nightly
RUN rustup target add x86_64-unknown-linux-musl
RUN cargo install cargo-chef

# caches info about our deps in recipe.json
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
# install system deps we need to be able to build our deps and/or binary
RUN apt-get update && apt-get -q -y install clang protobuf-compiler libssl-dev && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
# build just the deps
RUN cargo chef cook ${BUILD_TYPE} --recipe-path recipe.json
# copy and build main application
COPY . .
RUN cargo build ${BUILD_TYPE} --bin sui-indexer

# any base image with libc works (not alpine and others that would require a static binary)
FROM alpine AS runtime
# install runtime deps
RUN apk add openssl-dev
WORKDIR /app
COPY --from=builder /app/target/${CARGO_BUILD_TARGET}/*/sui-indexer /usr/local/bin
ENTRYPOINT ["/usr/local/bin/sui-indexer"]