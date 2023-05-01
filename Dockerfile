# set when building by using --build-arg BUILD_TYPE="..."
# set to "--release" to produce release builds
# set to "" to produce debug builds
#ARG BUILD_TYPE="--release"
ARG BUILD_TYPE=""

FROM rustlang/rust:nightly as builder
RUN apt-get update && apt-get -q -y install clang protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --bin $BUILD_TYPE sui-indexer


FROM debian:bullseye as runtime
WORKDIR /app
COPY --from=builder /app/target/*/sui-indexer /usr/local/bin
COPY ./config.yaml .
ENTRYPOINT ["sui-indexer", "all"]
