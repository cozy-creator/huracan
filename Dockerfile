# set when building by using --build-arg BUILD_TYPE="..."
# set to "--release" to produce release builds
# set to "" to produce debug builds
ARG BUILD_TYPE="--release"

FROM rustlang/rust:nightly
RUN apt-get update && apt-get -q -y install clang protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --bin $BUILD_TYPE sui-indexer
RUN cp target/*/sui-indexer /usr/local/bin
ENTRYPOINT ["sui-indexer", "all"]
