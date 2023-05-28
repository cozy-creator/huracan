FROM debian:bullseye
ARG BIN
RUN apt-get update && apt-get -q -y install ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=binaries $BIN /usr/local/bin
COPY ./config.yaml .
ENTRYPOINT ["$BIN"]
