FROM ubuntu:latest
ARG BIN
RUN apt-get update -q && apt-get install -q -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=binaries $BIN .
RUN ln $BIN binary
RUN chmod +x binary
COPY ./config.yaml .
ENTRYPOINT ["./binary"]