FROM debian:bullseye
ARG BIN
RUN apt-get update && apt-get -q -y install ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=binaries $BIN .
RUN chmod +x $BIN
RUN ln -s $BIN /usr/local/bin/binary
COPY ./config.yaml .
ENTRYPOINT ["binary"]
