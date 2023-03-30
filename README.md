# SUI Data Loader

Reads events from SUI RPC and pushes them into Apache Pulsar
- for now only subscription is supported
- events are batched before writing to pulsar

## Development
- clone repo
- run `./go setup`
- run `./go check` before commiting any changes(linting, formatting)
- run `cargo build` to compile the code or `cargo run` to run it
- recent protoc seems to be required by Pulsar
