# SUI Data Loader

Reads events from SUI RPC and pushes them into Apache Pulsar
- for now only subscription is supported
- events are batched before writing to pulsar

## Use
 - Extract: `cargo run -- --config-path ./config.yaml --print-config extract`
    Pulls for object changes via SUI RPC and pushes into Pulsar

 - Transform: `cargo run -- --config-path ./config.yaml --print-config transform`
    Reads from Pulsar, enriches with object data via SUI RPC and pushes into Pulsar

 - Load: `cargo run -- --config-path ./config.yaml --print-config load`
    Reads from Pulsar and writes to Mongo

## Development
- clone repo
- run `./go setup`
- run `./go check` before commiting any changes(linting, formatting)
- run `cargo build` to compile the code
- recent protoc seems to be required by Pulsar
- if pulsar authentication is required you can create a local `.env` file and specify `APP_PULSAR_TOKEN` or set this environment variable directly
- generally any config setting can be overriden with an environment variable, sections are separated by `_ ` and env variable should start with `APP_`
