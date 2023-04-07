# SUI Data Loader

Reads events from SUI RPC and pushes them into Apache Pulsar
- for now only subscription is supported
- events are batched before writing to pulsar

## Modes
 - Object change loader: `cargo run -- load-object-changes --config ./config.yaml --print-config`
 - Object loader/enricher: `cargo run -- load-objects --config ./config.yaml --print-config`

## Development
- clone repo
- run `./go setup`
- run `./go check` before commiting any changes(linting, formatting)
- run `cargo build` to compile the code
- recent protoc seems to be required by Pulsar
- if pulsar authentication is required you can create a local `.env` file and specify `APP_PULSAR_TOKEN` or set this environment variable directly
- generally any config setting can be overriden with an environment variable, sections are separated by `_ ` and env variable should start with `APP_`
