# SUI Data Loader

Reads events from SUI RPC and pushes them into Apache Pulsar
- for now only subscription is supported
- events are batched before writing to pulsar

## Use
Ensure config.yaml is located in root directory of this project. Most of the CLI options are not currently working.
Also ensure that all API keys, etc are configured in config.yaml.

- Extract: cargo run -- extract
    Pulls for object changes via SUI RPC and pushes into Pulsar
- Transform: cargo run -- transform
    Reads from Pulsar, enriches with object data via SUI RPC and pushes into Pulsar
- Load: cargo run -- load
    Reads from Pulsar and writes to Mongo

## Development
- clone repo
- run `./go setup`
- run `./go check` before commiting any changes(linting, formatting)
- run `cargo build` to compile the code
- recent protoc seems to be required by Pulsar
- if pulsar authentication is required you can create a local `.env` file and specify `APP_PULSAR_TOKEN` or set this environment variable directly
- generally any config setting can be overriden with an environment variable, sections are separated by `_ ` and env variable should start with `APP_`
