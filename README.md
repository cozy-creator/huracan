# SUI Data Loader

Reads object change events from SUI RPC, fetches the full objects at the given version, filters what we're interested in, writes objects to MongoDB.
Currently only a single-threaded mode is implemented, where all the work happens in a stream-based local pipeline inside a single process.
All Sui events are processed

## Use
Create a .env file in this project's root dir. Add your MongoDB URL in with username + password like this:

`APP_MONGO_URI=mongodb+srv://username:password@sui-testnet.7b6tqsn.mongodb.net/?retryWrites=true&w=majority`

Then run:

```bash
cargo run -- all [--start-from <tx digest to start or resume from>] [--no-mongo]
```

Use `--no-mongo` if you don't want to start filling a collection.
If you don't use that argument and thus write to MongoDB, ensure you're not writing to the same collection as another process,
e.g. by updating the `mongo` config in `config.yaml`, or by overriding (parts of) it via `.env`, like so:

```dotenv
APP_MONGO_OBJECTS_COLLECTION=my_objects_collection
```

This is so that multiple running processes do not interfere with each other. Even though multiple runs with the same code
and configuration will eventually produce the same database state.

## Environment-based configuration

Generally any config value in `config.yaml` can be overriden with an environment variable.
Nested values are separated by `_` and env variables should start with `APP_` to be considered.
For example, to override the `sui.api.http` YAML value, use `APP_SUI_API_HTTP=...`.
