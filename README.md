# Sui Object Extractor
Extracts all objects from Sui using RPC calls.

## Extraction Implementation Overview
Data is extracted by loading checkpoints. Each checkpoint contains multiple transactions. Each transaction may contain object updates - new objects, deleted objects, or updated objects. We leverage Tokio to concurrently extract this data in multiple threads.

### Initialization Overview
Upon initialization, the app does several things:
1. Connect to MongoDB and determine the last checkpoint it has read. If this is the first run, it will begin at the newest checkpoint and work toward the oldest checkpoint.
2. Connect to one or more RPC servers, each of which is used round-robin for RPC calls.
3. Create Tokio threads to handle concurrent RPC calls for each checkpoint.
4. Create a RocksDB database file, which maintains state for each Tokio thread. Because each checkpoint contains multiple transactions, and each transaction contains multiple object changes, we use multiple Tokio threads to handle each checkpoint. RocksDB is the persistent storage layer to allow several Tokio threads to determine which checkpoints and transactions have been processed.
5. Create a log file (if configured to do so). Otherwise, logs go to stdout.
6. Create workers - "step 1" workers fetch checkpoint data, "step 2" workers load
7. Create Pulsar topics. Each thread has its own Pulsar topic. The thread will only write to Pulsar when it fails to extract data via RPC. It will retry 


### Runtime Overview
Using RPC calls, the app polls for new checkpoints, fetches all transactions for each checkpoint, and then fetches all object changes for each transaction. There is a distinct Tokio channel called the 'control channel' which streams metadata about each stage of the data processing pipeline, ensuring integrity and that all transaction blocks and objects are ingested. It also tracks ingest latency. 

The app will automatically detect when there are several checkpoints in the backlog to index. This effectively triggers a backfilling operation which launches more ingest threads. This option can be disabled via  Because multiple workers could be ingesting the same data, the app maintains a list of in-flight object IDs as a key/value store in RocksDB. While transaction blocks are scanned, RocksDB queries are issued on each object ID, and skipped if there is a match. This ensures that only one Tokio worker is handling an object change and avoids duplicate CRUD operations to downstream systems.
#### Step 1 - Checkpoint and Transaction Block Crawling
Initialized in the `spawn_livescan()` function. Spawns one or more Tokio workers based on configuration option `livescan.workers.checkpoint`. When an un-indexed checkpoint is polled, the app paginates through all transaction blocks contained in the checkpoint.    
#### Step 2 - Fetch Complete Object Data
Because transaction blocks only contain object IDs for affected objects, additional RPC calls must be issued to fetch the complete object data.
#### Step 3 - CRUD Object Data to MongoDB
## 
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

Generally any config value in `config.yaml` can be overridden with an environment variable.
Nested values are separated by `_` and env variables should start with `APP_` to be considered.
For example, to override the `sui.api.http` YAML value, use `APP_SUI_API_HTTP=...`.
