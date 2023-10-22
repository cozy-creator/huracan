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
6. Create Tokio channels and workers. Each step of the pipeline relies on Tokio channels to manage data and facilitate backpressure. 
7. Create Pulsar topics. Each thread has its own Pulsar topic. The thread will only write to Pulsar when it fails to extract data via RPC.

### Runtime Overview
Using RPC calls, the app polls for new checkpoints, fetches all transactions for each checkpoint, and then fetches all object changes for each transaction. There is a distinct Tokio channel called the 'control channel' which streams metadata about each stage of the data processing pipeline, ensuring integrity and that all transaction blocks and objects are ingested. It also tracks ingest latency.

#### Operation Modes
- livescan: This default mode polls for the latest checkpoint and gradually traverses backward.
- backfill: This mode is enabled automatically when livescan is lagging by a certain number of checkpoints. The threshold is configured as `backfillthreshold`. This will trigger many more Tokio threads to be opened for RPC calls and MongoDB CRUD operations. This can cause crashes if the system cannot handle the input. There are several options available to help tune the overall throughput.

The app will automatically detect when there are several checkpoints in the backlog to index. This effectively triggers a backfilling operation which launches more ingest threads. Because multiple workers could be ingesting the same data, the app maintains a list of in-flight object IDs as a key/value store in RocksDB. While transaction blocks are scanned, RocksDB queries are issued on each object ID, and skipped if there is a match. This ensures that only one Tokio worker is handling an object change and avoids duplicate CRUD operations to downstream systems.
#### Step 1 - Checkpoint and Transaction Block Crawling
Initialized in the `spawn_livescan()` function. Spawns one or more Tokio workers based on configuration option `livescan.workers.checkpoint`. When an un-indexed checkpoint is polled, the app paginates through all transaction blocks contained in the checkpoint.    
#### Step 2 - Fetch Complete Object Data
Because transaction blocks only contain object IDs for affected objects, additional RPC calls must be issued to fetch the complete object data. In livescan
#### Step 3 - CRUD Object Data to MongoDB
Sui Object updates are published to MongoDB. If MongoDB cannot handle the incoming operations, the app will crash and you will need to detune the pipeline settings. The Rust code seems to extract data more quickly than MongoDB can keep up, if the configuration is too aggressive. If you see random crashes, this is a likely cause.

# GraphQL Webserver
Sui Object data that is loaded into MongoDB with the Sui Object Indexer is accessible via a GraphQL API. You may also queries MongoDB directly, if you so choose. All fields in the objects - including nested fields - are accessible via GraphQL. Unlike the Sui Core RPC and Indexing APIs, which only store the BCS of Sui objects, you can filter, sort, and run other queries using the fields inside your Sui objects.
- Located in `server` directory of the repo.
- Example queries are located in `example-queries` folder of the repo.
- We strongly recommend creating indices on critical fields used in your queries to improve performance and cost optimization of MongoDB. Examples are included in `example-queries`.
- Cost reduction and query speed can be achieved by narrowing down the number of objects you load into MongoDB via the Sui Object Indexer. For example, if you are only working with data from one or a handful of Sui Move Packages, you can configure the indexer to exclusivley load those items. This is documented in `config.yaml` in the `main` directory.

## Configuration and Deployment
Default values are configured in `config.yaml` and can be overriden with environment variables. All values are well documented inside `config.yaml`.
Generally any config value in `config.yaml` can be overridden with an environment variable.
Nested values are separated by `_` and env variables should start with `APP_` to be considered.
Example:
`APP_MONGO_URI=mongodb+srv://username:password@sui-testnet.7b6tqsn.mongodb.net/?retryWrites=true&w=majority`

### Whitelisting and Blacklisting by Sui Move Package ID
- Cost reduction and query speed can be achieved by narrowing down the number of objects you load into MongoDB via the Sui Object Indexer. For example, if you are only working with data from one or a handful of Sui Move Packages, you can configure the indexer to exclusivley load those items. This is documented in `config.yaml`.
- You may alternatively blacklist package IDs rather than whitelist.

### Prerequisites
1. MongoDB - We suggest MongoDB Atlas, but you may manage your own open source database instead.
2. InfluxDB - We suggest InfluxDB Cloud, but you may manage your own open source database instead.
3. Pulsar - We suggest StreamNative Cloud, but you may manage your own open source cluster instead.

### Cargo Install on Ubuntu 20.04 LTS (The Hard Way)
1. Clone repo into directory of your choice.
2. Configure `config.yaml`. All parameters are documented inside the file.
3. Ensure `pulsar-credentials.json` is located in correct directory. Default is `/opt/pulsar-credentials.json`. If using StreamNative Cloud, you can generate and download this file in the web UI.
4. Create MongoDB user and database. These must match the parameters in `config.yaml`. This is done in the UI in MongoDB Atlas. If you are using MongoSH, there are example commands in `example-queries/mongodb/mongosh-setup-examples`.
5. Create InfluxDB bucket and token. This is done in the web UI whether you are using InfluxDB Cloud or a self-managed, open-source database.
6. Open the port for the GraphQL web service. `sudo ufw allow 8000`
8. Install indexer application: Move to the `main` directory of the repo and run `cargo install --path .` This will create a new executable called `indexer`. This can take a long time to build Sui dependencies.
9. Start indexer in background: `setsid indexer`
10. Install GraphQL webserver application: Move to the `server` directory of the repo and run `cargo install --path .`
11. Start GraphQL webserver in background: `setsid`

### Tips for Running on Ubuntu 20.04 LTS
- If you are only running the indexer and webserver, a fairly small server will suffice (4gb RAM + 4 CPU cores). You may need a larger server to facilitate a large number of queries or aggressive indexing settings.
- If you used `setsid` to launch the apps, you can use `htop` to see the process info.
- Logs are located at `/var/log/indexer.log`

### Fly.io running a Docker Container (The Easy Way)
- Fly is a hosting platform that automatically builds, deploys, and runs Docker containers. You can also manage secrets like API keys and databse credentials in Fly.io. We have been able to host each Rust app on Fly for around $25 / month each.
- Note that you will still need to use external services for items in the `Prerequisites` section of this document.
- Each repo `main` (Indexer) and `server` (GraphQL API) contain an example Fly.io configuration file. The instance sizes and other configuration options are a great starting point for both services. You can find the most up-to-date instrucctions on the Fly.io website. https://fly.io/docs/apps/launch/
