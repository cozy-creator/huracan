use std::{
	cmp::Ordering,
	fmt::{Display, Formatter},
	io::Cursor,
	iter::zip,
	sync::atomic::{AtomicBool, Ordering::Relaxed},
};

use anyhow::Result;
use async_channel::Sender as ACSender;
use async_stream::stream;
use bson::{doc, Document};
use futures::Stream;
use futures_batch::ChunksTimeoutStreamExt;
use futures_util::SinkExt;
use macros::with_client_rotation;
use mongodb::{
	options::{ClientOptions, Compressor, ServerApi, ServerApiVersion},
	Database,
};
use pulsar::{Producer, Pulsar, TokioExecutor};
use rocksdb::{DBWithThreadMode, SingleThreaded};
use sui_sdk::{
	apis::ReadApi,
	error::SuiRpcResult,
	rpc_types::{
		ObjectChange as SuiObjectChange, SuiGetPastObjectRequest, SuiObjectData, SuiObjectDataOptions,
		SuiObjectResponse, SuiPastObjectResponse, SuiTransactionBlockResponseOptions, SuiTransactionBlockResponseQuery,
		TransactionBlocksPage,
	},
	SuiClient, SuiClientBuilder,
};
use sui_types::{
	base_types::{ObjectID, SequenceNumber, VersionNumber},
	digests::TransactionDigest,
	error::SuiObjectResponseError,
	messages_checkpoint::CheckpointSequenceNumber,
	query::TransactionFilter,
};
use tokio::{pin, sync::mpsc::Sender as TSender};

use crate::{
	_prelude::*,
	conf::{AppConfig, MongoConfig, RpcProviderConfig},
};

// sui now allows a max of 1000 objects to be queried for at once (used to be 50), at least on the
// endpoints we're using (try_multi_get_parsed_past_object, query_transaction_blocks)
const SUI_QUERY_MAX_RESULT_LIMIT: usize = 1000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Change {
	Created,
	Mutated,
	Deleted,
}

#[derive(Clone, Debug, Serialize, Deserialize, PulsarMessage)]
pub struct ObjectItem {
	pub cp:       CheckpointSequenceNumber,
	pub deletion: bool,
	pub id:       ObjectID,
	pub version:  SequenceNumber,
	pub bytes:    Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PulsarMessage)]
pub struct ObjectSnapshot {
	pub change:    Change,
	pub object_id: ObjectID,
	pub version:   SequenceNumber,
	pub object:    Option<Vec<u8>>,
}

impl ObjectSnapshot {
	fn from(change: SuiObjectChange) -> Option<(Self, String)> {
		use SuiObjectChange::*;
		let res = match change {
			Created { object_id, version, object_type, .. } => {
				(Self { change: Change::Created, object_id, version, object: None }, object_type.to_string())
			}
			Mutated { object_id, version, object_type, .. } => {
				(Self { change: Change::Mutated, object_id, version, object: None }, object_type.to_string())
			}
			Deleted { object_id, version, object_type, .. } => {
				(Self { change: Change::Deleted, object_id, version, object: None }, object_type.to_string())
			}
			_ => return None,
		};
		Some(res)
	}

	fn get_past_object_request(&self) -> SuiGetPastObjectRequest {
		SuiGetPastObjectRequest { object_id: self.object_id, version: self.version }
	}
}

#[derive(Clone)]
pub struct ClientPool {
	configs: Vec<RpcProviderConfig>,
	clients: Vec<Client>,
}

pub struct Client {
	id:      usize,
	config:  RpcProviderConfig,
	sui:     SuiClient,
	backoff: Option<(Instant, u8)>,
	reqs:    u64,
}

impl Clone for Client {
	// reset backoff and reqs
	fn clone(&self) -> Self {
		Self { id: self.id, config: self.config.clone(), sui: self.sui.clone(), backoff: None, reqs: 0 }
	}
}

impl Client {
	fn read_api(&self) -> &ReadApi {
		self.sui.read_api()
	}
}

impl ClientPool {
	pub async fn new(configs: Vec<RpcProviderConfig>) -> Result<Self> {
		let mut clients = Vec::with_capacity(configs.len());
		let mut self_ = Self { configs, clients };
		self_.clients.push(self_.make_client(0).await?);
		Ok(self_)
	}

	#[with_client_rotation]
	pub async fn get_latest_checkpoint_sequence_number(&mut self) -> SuiRpcResult<CheckpointSequenceNumber> {
		get_latest_checkpoint_sequence_number().await
	}

	#[with_client_rotation]
	pub async fn query_transaction_blocks(
		&mut self,
		query: SuiTransactionBlockResponseQuery,
		cursor: Option<TransactionDigest>,
		limit: Option<usize>,
		descending_order: bool,
	) -> SuiRpcResult<TransactionBlocksPage> {
		query_transaction_blocks(query.clone(), cursor, limit, descending_order).await
	}

	#[with_client_rotation]
	pub async fn get_object_with_options(
		&mut self,
		object_id: ObjectID,
		options: SuiObjectDataOptions,
	) -> SuiRpcResult<SuiObjectResponse> {
		get_object_with_options(object_id, options.clone()).await
	}

	#[with_client_rotation]
	pub async fn multi_get_object_with_options(
		&mut self,
		object_ids: Vec<ObjectID>,
		options: SuiObjectDataOptions,
	) -> SuiRpcResult<Vec<SuiObjectResponse>> {
		multi_get_object_with_options(object_ids.clone(), options.clone()).await
	}

	#[with_client_rotation]
	pub async fn try_get_parsed_past_object(
		&mut self,
		object_id: ObjectID,
		version: SequenceNumber,
		options: SuiObjectDataOptions,
	) -> SuiRpcResult<SuiPastObjectResponse> {
		try_get_parsed_past_object(object_id, version, options.clone()).await
	}

	#[with_client_rotation]
	pub async fn try_multi_get_parsed_past_object(
		&mut self,
		past_objects: Vec<SuiGetPastObjectRequest>,
		options: SuiObjectDataOptions,
	) -> SuiRpcResult<Vec<SuiPastObjectResponse>> {
		try_multi_get_parsed_past_object(past_objects.clone(), options.clone()).await
	}

	async fn make_client(&self, id: usize) -> Result<Client> {
		let config = self.configs[id].clone();
		let sui = SuiClientBuilder::default().build(&config.url).await?;
		Ok(Client { id, config, sui, backoff: None, reqs: 0 })
	}
}

pub async fn fullscan(
	cfg: &AppConfig,
	mut sui: ClientPool,
	pulsar: Pulsar<TokioExecutor>,
	rx_term: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
	let db = Arc::new(DBWithThreadMode::<SingleThreaded>::open_default(&cfg.rocksdbfile).unwrap());

	let mongo = {
		let mut client_options = ClientOptions::parse(&cfg.mongo.uri).await?;
		// use zstd compression for messages
		client_options.compressors = Some(vec![Compressor::Zstd { level: Some(cfg.mongo.zstdlevel) }]);
		client_options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
		let client = mongodb::Client::with_options(client_options)?;
		client.database(&cfg.mongo.db)
	};

	let checkpoint_max = sui.get_latest_checkpoint_sequence_number().await.unwrap() as usize;

	let default_num_workers = sui.configs.len();
	let num_step1_workers = cfg.workers.step1.unwrap_or(default_num_workers);
	// mpmc channel, as an easy way to balance incoming work from step 1 into multiple step 2 workers
	let (object_ids_tx, object_ids_rx) = async_channel::bounded(cfg.queuebuffers.step1out);
	// for the control channel, we want to add some blocking behavior in case the task acting
	// on the control messages falls too far behind -- we don't want to process major portions of the
	// chain without also storing info about our progress so we can resume about where we left off
	let (cp_control_tx, mut cp_control_rx) =
		tokio::sync::mpsc::channel(num_step1_workers * cfg.queuebuffers.cpcontrolfactor);
	// mostly we want to buffer up to mongo batch size items smoothly, assuming writes to mongo from a single writer will be fast enough
	let (mongo_tx, mut mongo_rx) = tokio::sync::mpsc::channel(cfg.mongo.batchsize * cfg.queuebuffers.mongoinfactor);

	// handle stopping gracefully
	let stop = Arc::new(AtomicBool::new(false));
	tokio::spawn({
		let stop = stop.clone();
		async move {
			let _ = rx_term.await;
			stop.store(true, Relaxed);
		}
	});

	// spawn as many step 1 workers as we have RPC server urls,
	// let each worker freely balance requests between them
	{
		let step_size = num_step1_workers;
		for partition in 0..num_step1_workers {
			tokio::spawn(fullscan_extract(
				cfg.clone(),
				checkpoint_max,
				step_size,
				partition,
				sui.clone(),
				db.clone(),
				stop.clone(),
				object_ids_tx.clone(),
				cp_control_tx.clone(),
			));
		}
	}

	// step 2 workers
	{
		let num_workers = cfg.workers.step2.unwrap_or(default_num_workers);
		for _ in 0..num_workers {
			tokio::spawn({
				let sui = sui.clone();
				let mut retries = make_producer(&cfg, &pulsar, "retries").await?;
				let object_ids_rx = object_ids_rx.clone();
				let mongo_tx = mongo_tx.clone();

				async move {
					let stream = transform(object_ids_rx, sui).await;
					let stream = stream! {
						for await (status, item) in stream {
							if let StepStatus::Err = status {
								retries.send(item).await.expect("failed to send retry message to pulsar!");
							} else {
								yield item;
							}
						}
					};
					// convert stream to tokio channel
					pin!(stream);
					while let Some(it) = stream.next().await {
						mongo_tx.send(it).await.expect("passing items from step 2 stream to mongo tokio channel");
					}
				}
			});
		}
	}

	// step 3: a single worker should be fine as we can run large, efficient batches
	let mut retries = make_producer(&cfg, &pulsar, "retries").await?;
	// transform tokio receiver to stream
	let stream = stream! {
		while let Some(it) = mongo_rx.recv().await {
			yield it;
		}
	};
	let stream = load(&cfg, stream, &mongo).await;
	let mut completions_left = HashMap::new();
	pin!(stream);
	loop {
		tokio::select! {
			Some((status, item)) = stream.next() => {
				let cp = item.cp;
				if let StepStatus::Err = status {
					retries.send(item).await.expect("failed to send retry message to pulsar!");
				}
				let v = completions_left.entry(cp).and_modify(|n| *n -= 1).or_insert(-1i64);
				if *v == 0 {
					mongo_checkpoint(&cfg, &mongo, cp).await;
					completions_left.remove(&cp);
				}
			},
			Some((cp, num_items)) = cp_control_rx.recv() => {
				let v = completions_left.entry(cp).and_modify(|n| *n += num_items as i64).or_insert(num_items as i64);
				if *v == 0 {
					mongo_checkpoint(&cfg, &mongo, cp).await;
					completions_left.remove(&cp);
				}
			},
			// if both branches return None, we're complete
			else => break,
		}
	}

	Ok(())
}

async fn make_producer(cfg: &AppConfig, pulsar: &Pulsar<TokioExecutor>, ty: &str) -> Result<Producer<TokioExecutor>> {
	Ok(pulsar
		.producer()
		// e.g. {persistent://public/default/}{prod}_{testnet}_{objects}_{retries}
		// braces added for clarity of discerning between the different parts
		.with_topic(&format!("{}{}_{}_{}_{}", cfg.pulsar.topicbase, cfg.env, cfg.net, cfg.mongo.collectionbase, ty))
		.build()
		.await?)
}

fn mongo_collection_name(cfg: &AppConfig, suffix: &str) -> String {
	format!("{}_{}_{}{}", cfg.env, cfg.net, cfg.mongo.collectionbase, suffix)
}

async fn mongo_checkpoint(cfg: &AppConfig, db: &Database, cp: CheckpointSequenceNumber) {
	let mut retries_left = cfg.mongo.retries;
	loop {
		if let Err(err) = db
			.run_command(
				doc! {
					// e.g. prod_testnet_objects_checkpoints
					"update": mongo_collection_name(&cfg, "_checkpoints"),
					"updates": vec![
						doc! {
							// FIXME how do we store a u64 in mongo? this will be an issue when the chain
							//		 has been running for long enough!
							"q": doc! { "_id": cp as i64 },
							"u": doc! { "_id": cp as i64 },
							"upsert": true,
						}
					]
				},
				None,
			)
			.await
		{
			warn!("failed saving checkpoint to mongo: {}", err);
			if retries_left > 0 {
				retries_left -= 1;
				continue
			}
			error!(error = ?err, "checkpoint {} fully completed, but could not save checkpoint status to mongo!", cp);
		}
		break
	}
}

async fn fullscan_extract(
	cfg: AppConfig,
	checkpoint_max: usize,
	step_size: usize,
	partition: usize,
	mut sui: ClientPool,
	db: Arc<DBWithThreadMode<SingleThreaded>>,
	stop: Arc<AtomicBool>,
	object_ids_tx: ACSender<ObjectItem>,
	cp_control_tx: TSender<(CheckpointSequenceNumber, u32)>,
) {
	// walk partitioned checkpoints range from newest to oldest
	for cp in (1..=checkpoint_max - partition).rev().step_by(step_size) {
		// do we need to stop?
		if stop.load(Relaxed) {
			break
		}
		// start fetching all tx blocks for this checkpoint
		let q = SuiTransactionBlockResponseQuery::new(
			Some(TransactionFilter::Checkpoint(cp as CheckpointSequenceNumber)),
			Some(SuiTransactionBlockResponseOptions::new().with_object_changes()),
		);
		let mut cursor = None;
		let mut retries_left = cfg.sui.step1retries;
		let mut num_objects = 0u32;

		loop {
			let page = sui.query_transaction_blocks(q.clone(), cursor, Some(SUI_QUERY_MAX_RESULT_LIMIT), true).await;
			match page {
				Ok(page) => {
					retries_left = cfg.sui.step1retries;
					for tx_block in page.data {
						if let Some(changes) = tx_block.object_changes {
							for change in changes {
								use SuiObjectChange::*;
								let (object_id, version, deleted) = match change {
									Created { object_id, version, .. } | Mutated { object_id, version, .. } => {
										(object_id, version, false)
									}
									Deleted { object_id, version, .. } => (object_id, version, true),
									_ => continue,
								};
								let k = object_id.as_slice();
								// known?
								if let None = db.get_pinned(k).unwrap() {
									// no, new one, so we mark it as known
									db.put(k, Vec::new()).unwrap();
									num_objects += 1;
									// send to step 2
									let send_res = object_ids_tx
										.send(ObjectItem {
											cp: cp as CheckpointSequenceNumber,
											deletion: deleted,
											id: object_id,
											version,
											bytes: Default::default(),
										})
										.await;
									if send_res.is_err() {
										// channel closed, consumers stopped
										return
									}
								}
							}
						}
					}
					if !page.has_next_page {
						// we're done with this cp
						// send control message about number of expected object tasks from this cp
						cp_control_tx.send((cp as CheckpointSequenceNumber, num_objects)).await.unwrap();
						break
					} else if page.next_cursor.is_none() {
						warn!("[[sui api issue?]] query_transaction_blocks({}, {:?}) page.has_next_page == true, but there is no page.next_cursor! continuing as if no next page!", cp, cursor);
						cp_control_tx.send((cp as CheckpointSequenceNumber, num_objects)).await.unwrap();
						break
					} else {
						cursor = page.next_cursor;
					}
				}
				Err(err) => {
					if retries_left == 0 {
						warn!(error = ?err, "Exhausted all retries fetching step 1 data, leaving checkpoint {} unfinished for this run", cp);
					}
					warn!(error = ?err, "There was an error reading object changes... retrying (retry #{}) after short timeout", retries_left);
					retries_left -= 1;
					tokio::time::sleep(Duration::from_millis(cfg.sui.step1retrytimeoutms)).await;
				}
			}
		}
	}
}

pub async fn extract<'a, P: FnMut(Option<TransactionDigest>, TransactionDigest) + 'a>(
	mut sui: ClientPool,
	mut rx_term: tokio::sync::oneshot::Receiver<()>,
	start_from: Option<TransactionDigest>,
	mut on_next_page: P,
) -> Result<impl Stream<Item = ObjectSnapshot> + 'a> {
	let q = SuiTransactionBlockResponseQuery::new(
		None,
		Some(SuiTransactionBlockResponseOptions::new().with_object_changes()),
	);
	let mut cursor = start_from;
	let mut skip_page = false;
	let mut retry_count = 0;

	Ok(stream! {
		loop {
			tokio::select! {
				page = sui.query_transaction_blocks(q.clone(), cursor, Some(SUI_QUERY_MAX_RESULT_LIMIT), false) => {
					match page {
						Ok(page) => {
							retry_count = 0;
							if !skip_page {
								for tx_block in page.data {
									if let Some(changes) = tx_block.object_changes {
										for change in changes {
											if let Some((change, _object_type)) = ObjectSnapshot::from(change) {
												// step 1 object skipping filters:
												// for now we want all objects, might handle these differently later

												// // skip objects of type "coin"
												// if object_type.starts_with("0x2::coin::Coin") {
												// 	continue
												// }
												yield change;
											}
										}
									}
								}
							}
							if page.next_cursor.is_none() {
								// TODO if we had an API that could give us the prev/next tx digests for any given tx digest
								//		we could solve this a little more elegantly!
								info!("no next page info from sui, will try to find the next page after a short timeout...");
								skip_page = true;
								tokio::time::sleep(Duration::from_millis(500)).await;
							} else {
								skip_page = false;
								on_next_page(cursor.clone(), page.next_cursor.unwrap());
								cursor = page.next_cursor;
							}
						},
						Err(err) => {
							warn!(error = ?err, "There was an error reading object changes... retrying (retry #{}) after short timeout", retry_count);
							retry_count += 1;
							tokio::time::sleep(Duration::from_millis(500)).await;
						}
					}
				}
				_ = &mut rx_term => break
			}
		}
	})
}

pub enum StepStatus {
	Ok,
	Err,
}

impl Display for StepStatus {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Self::Ok => f.write_str("Ok"),
			Self::Err => f.write_str("Err"),
		}
	}
}

fn parse_get_object_response(id: &ObjectID, res: SuiObjectResponse) -> Option<(VersionNumber, Vec<u8>)> {
	if let Some(err) = res.error {
		use SuiObjectResponseError::*;
		match err {
			Deleted { object_id, version, digest: _ } => {
				warn!(object_id = ?object_id, version = ?version, "object not available: object has been deleted");
			}
			NotExists { object_id } => {
				warn!(object_id = ?object_id, "object not available: object doesn't exist");
			}
			Unknown => {
				warn!(object_id = ?id, "object not available: unknown error");
			}
			DisplayError { error } => {
				warn!(object_id = ?id, "object not available: display error: {}", error);
			}
		};
		return None
	}
	if let Some(obj) = res.data {
		let mut bytes = Vec::with_capacity(4096);
		let bson = bson::to_bson(&obj).unwrap();
		bson.as_document().unwrap().to_writer(&mut bytes).unwrap();
		return Some((obj.version, bytes))
	}
	warn!(object_id = ?id, "neither .data nor .error was set in get_object response!");
	return None
}

pub async fn transform<'a, S: Stream<Item = ObjectItem> + 'a>(
	stream: S,
	mut sui: ClientPool,
) -> impl Stream<Item = (StepStatus, ObjectItem)> + 'a {
	// batch incoming items so we can amortize the cost of sui api calls,
	// but send them off one by one, so any downstream consumer (e.g. Pulsar client) can apply their
	// own batching logic, if necessary (e.g. Pulsar producer will auto-batch transparently, if configured)

	let stream = stream.chunks_timeout(50, Duration::from_millis(1_000));

	transform_batched(stream, sui).await
}

async fn transform_batched<'a, S: Stream<Item = Vec<ObjectItem>> + 'a>(
	stream: S,
	mut sui: ClientPool,
) -> impl Stream<Item = (StepStatus, ObjectItem)> + 'a {
	let query_opts = SuiObjectDataOptions {
		show_type:                 true,
		show_owner:                true,
		show_previous_transaction: true,
		show_display:              false,
		show_content:              true,
		show_bcs:                  true,
		show_storage_rebate:       true,
	};

	stream! {
		for await mut chunk in stream {
			// skip loading objects for 'delete' type changes, as we're just going to delete them from our working set anyway
			for item in chunk.drain_filter(|o| o.deletion) {
				yield (StepStatus::Ok, item);
			}
			let obj_ids = chunk.iter().map(|item| item.id).collect::<Vec<_>>();
			match sui.multi_get_object_with_options(obj_ids, query_opts.clone()).await {
				Err(err) => {
					warn!(error = format!("{err:?}"), "cannot fetch object data for one or more objects, retrying them individually");
					// try one by one
					// TODO this should be super easy to do in parallel, firing off the reqs on some tokio thread pool executor
					for mut item in chunk {
						match sui.get_object_with_options(item.id, query_opts.clone()).await {
							Err(err) => {
								error!(object_id = ?item.id, error = format!("{err:?}"), "individual fetch also failed");
								yield (StepStatus::Err, item);
							},
							Ok(res) => {
								if let Some((version, bytes)) = parse_get_object_response(&item.id, res) {
									item.version = version;
									item.bytes = bytes;
									yield (StepStatus::Ok, item);
								}
							}
						}
					}
				},
				Ok(objs) => {
					// XXX: relying on a possible Sui API implementation detail
					// the sui endpoint is implemented such that the response items are in the same
					// order as the input items, so we don't have to search or otherwise match them
					if objs.len() != chunk.len() {
						panic!("sui.multi_get_object_with_options() mismatch between input and result len!");
					}
					for (mut item, res) in zip(chunk, objs) {
						// TODO if we can't get object info, do we really want to skip indexing this change? or is there something more productive we can do?
						if let Some((version, bytes)) = parse_get_object_response(&item.id, res) {
							item.version = version;
							item.bytes = bytes;
							yield (StepStatus::Ok, item);
						}
					}
				}
			}
		}
	}
}

pub async fn load<'a, S: Stream<Item = ObjectItem> + 'a>(
	cfg: &'a AppConfig,
	stream: S,
	db: &'a Database,
) -> impl Stream<Item = (StepStatus, ObjectItem)> + 'a {
	let stream = stream.chunks_timeout(cfg.mongo.batchsize, Duration::from_millis(cfg.mongo.batchwaittimeoutms));
	// e.g. prod_testnet_objects
	let collection = mongo_collection_name(&cfg, "");

	stream! {
		for await chunk in stream {
			let mut retries_left = cfg.mongo.retries;
			loop {
				// for now mongo's rust driver doesn't offer a way to directly do bulk updates / batching
				// there's a high-level API only for inserting many, but not for updating or deleting many,
				// and neither for mixing all of those easily
				// but what it does provide is the generic run_command() method,
				let updates = chunk.iter().map(|item| {
					let v = item.version.to_string();
					let v_ = u64::from_str_radix(&v[2..], 16).unwrap();
					// FIXME our value range here is u64, but I can't figure out how to get a BSON repr of a u64?!
					let v_ = v_ as i64;
					if item.deletion {
						// we're assuming each object id will ever exist only once, so when deleting
						// we don't check for previous versions
						// we execute the delete, whenever it may come in, and it's final
						doc! {
							"q": doc! { "_id": item.id.to_string() },
							"u": doc! {
								"$set": {
									"_id": item.id.to_string(),
									"version": v,
									"version_": v_,
									"deleted": true,
								},
							},
							"upsert": true,
							"multi": false,
						}
					} else {
						// we will only upsert and object if this current version is higher than any previously stored one
						// (if the object has already been deleted, we still allow setting any other fields, including
						// any previously valid full object state... probably not needed, but also not incorrect)
						let mut c = Cursor::new(&item.bytes);
						doc! {
							"q": doc! { "_id": item.id.to_string() },
							// use an aggregation pipeline in our update, so that we can conditionally update
							// the version and object only if the previous version was lower than our current one
							"u": vec![doc! {
								"$set": {
									"_id": item.id.to_string(),
									// version_ must be added first, so that it's available in the next items in the pipeline
									// it has a more complex condition, so it's also added if the field doesn't exist yet
									// afterwards, the other fields can rely on it being present
									"version_": {"$cond": { "if": { "$or": [ { "$lt": [ "$version_", v_ ] }, { "$lte": [ "$version", None::<i32> ] } ] }, "then": v_, "else": "$version_" }},
									"version": {"$cond": { "if": { "$lt": [ "$version_", v_ ] }, "then": v.clone(), "else": "$version" }},
									"object": {"$cond": { "if": { "$lt": [ "$version_", v_ ] }, "then": Document::from_reader(&mut c).unwrap(), "else": "$object" }},
								},
							}],
							"upsert": true,
							"multi": false,
						}
					}
				}).collect::<Vec<_>>();
				let n = updates.len();
				let res = db.run_command(doc! {
					"update": &collection,
					"updates": updates,
				}, None).await;
				match res {
					Ok(res) => {
						// res: {n: i32, upserted: [{index: i32, _id: String}, ...], nModified: i32, writeErrors: [{index: i32, code: i32}, ...]}
						if res.get_i32("n").unwrap() != n as i32 {
							panic!("failed to execute at least one of the upserts: {:#?}", res.get_array("writeErrors").unwrap());
						}
						let inserted = if let Ok(upserted) = res.get_array("upserted") {
							upserted.len()
						} else {
							0
						};
						let modified = res.get_i32("nModified").unwrap();
						let missing = n - (inserted + modified as usize);
						let missing_info = if missing > 0 {
							format!(" // {} items without effect!", missing)
						} else {
							String::new()
						};
						info!("|> mongo: {} total / {} updated / {} created{}", n, modified, inserted, missing_info);
						for item in chunk {
							yield (StepStatus::Ok, item);
						}
						break
					}
					Err(err) => {
						// the whole thing failed; retry a few times, then assume it's a bug
						if retries_left == 0 {
							panic!("final attempt to run mongo batch failed: {:?}", err);
						}
						warn!("error running mongo batch, will retry {} more times: {:?}", retries_left, err);
						retries_left -= 1;
					}
				}
			}
		}
	}
}
