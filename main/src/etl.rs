use std::{
	collections::{btree_map::OccupiedError, BTreeMap},
	fmt::{Display, Formatter},
	io::Cursor,
	sync::atomic::{AtomicU16, Ordering::Relaxed},
	vec::IntoIter,
	iter::zip,
};
use anyhow::Result;
use async_channel::{Receiver as ACReceiver, Sender as ACSender};
use async_stream::stream;
use bson::{doc, Document};
use chrono::Utc;
use futures::Stream;
use futures_batch::ChunksTimeoutStreamExt;
use influxdb::InfluxDbWriteable;
use mongodb::{Database, options::FindOneOptions};
use pulsar::{Pulsar, TokioExecutor};
use rocksdb::{DBWithThreadMode, SingleThreaded};
use sui_sdk::rpc_types::{
	SuiObjectDataOptions, SuiTransactionBlockResponseOptions, SuiTransactionBlockResponseQuery, TransactionFilter,
};
use sui_types::{
	base_types::{ObjectID, SequenceNumber, TransactionDigest},
	messages_checkpoint::CheckpointSequenceNumber,
};
use tokio::{
	pin,
	sync::mpsc::{Receiver as TReceiver, Sender as TSender, UnboundedReceiver, UnboundedSender},
	task::JoinHandle,
};

use crate::{
	_prelude::*,
	client,
	client::{ClientPool, parse_get_object_response},
	conf::{AppConfig, PipelineConfig},
	ctrl_c_bool, mongo,
	mongo::{Checkpoint, mongo_checkpoint},
	utils::make_descending_ranges
};
use crate::conf::get_influx_singleton;
use crate::influx::{
	get_influx_timestamp_as_milliseconds,
	InsertObject,
	MissingObject,
	ModifiedObject,
	write_metric_rpc_error,
	write_metric_rpc_request,
	write_metric_mongo_write_error
};


// sui now allows a max of 1000 objects to be queried for at once (used to be 50), at least on the
// endpoints we're using (try_multi_get_parsed_past_object, query_transaction_blocks)
const SUI_QUERY_MAX_RESULT_LIMIT: usize = 1000;

// Our internal representation of a Sui object change. The `bytes` property is left empty before we fetch the full object data.
// This is the final output from the checkpoint/transaction block crawl. It is published to the object stream to queue an RPC lookup of the full object data.
// After the object data lookup, `bytes` is populated and this struct is queued for CRUD into downstream systems.
#[derive(Clone, Debug, Serialize, Deserialize, PulsarMessage)]
pub struct ObjectItem {
	pub cp:            CheckpointSequenceNumber,
	pub deletion:      bool,
	pub id:            ObjectID,
	pub version:       SequenceNumber,
	pub ts_sui:        Option<u64>,
	pub ts_first_seen: u64,
	pub ingested_via:  IngestRoute,
	pub bytes:         Vec<u8>,
}

// We track the last pipeline an ObjectItem has been through.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum IngestRoute {
	// This ObjectItem was generated in `do_poll()` and does not yet contain full object data `bytes`.
	Poll,
	// This ObjectItem was generated in a Livescan pipeline.
	Livescan,
	// This ObjectItem was generated in a Backfill pipeline.
	Backfill,
}

// Ensure each data extraction step is successful. If a step is Err, it will be placed in the retry pipeline.
#[derive(Debug)]
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

// This is the entrypoint when environment variable BACKFILL_ONLY = true. This allows us to begin a highly parallel backfill starting at a specific checkpoint.
pub async fn run_backfill_only(cfg: &AppConfig, start_checkpoint: Option<u64>) -> Result<()> {
	let sui = cfg.sui().await?;
	let pulsar = crate::pulsar::create(&cfg).await?;
	let (_, handle) = spawn_backfill_pipeline(&cfg, &cfg.backfill, sui, pulsar, start_checkpoint).await?;
	handle.await?;
	Ok(())
}

// This is the default operation mode and will initialize a livescan with the most recent Sui checkpoint.
// If our indexer is behind by "backfillthreshold", it will also initialize a separate backfill pipeline.
pub async fn run(cfg: &AppConfig) -> Result<()> {
	info!("ExtractionInfo: Initializing run().");
	let mut sui = cfg.sui().await?;
	let pulsar = crate::pulsar::create(&cfg).await?;
	let stop = ctrl_c_bool();
	let pause_livescan = Arc::new(AtomicU16::new(0));

	// Initialize livescan.
	let (mut poll_livescan_items, _poll_observed_cps) = spawn_checkpoint_poll(cfg, sui.clone(), pause_livescan.clone()).await;

	// TODO: These unbounded channels could be the source of the memory leak.
	let (livescan_items_tx, livescan_items_rx) = async_channel::unbounded();
	let (poll_livescan_items_transactions_tx, mut poll_livescan_items_transactions) = tokio::sync::mpsc::unbounded_channel();

	// Purpose of poll_livescan_items stream:
	// 1) Pass on as normal input to livescan pipeline immediately.
	// 2) Pass all seen transaction digests on to another stream.
	tokio::spawn({
		let livescan_items_tx = livescan_items_tx.clone();
		async move {
			while let Some(it) = poll_livescan_items.next().await {
				// doing this first because we can copy the digest and then move the full value
				if let Some(tx) = &it.0 {
					if poll_livescan_items_transactions_tx.send(*tx).is_err() {
						break
					}
				}
				// copy over to normal livescan pipeline input channel
				if livescan_items_tx.send(it).await.is_err() {
					break
				}
			}
		}
	});

	// rest of the livescan pipeline
	let (livescan_cp_control_tx, livescan_handle) =
		spawn_pipeline_tail(cfg.clone(), cfg.livescan.clone(), sui.clone(), pulsar.clone(), livescan_items_rx.clone())
			.await?;

	// observe checkpoints flow:
	// if we fell behind too far, we focus on backfilling until caught up:
	// spawn scan + throttle livescan work
	// else ensure completion of latest checkpoints
	tokio::spawn({
		let stop = stop.clone();
		let cfg = cfg.clone();

		// load latest completed checkpoint
		// if this is too far back
		// run backfill, potentially throttle livescan work
		// also remember that checkpoint as the last one to use for mini-scans
		// loop:
		// 	run livescan for any checkpoints since last livescan relevant

		async move {
			let mongo = cfg.mongo.client(&cfg.livescan.mongo).await.unwrap();
			let coll = mongo.collection::<Checkpoint>(&mongo::mongo_collection_name(&cfg, "_checkpoints"));

			// get initial highest checkpoint that we'll use for starting our "livescan" strategy
			let start_cp_for_offset = loop {
				let cp = match sui.get_latest_checkpoint_sequence_number().await {
					Ok(cp) => cp as u64,
					Err(e) => {
						error!("ExtractionError: Failed getting latest checkpoint: {:?}", e);
						continue
					}
				};
				break cp
			};
			// run first livescan from last known completed checkpoint
			let mut last_livescan_cp = coll
				.find_one(None, FindOneOptions::builder().sort(doc! {"_id": -1}).build())
				.await
				.unwrap()
				.map(|cp| cp._id)
				.unwrap_or(0);
			let mut txns_already_processed = BTreeMap::new();

			let mut last_poll = Instant::now().checked_sub(Duration::from_millis(cfg.pollintervalms)).unwrap();

			loop {
				if stop.load(Relaxed) {
					break
				}

				let wait_ms = cfg.pollintervalms.saturating_sub(last_poll.elapsed().as_millis() as u64);
				if wait_ms > 0 {
					tokio::time::sleep(Duration::from_millis(wait_ms)).await;
				}

				let latest_cp = match sui.get_latest_checkpoint_sequence_number().await {
					Ok(cp) => cp as u64,
					Err(e) => {
						error!("ExtractionError: failed getting latest checkpoint: {:?}", e);
						continue
					}
				};
				last_poll = Instant::now();

				// TODO we need to just hook up to our own stream of outgoing completed checkpoints
				//		so we don't have to ask MongoDB here
				// get last fully completed cp as a starting point for computing how far we've fallen behind
				// and whether we need to go into backfill mode
				let last_completed_cp = coll
					.find_one(None, FindOneOptions::builder().sort(doc! {"_id": -1}).build())
					.await
					.unwrap()
					.map(|cp| cp._id);
				let behind_cp = latest_cp - last_completed_cp.unwrap_or(0) as u64;
				info!("ExtractionInfo: Currently behind by {} checkpoints.", behind_cp);
				if behind_cp > cfg.backfillthreshold as u64 {
					warn!("IngestWarning: Initializing backfill pipeline.");
					if cfg.pausepollonbackfill {
						// ask low-latency work to pause
						pause_livescan.store(250, Relaxed);
						warn!("IngestWarning: Pausing livescan pipeline.");
					}
					// run backfill
					let (checkpointcompleted_rx, handle) =
						spawn_backfill_pipeline(&cfg, &cfg.backfill, sui.clone(), pulsar.clone(), None)
							.await
							.unwrap();
					// We continue low-latency work as soon as the first checkpoint crawl of the backfill completes,
					// so we incur as little unnecessary latency as possible while just waiting for
					// some remaining items to be completed
					tokio::spawn({
						let pause = pause_livescan.clone();
						async move {
							checkpointcompleted_rx.await.ok();
							pause.store(0, Relaxed);
						}
					});
					// TODO I think instead of waiting for the backfill to finish we just want to get
					//		the max cp returned when spawning, remember if the thing is currently still
					//		running, and if so, just don't spawn it again... while continuing to run
					//		the normal pipeline
					let max_cp = handle.await.unwrap();
					if max_cp > 0 {
						// update all reference checkpoints
						last_livescan_cp = max_cp;
					}
					// check again
					continue
				}

				if latest_cp == last_livescan_cp {
					info!("ExtractionInfo: No new checkpoint detected. Latest checkpoint: {}", latest_cp);
					// no new checkpoint available, try again
					continue
				}

				// prepare running livescan frontend

				// collect all currently known-processed tx digests from polling strategy
				// XXX ideally we'd write some code that directly retrieves only the currently ready
				// 		items from poll_livescan_items_transactions, but I don't know how to do that (yet),
				//		so applying a trick of waiting for a max of X ms via tokio timer + select!
				//		(luckily, select! knows how to poll like we need it here!)
				let timeout = tokio::time::sleep(Duration::from_millis(10));
				pin!(timeout);
				// checkpoint-based marker value we use to keep track of the time at which we add values
				// to poll_livescan_items_transactions, so we can do regular cleanup and prevent the collection
				// from growing indefinitely
				// needs to be at >= 1 so it works with our approach below
				let cp_offset_marker = (latest_cp - start_cp_for_offset).max(1) as i64;
				loop {
					tokio::select! {
						Some(tx) = poll_livescan_items_transactions.recv() => {
							// we're using the cp offset relative to process start as the value so we can keep track of
							// when the entry was made, so even if we should have bugs leading to mismatching
							// and non-removal of entries, we can still remove them on an age basis
							// and thus prevent memory leaks
							match txns_already_processed.try_insert(tx, cp_offset_marker) {
								Ok(_) => {
									// it was a new entry
								},
								Err(OccupiedError { entry: e, .. }) => {
									let v = e.get();
									if *v > 0 {
										// duplicate processing from polling side, probably a bug somewhere we need to fix!
										warn!("[FIXME] duplicate transaction from polling side! ignoring, but this may indicate a bug in our own or in external code!");
									} else if *v < 0 {
										// it's been seen by livescan first and already "flushed",
										// so we need to remove this entry
										e.remove();
									} else {
										unreachable!()
									}
								}
							}
						},
						_ = &mut timeout => {
							break
						}
					}
				}
				// gc: stop remembering txs added more than 120 checkpoints ago
				txns_already_processed.drain_filter(|_, v| latest_cp as i64 - v.abs() > 120);

				// spawn the livescan
				let (mut scan_items, mut livescan_cp_control_rx) =
					spawn_livescan(latest_cp, last_livescan_cp, &cfg, sui.clone()).await;

				// process all items it returns, to figure out which ones may have been missed by the polling strategy
				// and calculate and send off checkpoint control messages as we go, too
				let mut cur_cp = 0u64;
				let mut num_items = 0u32;
				let mut skip = false;
				while let Some((tx, item)) = scan_items.next().await {
					// if it's a new tx, then we want to check if we need to skip all items of this tx
					if let Some(tx) = tx {
						// keep track of "already processed" txs, figure out if we want to skip this tx
						// first, see not on try_insert() above
						// then, we're using a negative value here, so we can discern between entries
						// made from the polling side (positive) vs livescan side (negative)
						match txns_already_processed.try_insert(tx, -cp_offset_marker) {
							Ok(_) => {
								// tx was not seen yet
								skip = false;
							}
							Err(OccupiedError { entry: e, .. }) => {
								let v = e.get();
								if *v < 0 {
									// duplicate processing from livescan side, probably a bug somewhere we need to fix!
									error!("ExtractionError: Duplicate transaction from livescan side! ignoring, but this may indicate a bug in our own or in external code!");
									skip = false;
								} else if *v > 0 {
									// tx was already seen from polling side, so we want to skip it here
									// meaning: we want to prevent the associated objects to be processed
									// as that would mean processing them a second time!
									e.remove();
									skip = true;
								} else {
									unreachable!()
								}
							}
						}
					}

					// checkpoint handling:
					// - count items for current checkpoint
					// - submit previous checkpoint count if we've just changed checkpoints
					let cp = item.cp as u64;
					if cur_cp != cp {
						info!("ExtractionInfo: Current unprocessed items in checkpoint: {}", cur_cp);
						if cur_cp != 0 {
							if livescan_cp_control_tx.send((cur_cp as CheckpointSequenceNumber, num_items)).await.is_err() {
								break
							}
						}
						cur_cp = cp;
						num_items = 0;
					}

					num_items += 1;

					// if we're not currently skipping, then we also need to forward the item to the pipeline
					if !skip {
						if livescan_items_tx.send((tx, item)).await.is_err() {
							break
						}
					}
				}

				// final cp completion was not sent, as there was no cp change to trigger it
				if cur_cp != 0 {
					livescan_cp_control_tx.send((cur_cp as CheckpointSequenceNumber, num_items)).await.ok();
				}

				// now store completions for all checkpoints we skipped above due to not receiving any items for
				while let Some((cp, num_items)) = livescan_cp_control_rx.recv().await {
					if num_items == 0 {
						livescan_cp_control_tx.send((cp, num_items)).await.ok();
					}
				}

				// we're done, the livescan is complete
				// (at least the frontend; backend items may still be processing in the background)
				last_livescan_cp = latest_cp;
			}
		}
	});

	// keep running for as long as our low-latency pipeline is running
	livescan_handle.await?;

	Ok(())
}

// Crawl the entire history of checkpoints on the Sui blockchain.
async fn spawn_checkpoint_poll(
	cfg: &AppConfig,
	sui: ClientPool,
	pause: Arc<AtomicU16>,
) -> (ACReceiver<(Option<TransactionDigest>, ObjectItem)>, UnboundedReceiver<CheckpointSequenceNumber>) {
	info!("ExtractionInfo: Spawning checkpoint poll");
	let (observed_checkpoints_tx, observed_checkpoints_rx) = tokio::sync::mpsc::unbounded_channel();
	let (items_tx, items_rx) = async_channel::bounded(cfg.livescan.queuebuffers.checkpointout);
	tokio::spawn(do_poll(cfg.clone(), sui.clone(), pause.clone(), observed_checkpoints_tx, items_tx));
	(items_rx, observed_checkpoints_rx)
}

async fn spawn_livescan(
	checkpoint_max: u64,
	stop_at_cp: u64,
	cfg: &AppConfig,
	sui: ClientPool,
) -> (ACReceiver<(Option<TransactionDigest>, ObjectItem)>, TReceiver<(CheckpointSequenceNumber, u32)>) {
	info!("ExtractionInfo: Spawning livescan.");
	let default_num_workers = sui.configs.len();
	let num_checkpoint_workers = cfg.livescan.workers.checkpoint.unwrap_or(default_num_workers);

	// turn our last cp into a fake completed range, which will make the scan stop
	let completed_checkpoint_ranges = vec![(stop_at_cp, 0)];

	let (items_tx, items_rx) = async_channel::bounded(cfg.livescan.queuebuffers.checkpointout);
	let (cp_control_tx, cp_control_rx) = tokio::sync::mpsc::channel(cfg.livescan.queuebuffers.cpcompletions);
	let step_size = num_checkpoint_workers;
	for partition in 0..num_checkpoint_workers {
		tokio::spawn(do_scan(
			cfg.livescan.clone(),
			IngestRoute::Livescan,
			checkpoint_max,
			completed_checkpoint_ranges.clone(),
			step_size,
			partition,
			sui.clone(),
			None,
			items_tx.clone(),
			cp_control_tx.clone(),
		));
	}

	(items_rx, cp_control_rx)
}

async fn spawn_pipeline_tail(
	cfg: AppConfig,
	pc: PipelineConfig,
	sui: ClientPool,
	pulsar: Pulsar<TokioExecutor>,
	object_ids_rx: ACReceiver<(Option<TransactionDigest>, ObjectItem)>,
) -> Result<(TSender<(CheckpointSequenceNumber, u32)>, JoinHandle<u64>)> {
	info!("ExtractionInfo: Spawning pipeline tail.");
	let mongo = cfg.mongo.client(&pc.mongo).await?;

	let default_num_workers = sui.configs.len();
	let num_object_workers = pc.workers.object.unwrap_or(default_num_workers);
	let num_mongo_workers = pc.workers.mongo.unwrap_or(default_num_workers);
	info!("ExtractionInfo: workers: object: {}; mongo: {}", num_object_workers, num_mongo_workers);

	// mostly we want to buffer up to mongo batch size items smoothly, assuming writes to mongo from a single writer will be fast enough
	let (mongo_tx, mongo_rx) =
		async_channel::bounded(pc.mongo.batchsize * pc.queuebuffers.mongoinfactor * num_mongo_workers);

	// Initialize object workers which read object changes from the checkpoint step, and fetch full object data via RPC.
	{
		for _ in 0..num_object_workers {
			tokio::spawn({
				let sui = sui.clone();
				let mut retries = crate::pulsar::make_producer(&cfg, &pulsar, "retries").await?;
				let batch_size = pc.objectqueries.batchsize;
				let batch_wait_timeout = pc.objectqueries.batchwaittimeoutms;
				let object_ids_rx = object_ids_rx.clone();
				let mongo_tx = mongo_tx.clone();

				async move {
					let object_ids_rx = object_ids_rx
						.map(|(_, item)| item)
						.chunks_timeout(batch_size, Duration::from_millis(batch_wait_timeout));
					let stream = transform_batched(object_ids_rx, sui).await;
					let stream = stream! {
						for await (status, item) in stream {
							if let StepStatus::Err = status {
								retries.send(item).await.expect("ExtractionError: failed to send retry message to pulsar!");
							} else {
								yield item;
							}
						}
					};
					// convert stream to channel
					pin!(stream);
					while let Some(it) = stream.next().await {
						mongo_tx.send(it).await.expect("ExtractionInfo: passing items from object data stream to mongo tokio channel");
					}
				}
			});
		}
		drop(object_ids_rx);
		drop(mongo_tx);
	}

	let (last_tx, mut last_rx) = tokio::sync::mpsc::channel(pc.queuebuffers.last);

	// step 3: mongo workers
	{
		for _ in 0..num_mongo_workers {
			let mongo_rx = mongo_rx.clone();
			let mongo_rx =
				mongo_rx.chunks_timeout(pc.mongo.batchsize, Duration::from_millis(pc.mongo.batchwaittimeoutms));
			tokio::spawn(load_batched(cfg.clone(), pc.clone(), mongo_rx, mongo.clone(), last_tx.clone()));
		}
		drop(mongo_rx);
		drop(last_tx);
	}

	// for the control channel, we want to add some blocking behavior in case the task acting
	// on the control messages falls too far behind -- we don't want to process major portions of the
	// chain without also storing info about our progress so we can resume about where we left off
	let (cp_control_tx, mut cp_control_rx) = tokio::sync::mpsc::channel(pc.queuebuffers.cpcompletions);

	let handle = tokio::spawn({
		let cfg = cfg.clone();
		let pc = pc.clone();
		async move {
			// finally: check completions, issue retries
			let mut retries = crate::pulsar::make_producer(&cfg, &pulsar, "retries").await.unwrap();
			let mut completions_left = HashMap::new();
			let mut max_cp_completed = 0u64;
			let mut last_latency = 0;
			loop {
				let (cp, v) = tokio::select! {
					Some((status, item, completed_at)) = last_rx.recv() => {
						if let (Some(ts_sui), Some(completed)) = (item.ts_sui, completed_at) {
							let latency = completed.checked_sub(item.ts_first_seen).unwrap_or(0);
							// we don't want to log the same value more than once consecutively
							if latency != last_latency {
								let source = match item.ingested_via {
									IngestRoute::Poll => "P",
									IngestRoute::Livescan => "L",
									IngestRoute::Backfill => "B",
								};
								info!("[{}] {}ms // {}ms", source, latency, completed - ts_sui);
								last_latency = latency;
							}
						}
						let cp = item.cp;
						if cp == 0 {
							// ignore, not really a checkpoint
							continue;
						}
						if let StepStatus::Err = status {
							retries.send(item).await.expect("ExtractionError: failed to send retry message to pulsar!");
						}
						(cp, completions_left.entry(cp).and_modify(|n| *n -= 1).or_insert(-1i64))
					},
					Some((cp, num_items)) = cp_control_rx.recv() => {
						(cp, completions_left.entry(cp).and_modify(|n| *n += num_items as i64).or_insert(num_items as i64))
					},
					// if both branches return None, we're complete
					else => break,
				};
				if *v == 0 {
					mongo_checkpoint(&cfg, &pc, &mongo, cp).await;
					completions_left.remove(&cp);
					max_cp_completed = max_cp_completed.max(cp);
				}
			}
			max_cp_completed
		}
	});

	Ok((cp_control_tx, handle))
}
// The backfill pipeline crawls several checkpoints concurrently. Although this is faster for backfilling, it can overwhelm downstream systems with too many CRUD operations. It can also cause some delay for ingesting the latest checkpoint data.
// Each backfill pipeline creates its own RocksDB instance, which is used to prevent ingesting the same data points repeatedly across multiple threads.
#[allow(unused)]
async fn spawn_backfill_pipeline(
	cfg: &AppConfig,
	pc: &PipelineConfig,
	mut sui: ClientPool,
	pulsar: Pulsar<TokioExecutor>,
	start_checkpoint: Option<u64>,
) -> Result<(tokio::sync::oneshot::Receiver<()>, JoinHandle<u64>)> {
	info!("ExtractionInfo: Spawning backfill pipeline.");
	let db = {
		// give each pipeline its own rocksdb instance
		let rocksdbfile = format!("{}_{}", cfg.rocksdbfile, pc.name);
		// this is a new run, so we remove any previous rocksdb data to avoid skipping newer objects
		// incorrectly, because we're re-scanning from newer txs than where we started last time
		std::fs::remove_dir_all(&rocksdbfile).ok();
		Arc::new(DBWithThreadMode::<SingleThreaded>::open_default(&rocksdbfile).unwrap())
	};

	let mongo = cfg.mongo.client(&pc.mongo).await?;

	let checkpoint_max = if let Some(start) = start_checkpoint {
		start
	} else {
		sui.get_latest_checkpoint_sequence_number().await.unwrap() as u64
	};
	info!("ExtractionInfo: Latest checkpoint was fetched via RPC: {}", checkpoint_max);
	let default_num_workers = sui.configs.len();
	let num_checkpoint_workers = pc.workers.checkpoint.unwrap_or(default_num_workers);

	// fetch already completed checkpoints
	let completed_checkpoint_ranges = {
		let coll = mongo.collection::<Checkpoint>(&mongo::mongo_collection_name(&cfg, "_checkpoints"));
		let mut stop_at = 0;
		let mut cpids = coll
			.find(None, None)
			.await
			.unwrap()
			.map(|r| {
				let cp = r.unwrap();
				if cp.stop.unwrap_or(false) {
					stop_at = stop_at.max(cp._id);
				}
				cp._id
			})
			.collect::<Vec<_>>()
			.await;
		// ensure that we actually stop there
		if stop_at > 0 {
			cpids.sort_unstable();
			// chop off all checkpoints lower than stop_at
			cpids.drain_filter(|cp| *cp < stop_at);
			let mut ranges = make_descending_ranges(cpids);
			// add where to stop as the last range item
			ranges.push((stop_at, 0));
			ranges
		} else {
			make_descending_ranges(cpids)
		}
	};

	// MPMC channel, as an easy way to balance incoming work from checkpoint workers into multiple object workers.
	info!("ExtractionInfo: Initializing Tokio channel for object workers with bound limit {}", pc.queuebuffers.checkpointout);
	let (object_ids_tx, object_ids_rx) = async_channel::bounded(pc.queuebuffers.checkpointout);

	let (cp_control_tx, handle) =
		spawn_pipeline_tail(cfg.clone(), pc.clone(), sui.clone(), pulsar, object_ids_rx).await?;

	info!("Initializing {} number of backfill workers.", num_checkpoint_workers);
	let (checkpointfinished_tx, checkpointfinished_rx) = tokio::sync::oneshot::channel();
	{
		let step_size = num_checkpoint_workers;
		let mut handles = Vec::with_capacity(num_checkpoint_workers);
		for partition in 0..num_checkpoint_workers {
			handles.push(tokio::spawn(do_scan(
				pc.clone(),
				IngestRoute::Backfill,
				checkpoint_max,
				completed_checkpoint_ranges.clone(),
				step_size,
				partition,
				sui.clone(),
				Some(db.clone()),
				object_ids_tx.clone(),
				cp_control_tx.clone(),
			)));
		}
		drop(object_ids_tx);
		drop(cp_control_tx);
		tokio::spawn(async move {
			futures::future::join_all(handles).await;
			checkpointfinished_tx.send(()).ok();
		});
	}

	Ok((checkpointfinished_rx, handle))
}

// TODO: This function is WIP
async fn do_walk(
	pc: PipelineConfig,
	ingest_route: IngestRoute,
	checkpoint_start: u64,
	completed_checkpoint_ranges: Vec<(u64, u64)>,
	mut sui: ClientPool,
	db: Option<Arc<DBWithThreadMode<SingleThreaded>>>,
	object_ids_tx: ACSender<(Option<TransactionDigest>, ObjectItem)>,
	cp_control_tx: TSender<(CheckpointSequenceNumber, u32)>,
) {
	// general idea:
	// query first incomplete checkpoint
	// skip items while within completed range
	// remember last page digest
	// if checkpoint of last item in current batch is still > N checkpoints away from next incomplete
	// checkpoint, then we want to query by checkpoint again, otherwise continue from digest

	let mut cp = checkpoint_start;
	let mut last_cp = u64::MAX;

	let mut completed_iter = completed_checkpoint_ranges.into_iter();
	let mut completed_range = completed_iter.next();

	'cp: loop {
		let mut num_objects = 0u32;

		macro_rules! traverse_checkpoints {
			($next_cp:expr) => {
				let next_cp = $next_cp;
				match traverse_checkpoints(
					&cp_control_tx,
					&mut cp,
					&mut completed_iter,
					&mut completed_range,
					&mut num_objects,
					next_cp,
				)
				.await
				{
					MoveAction::Continue => {
						// just continue as we would
					}
					MoveAction::NextCheckpoint => continue 'cp,
					MoveAction::End => break 'cp,
				}
			};
		}

		// ensure we don't get into an infinite loop with the current checkpoint
		if cp == last_cp {
			traverse_checkpoints!(cp - 1);
			last_cp = cp;
		}

		let mut query_cp = Some(cp);
		let mut cursor = None;
		let mut retries_left = pc.checkpointretries;

		loop {
			// loop that progresses through all pages of a query which has been started by checkpoint,
			// but then does not limit itself to that single checkpoint
			let call_start_ts = Utc::now().timestamp_millis() as u64;
			let q = SuiTransactionBlockResponseQuery::new(
				query_cp.take().map(|cp| TransactionFilter::Checkpoint(cp as CheckpointSequenceNumber)),
				Some(SuiTransactionBlockResponseOptions::new().with_object_changes()),
			);
			let page = sui.query_transaction_blocks(q, cursor, Some(SUI_QUERY_MAX_RESULT_LIMIT), true).await;
			match page {
				Ok(page) => {
					retries_left = pc.checkpointretries;
					for block in page.data {
						let block_cp = block.checkpoint.unwrap() as u64;
						if block_cp != cp {
							traverse_checkpoints!(block_cp);
						}
						// if we're still here, we want to track the next cursor immediately, as we
						// might not actually have any changes in this block or will be skipping it
						cursor = Some(block.digest);
						// skip this block?
						if block_cp > cp {
							continue
						}
						if let Some(changes) = block.object_changes {
							let mut tx_digest_once = Some(block.digest);
							for change in changes {
								let Some((object_id, version, deleted)) = client::parse_change(change) else {
                                    continue;
                                };
								if let Some(db) = &db {
									let k = object_id.as_slice();
									// known?
									if let None = db.get_pinned(k).unwrap() {
										// no, new one, so we mark it as known
										// FIXME put version so we can ensure only older versions are skipped
										//	     in case we process things out of order
										db.put(k, Vec::new()).unwrap();
									// keep going below
									} else {
										continue
									}
								}
								num_objects += 1;
								// send to step 2
								let send_res = object_ids_tx
									.send((
										tx_digest_once.take(),
										ObjectItem {
											cp: cp as CheckpointSequenceNumber,
											deletion: deleted,
											id: object_id,
											version,
											ts_sui: block.timestamp_ms,
											ts_first_seen: call_start_ts,
											ingested_via: ingest_route,
											bytes: Default::default(),
										},
									))
									.await;
								if send_res.is_err() {
									// channel closed, consumers stopped
									break 'cp
								}
							}
						}
					}
					if !page.has_next_page {
						break
					} else if page.next_cursor.is_none() {
						warn!("ExtractionError: query_transaction_blocks({}, {:?}) page.has_next_page == true, but there is no page.next_cursor! continuing as if no next page. Posslbie Sui API bug.", cp, cursor);
						break
					} else {
						cursor = page.next_cursor;
					}
				}
				Err(err) => {
					if retries_left == 0 {
						warn!(error = ?err, "ExtractionError: Exhausted all retries fetching checkpoint data, leaving checkpoint {} unfinished for this run", cp);
						break
					}
					warn!(error = ?err, "ExtractionError: There was an error reading object changes... retrying (retry #{}) after short timeout", retries_left);
					retries_left -= 1;
					tokio::time::sleep(Duration::from_millis(pc.checkpointretrytimeoutms)).await;
				}
			}
		}
	}
}

enum MoveAction {
	Continue,
	NextCheckpoint,
	End,
}

async fn traverse_checkpoints(
	cp_control_tx: &TSender<(CheckpointSequenceNumber, u32)>,
	cp: &mut u64,
	completed_iter: &mut IntoIter<(u64, u64)>,
	completed_range: &mut Option<(u64, u64)>,
	num_objects: &mut u32,
	next_cp: u64,
) -> MoveAction {
	info!("ExtractionInfo: Initializing traverse_checkpoints()");
	for cp in *cp..next_cp {
		// we're done with this previous cp
		// send control message about number of expected object tasks from it
		cp_control_tx.send((cp as CheckpointSequenceNumber, *num_objects)).await.unwrap();
		*num_objects = 0;
	}

	*cp = next_cp;

	// find next checkpoint
	loop {
		if let Some((end, start)) = completed_range {
			if *cp < *start {
				// cp too low already, check next one
				*completed_range = completed_iter.next();
			} else if *cp <= *end {
				// if we're in range and start is 1 or 0, we're entirely done,
				// as that means that all remaining checkpoints are already complete
				if *start <= 1 {
					return MoveAction::End
				}
				// match!
				*cp = *start - 1;
				// we also already know that we need to compare with the next range item
				*completed_range = completed_iter.next();

				// is jumping to that cp faster than continuing to walk?
				const CHECKPOINT_DENSITY: f32 = 0.3;
				if (next_cp - *cp) > (SUI_QUERY_MAX_RESULT_LIMIT as f32 / CHECKPOINT_DENSITY) as u64 * 2 {
					// jump!
					return MoveAction::NextCheckpoint
				}

				break
			} else {
				// this cp is still higher than our highest range end, so we wait and try again
				// while cp is getting lower and lower, until we have a potential match
				break
			}
		} else {
			break
		}
	}

	MoveAction::Continue
}

// This is the technique used in Livescan and Backfill mode.
async fn do_scan(
	pc: PipelineConfig,
	ingest_route: IngestRoute,
	checkpoint_max: u64,
	completed_checkpoint_ranges: Vec<(u64, u64)>,
	step_size: usize,
	partition: usize,
	mut sui: ClientPool,
	db: Option<Arc<DBWithThreadMode<SingleThreaded>>>,
	object_ids_tx: ACSender<(Option<TransactionDigest>, ObjectItem)>,
	cp_control_tx: TSender<(CheckpointSequenceNumber, u32)>,
) {
	info!("ExtractionInfo: Initializing do_scan()");
	let stop = ctrl_c_bool();
	let mut completed_iter = completed_checkpoint_ranges.iter();
	let mut completed_range = completed_iter.next();
	let mut iter = (1..=checkpoint_max as usize - partition).rev().step_by(step_size).into_iter();
	// walk partitioned checkpoints range from newest to oldest
	'cp: while let Some(cp) = iter.next() {
		let cp = cp as u64;
		// do we need to stop?
		if stop.load(Relaxed) {
			break
		}
		// check if we've already completed this checkpoint:
		// ranges are sorted from highest to lowest, so we can iterate them in tandem with the
		// checkpoint sequence itself
		loop {
			if let Some((end, start)) = completed_range {
				if cp < *start {
					// cp too low already, check next one
					completed_range = completed_iter.next();
				} else if cp <= *end {
					// if we're in range and start is 1 or 0, we're entirely done,
					// as that means that all remaining checkpoints are already complete
					if *start <= 1 {
						break 'cp
					}
					// match! advance by whatever number of steps we need to end this iteration
					// at checkpoint `start`, or right before, considering our step size
					iter.advance_by(((cp - *start) as usize).div_floor(step_size)).ok();
					// we also already know that we need to compare with the next range item
					completed_range = completed_iter.next();
					continue 'cp
				} else {
					// this cp is still higher than our highest range end, so we wait and try again
					// while cp is getting lower and lower, until we have a potential match
					break
				}
			} else {
				break
			}
		}
		// start fetching all tx blocks for this checkpoint
		let q = SuiTransactionBlockResponseQuery::new(
			Some(TransactionFilter::Checkpoint(cp as CheckpointSequenceNumber)),
			Some(SuiTransactionBlockResponseOptions::new().with_object_changes()),
		);
		let mut cursor = None;
		let mut retries_left = pc.checkpointretries;
		let mut num_objects = 0u32;

		loop {
			let call_start_ts = Utc::now().timestamp_millis() as u64;
			let page = sui.query_transaction_blocks(q.clone(), cursor, Some(SUI_QUERY_MAX_RESULT_LIMIT), true).await;
			match page {
				Ok(page) => {
					retries_left = pc.checkpointretries;
					for block in page.data {
						if let Some(changes) = block.object_changes {
							let mut tx_digest_once = Some(block.digest);
							for change in changes {
								let Some((object_id, version, deleted)) = client::parse_change(change) else {
                                    continue;
                                };
								if let Some(db) = &db {
									let k = object_id.as_slice();
									// known?
									if let None = db.get_pinned(k).unwrap() {
										// no, new one, so we mark it as known
										// FIXME put version so we can ensure only older versions are skipped
										//	     in case we process things out of order
										db.put(k, Vec::new()).unwrap();
									// keep going below
									} else {
										continue
									}
								}
								num_objects += 1;
								// send to step 2
								let send_res = object_ids_tx
									.send((
										tx_digest_once.take(),
										ObjectItem {
											cp: cp as CheckpointSequenceNumber,
											deletion: deleted,
											id: object_id,
											version,
											ts_sui: block.timestamp_ms,
											ts_first_seen: call_start_ts,
											ingested_via: ingest_route,
											bytes: Default::default(),
										},
									))
									.await;
								if send_res.is_err() {
									// channel closed, consumers stopped
									break 'cp
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
						error!("ExtractionError: query_transaction_blocks({}, {:?}) page.has_next_page == true, but there is no page.next_cursor! continuing as if no next page. Possible API bug.", cp, cursor);
						cp_control_tx.send((cp as CheckpointSequenceNumber, num_objects)).await.unwrap();
						break
					} else {
						cursor = page.next_cursor;
					}
				}
				Err(err) => {
					if retries_left == 0 {
						warn!(error = ?err, "ExtractionError: Exhausted all retries fetching checkpoint data, leaving checkpoint {} unfinished for this run", cp);
						break
					}
					error!(error = ?err, "ExtractionError: There was an error reading object changes... retrying (retry #{}) after short timeout", retries_left);
					retries_left -= 1;
					tokio::time::sleep(Duration::from_millis(pc.checkpointretrytimeoutms)).await;
				}
			}
		}
	}
}

// TODO use first configured rpc source instead of RR, assuming that's our lowest-latency one
async fn do_poll(
	cfg: AppConfig,
	mut sui: ClientPool,
	pause: Arc<AtomicU16>,
	observed_checkpoints_tx: UnboundedSender<CheckpointSequenceNumber>,
	items: ACSender<(Option<TransactionDigest>, ObjectItem)>,
) {
	info!("ExtractionInfo: Initializing do_poll()");
	let q = SuiTransactionBlockResponseQuery::new(
		None,
		Some(SuiTransactionBlockResponseOptions::new().with_object_changes()),
	);

	let stop = ctrl_c_bool();
	let mut cursor = None;
	let mut retry_count = 0;
	let mut desc = true;
	let mut checkpoints = HashSet::with_capacity(64);
	let mut last_poll = Instant::now().checked_sub(Duration::from_millis(cfg.pollintervalms)).unwrap();

	loop {
		if stop.load(Relaxed) {
			break
		}
		// XXX not sure yet if pausing this way is silly or smart, but it should work at least
		loop {
			let pause = pause.load(Relaxed);
			if pause == 0 {
				break
			}
			tokio::time::sleep(Duration::from_millis(pause as u64)).await;
		}
		{
			let wait_ms = cfg.pollintervalms.saturating_sub(last_poll.elapsed().as_millis() as u64);
			if wait_ms > 0 {
				tokio::time::sleep(Duration::from_millis(wait_ms)).await;
			}
		}
		let call_start = Instant::now();
		// for latency tracking, to make it a little easier, we're adding half of the time spent waiting since the last poll
		// since that's the average of when a tx will have been added to the chain in the meantime
		let latency_first_seen_ms =
			Utc::now().timestamp_millis() as u64 + last_poll.elapsed().as_millis().div_ceil(2) as u64;
		match sui.query_transaction_blocks(q.clone(), cursor, Some(SUI_QUERY_MAX_RESULT_LIMIT), desc).await {
			Ok(mut page) => {
				write_metric_rpc_request("query_transaction_blocks").await;
				// we want to throttle only on successful responses, otherwise we'd rather try again immediately
				last_poll = call_start;
				retry_count = 0;
				if page.data.is_empty() {
					info!("ExtractionInfo: No new txs when querying with desc={} cursor={:?}, retrying immediately", desc, cursor);
					continue
				}
				// we want to process items in asc order
				if desc {
					page.data.reverse();
					// and we only want to query for desc order for the first iteration, since at that
					// point we don't have a tx id to start from for querying in asc order
					desc = false;
				}
				cursor = Some(page.data.last().unwrap().digest);

				checkpoints.clear();
				for block in page.data {
					// if we found a new (to this iteration) checkpoint, we want to let the checkpoints-based
					// processor know immediately
					// we also skip those items here, so we don't need to coordinate with it
					if let Some(cp) = block.checkpoint && checkpoints.insert(cp) {
                        observed_checkpoints_tx.send(cp).ok();
                        continue;
                    }
					let mut tx_digest_once = Some(block.digest);
					let Some(changes) = block.object_changes else { continue; };
					for (id, version, deletion) in changes.into_iter().filter_map(client::parse_change) {
						if items
							.send((
								tx_digest_once.take(),
								ObjectItem {
									cp: 0,
									deletion,
									id,
									version,
									ts_sui: block.timestamp_ms,
									ts_first_seen: latency_first_seen_ms,
									ingested_via: IngestRoute::Poll,
									bytes: Default::default(),
								},
							))
							.await
							.is_err()
						{
							// channel closed, stop processing
							return
						}
					}
				}
			}
			Err(err) => {
				let timeout_ms = 100;
				warn!(error = ?err, "ExtractionError: Error polling tx blocks; retry #{} after {}ms timeout", retry_count, timeout_ms);
				retry_count += 1;
				tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
			}
		}
	}
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
					write_metric_rpc_error("multi_get_object_with_options").await;
					// try one by one
					// TODO this should be super easy to do in parallel, firing off the reqs on some tokio thread pool executor
					for mut item in chunk {
						match sui.get_object_with_options(item.id, query_opts.clone()).await {
							Err(err) => {
								error!(object_id = ?item.id, error = format!("{err:?}"), "individual fetch also failed");
								write_metric_rpc_error("get_object_with_options").await;
								yield (StepStatus::Err, item);
							},
							Ok(res) => {
								// TODO send them off in batches
								if let Some((version, bytes)) = parse_get_object_response(&item.id, res).await {
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
						write_metric_rpc_error("unexpected_payload").await;
						panic!("sui.multi_get_object_with_options() mismatch between input and result len!");
					}
					for (mut item, res) in zip(chunk, objs) {
						// TODO if we can't get object info, do we really want to skip indexing this change? or is there something more productive we can do?
						// TODO send them off in batches
						if let Some((version, bytes)) = parse_get_object_response(&item.id, res).await {
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

async fn load_batched<'a, S: Stream<Item = Vec<ObjectItem>> + 'a>(
	cfg: AppConfig,
	pc: PipelineConfig,
	stream: S,
	db: Database,
	last_tx: TSender<(StepStatus, ObjectItem, Option<u64>)>,
) {
	// e.g. prod_testnet_objects
	let collection = mongo::mongo_collection_name(&cfg, "");
	let influx_client = get_influx_singleton();

	pin!(stream);
	while let Some(chunk) = stream.next().await {
		let mut retries_left = pc.mongo.retries;
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
			let res = db
				.run_command(
					doc! {
						"update": &collection,
						"updates": updates,
					},
					None,
				)
				.await;
			match res {
				Ok(res) => {
					// res: {n: i32, upserted: [{index: i32, _id: String}, ...], nModified: i32, writeErrors: [{index: i32, code: i32}, ...]}
					if res.get_i32("n").unwrap() != n as i32 {
						write_metric_mongo_write_error().await;
						panic!(
							"failed to execute at least one of the upserts: {:#?}",
							res.get_array("writeErrors").unwrap()
						);
					}

					let completed_at = pc.tracklatency.then(|| Utc::now().timestamp_millis() as u64);
					// TODO send whole batch at once
					for item in chunk {
						last_tx.send((StepStatus::Ok, item, completed_at)).await.unwrap();
					}

					let inserted = if let Ok(upserted) = res.get_array("upserted") { upserted.len() } else { 0 };
					let modified = res.get_i32("nModified").unwrap();
					let missing = n - (inserted + modified as usize);
					let missing_info =
						if missing > 0 { format!(" // {} items without effect!", missing) } else { String::new() };
					info!("|> mongo: {} total / {} updated / {} created{}", n, modified, inserted, missing_info);

					// We track the number of MongoDB operations in InfluxDB.
					let ts = get_influx_timestamp_as_milliseconds().await;
					let influx_items = vec!(
						InsertObject {
							time: ts,
							count: inserted as i32,
						}.into_query("inserted_object"),
						ModifiedObject {
                            time: ts,
                            count: modified,
                        }.into_query("modified_object"),
						MissingObject {
							time: ts,
                            count: missing as i32,
						}.into_query("missing_object"),
					);
					let write_result = influx_client.query(influx_items).await;
					match write_result {
						Ok(string) => debug!(string),
						Err(error) => warn!("Could not write to influx: {}", error),
					}
					break;
				}
				Err(err) => {
					// the whole thing failed; retry a few times, then assume it's a bug
					// Report to InfluxDB
					write_metric_mongo_write_error().await;
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
