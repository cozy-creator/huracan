use std::{
	fmt::{Display, Formatter},
	io::Cursor,
	iter::zip,
	sync::atomic::{AtomicBool, AtomicU16, Ordering::Relaxed},
};

use anyhow::Result;
use async_channel::Sender as ACSender;
use async_stream::stream;
use bson::{doc, Document};
use futures::Stream;
use futures_batch::ChunksTimeoutStreamExt;
use mongodb::{options::FindOneOptions, Database};
use pulsar::{Producer, Pulsar, TokioExecutor};
use rocksdb::{DBWithThreadMode, SingleThreaded};
use sui_sdk::rpc_types::{
	ObjectChange as SuiObjectChange, SuiObjectDataOptions, SuiObjectResponse, SuiTransactionBlockResponseOptions,
	SuiTransactionBlockResponseQuery,
};
use sui_types::{
	base_types::{ObjectID, SequenceNumber, VersionNumber},
	error::SuiObjectResponseError,
	messages_checkpoint::CheckpointSequenceNumber,
	query::TransactionFilter,
};
use tokio::{
	pin,
	sync::mpsc::{Sender as TSender, UnboundedSender},
	task::JoinHandle,
};

use crate::{
	_prelude::*,
	client::ClientPool,
	conf::{AppConfig, PipelineConfig},
	ctrl_c_bool,
	utils::make_descending_ranges,
};

// sui now allows a max of 1000 objects to be queried for at once (used to be 50), at least on the
// endpoints we're using (try_multi_get_parsed_past_object, query_transaction_blocks)
const SUI_QUERY_MAX_RESULT_LIMIT: usize = 1000;

#[derive(Clone, Debug, Serialize, Deserialize, PulsarMessage)]
pub struct ObjectItem {
	pub cp:       CheckpointSequenceNumber,
	pub deletion: bool,
	pub id:       ObjectID,
	pub version:  SequenceNumber,
	pub bytes:    Vec<u8>,
}

fn parse_change(change: SuiObjectChange) -> Option<(ObjectID, SequenceNumber, bool)> {
	use SuiObjectChange::*;
	Some(match change {
		Created { object_id, version, .. } | Mutated { object_id, version, .. } => (object_id, version, false),
		Deleted { object_id, version, .. } => (object_id, version, true),
		_ => return None,
	})
}

pub async fn start(cfg: &AppConfig, mut sui: ClientPool, pulsar: Pulsar<TokioExecutor>) -> Result<()> {
	let stop = ctrl_c_bool();

	let pause = Arc::new(AtomicU16::new(0));
	// poll for latest txs
	// (low-latency focus)
	spawn_poll().await;
	let (observed_checkpoints_tx, _observed_checkpoints_rx) = tokio::sync::mpsc::unbounded_channel();
	let (poll_tx, _poll_rx) = tokio::sync::mpsc::channel(cfg.lowlatency.queuebuffers.step1out);
	tokio::spawn(do_poll(sui.clone(), stop.clone(), pause.clone(), observed_checkpoints_tx, poll_tx));

	// rest of the low-latency pipeline
	spawn_pipeline_tail().await;

	// observe checkpoints flow:
	// if we fell behind too far, we focus on throughput until caught up:
	// 	spawn scan + throttle low-latency work
	// else ensure completion of latest checkpoints
	tokio::spawn({
		let stop = stop.clone();
		let pause = pause.clone();

		// load latest completed checkpoint
		// if this is too far back
		// 	run full scan, potentially throttle ll work
		// also remember that cp as the last one to use for mini-scans
		// loop:
		// 	run microscan for any cps since last microscan relevant

		async move {
			let mongo = cfg.mongo.client(&cfg.lowlatency.mongo).await.unwrap();
			let coll = mongo.collection::<Checkpoint>(&mongo_collection_name(&cfg, "_checkpoints"));

			// get initial highest checkpoint that we'll use for starting our "microscan" strategy
			let mut last_microscan_cp = sui.get_latest_checkpoint_sequence_number().await.unwrap() as u64;
			// get last fully completed cp as a starting point for computing how far we've fallen behind
			// and whether we need to go into high-throughput catch-up mode
			let mut last_completed_cp = coll
				.find_one(None, FindOneOptions::builder().sort(doc! {"_id": -1}).build())
				.await
				.unwrap()
				.map(|cp| cp._id);

			loop {
				if stop.load(Relaxed) {
					break
				}

				// 2 minutes?
				const FULLSCAN_IF_BEHIND_BY: u64 = 120;

				let latest_cp = sui.get_latest_checkpoint_sequence_number().await.unwrap() as u64;
				if latest_cp - last_completed_cp.unwrap_or(0) > FULLSCAN_IF_BEHIND_BY {
					// ask low-latency work to pause
					pause.store(250, Relaxed);
					// run fullscan
					let (handle, step1completed_rx) =
						spawn_fullscan_pipeline(&cfg, &cfg.throughput, sui.clone(), pulsar.clone()).await.unwrap();
					// continue low-latency work as soon as step 1 of the fullscan completes,
					// so we incur as little unnecessary latency as possible while just waiting for
					// some remaining items to be completed
					tokio::spawn({
						let pause = pause.clone();
						async move {
							step1completed_rx.await.ok();
							pause.store(0, Relaxed);
						}
					});
					let max_cp = handle.await.unwrap();
					if max_cp > 0 {
						// update all reference checkpoints
						last_completed_cp = Some(max_cp);
						last_microscan_cp = max_cp;
					}
					// check again
					continue
				}

				spawn_scan(latest_cp, last_microscan_cp);

				// tokio::select! {}
			}
		}
	});

	Ok(())
}

async fn spawn_pipeline_tail() {}

async fn spawn_scan() {}

async fn spawn_poll() {}

#[derive(Serialize, Deserialize)]
struct Checkpoint {
	// TODO mongo u64 issue
	_id: u64,
}

#[allow(unused)]
async fn spawn_fullscan_pipeline(
	cfg: &AppConfig,
	pc: &PipelineConfig,
	mut sui: ClientPool,
	pulsar: Pulsar<TokioExecutor>,
) -> Result<(JoinHandle<u64>, tokio::sync::oneshot::Receiver<()>)> {
	let db = {
		// give each pipeline its own rocksdb instance
		let rocksdbfile = format!("{}_{}", cfg.rocksdbfile, pc.name);
		// this is a new run, so we remove any previous rocksdb data to avoid skipping newer objects
		// incorrectly, because we're re-scanning from newer txs than where we started last time
		std::fs::remove_dir_all(&rocksdbfile).ok();
		Arc::new(DBWithThreadMode::<SingleThreaded>::open_default(&rocksdbfile).unwrap())
	};

	let mongo = cfg.mongo.client(&pc.mongo).await?;

	//
	let checkpoint_max = sui.get_latest_checkpoint_sequence_number().await.unwrap() as u64;

	let default_num_workers = sui.configs.len();
	let num_step1_workers = pc.workers.step1.unwrap_or(default_num_workers);
	let num_step2_workers = pc.workers.step2.unwrap_or(default_num_workers);
	let num_mongo_workers = pc.workers.mongo.unwrap_or(num_step1_workers);
	info!("workers: step1: {}; step2: {}; mongo: {}", num_step1_workers, num_step2_workers, num_mongo_workers);

	//
	// fetch already completed checkpoints
	let completed_checkpoint_ranges = {
		let coll = mongo.collection::<Checkpoint>(&mongo_collection_name(&cfg, "_checkpoints"));
		let mut cpids = coll.find(None, None).await.unwrap().map(|r| r.unwrap()._id).collect::<Vec<_>>().await;
		make_descending_ranges(cpids)
	};

	// mpmc channel, as an easy way to balance incoming work from step 1 into multiple step 2 workers
	let (object_ids_tx, object_ids_rx) = async_channel::bounded(pc.queuebuffers.step1out);
	// for the control channel, we want to add some blocking behavior in case the task acting
	// on the control messages falls too far behind -- we don't want to process major portions of the
	// chain without also storing info about our progress so we can resume about where we left off
	let (cp_control_tx, mut cp_control_rx) =
		tokio::sync::mpsc::channel(num_step1_workers * pc.queuebuffers.cpcontrolfactor);
	// mostly we want to buffer up to mongo batch size items smoothly, assuming writes to mongo from a single writer will be fast enough
	let (mongo_tx, mongo_rx) =
		async_channel::bounded(pc.mongo.batchsize * pc.queuebuffers.mongoinfactor * num_mongo_workers);
	let (last_tx, mut last_rx) = tokio::sync::mpsc::channel(pc.queuebuffers.last);

	// spawn as many step 1 workers as we have RPC server urls,
	// let each worker freely balance requests between them
	let (step1finished_tx, step1finished_rx) = tokio::sync::oneshot::channel();
	{
		let step_size = num_step1_workers;
		let mut handles = Vec::with_capacity(num_step1_workers);
		for partition in 0..num_step1_workers {
			handles.push(tokio::spawn(do_scan(
				pc.clone(),
				checkpoint_max,
				completed_checkpoint_ranges.clone(),
				step_size,
				partition,
				sui.clone(),
				db.clone(),
				object_ids_tx.clone(),
				cp_control_tx.clone(),
			)));
		}
		drop(object_ids_tx);
		drop(cp_control_tx);
		tokio::spawn(async move {
			futures::future::join_all(handles).await;
			step1finished_tx.send(()).ok();
		});
	}

	// step 2 workers
	{
		for _ in 0..num_step2_workers {
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
					// convert stream to channel
					pin!(stream);
					while let Some(it) = stream.next().await {
						mongo_tx.send(it).await.expect("passing items from step 2 stream to mongo tokio channel");
					}
				}
			});
		}
		drop(object_ids_rx);
		drop(mongo_tx);
	}

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

	let handle = tokio::spawn({
		let cfg = cfg.clone();
		let pc = pc.clone();
		async move {
			// finally: check completions, issue retries
			let mut retries = make_producer(&cfg, &pulsar, "retries").await.unwrap();
			let mut completions_left = HashMap::new();
			let mut max_cp_completed = 0u64;
			loop {
				tokio::select! {
					Some((status, item)) = last_rx.recv() => {
						let cp = item.cp;
						if let StepStatus::Err = status {
							retries.send(item).await.expect("failed to send retry message to pulsar!");
						}
						let v = completions_left.entry(cp).and_modify(|n| *n -= 1).or_insert(-1i64);
						if *v == 0 {
							mongo_checkpoint(&cfg, &pc, &mongo, cp).await;
							completions_left.remove(&cp);
							max_cp_completed = max_cp_completed.max(cp as u64);
						}
					},
					Some((cp, num_items)) = cp_control_rx.recv() => {
						let v = completions_left.entry(cp).and_modify(|n| *n += num_items as i64).or_insert(num_items as i64);
						if *v == 0 {
							mongo_checkpoint(&cfg, &pc, &mongo, cp).await;
							completions_left.remove(&cp);
							max_cp_completed = max_cp_completed.max(cp as u64);
						}
					},
					// if both branches return None, we're complete
					else => break,
				}
			}
			max_cp_completed
		}
	});

	Ok((handle, step1finished_rx))
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

async fn mongo_checkpoint(cfg: &AppConfig, pc: &PipelineConfig, db: &Database, cp: CheckpointSequenceNumber) {
	let mut retries_left = pc.mongo.retries;
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

async fn do_scan(
	pc: PipelineConfig,
	checkpoint_max: u64,
	completed_checkpoint_ranges: Vec<(u64, u64)>,
	step_size: usize,
	partition: usize,
	mut sui: ClientPool,
	db: Arc<DBWithThreadMode<SingleThreaded>>,
	object_ids_tx: ACSender<ObjectItem>,
	cp_control_tx: TSender<(CheckpointSequenceNumber, u32)>,
) {
	let stop = ctrl_c_bool();
	let mut completed_iter = completed_checkpoint_ranges.iter();
	let mut completed_range = completed_iter.next();
	// walk partitioned checkpoints range from newest to oldest
	'cp: for cp in (1..=checkpoint_max as usize - partition).rev().step_by(step_size) {
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
				} else if cp < *end {
					// match! skip this cp and continue with next one!
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
		let mut retries_left = pc.step1retries;
		let mut num_objects = 0u32;

		loop {
			let page = sui.query_transaction_blocks(q.clone(), cursor, Some(SUI_QUERY_MAX_RESULT_LIMIT), true).await;
			match page {
				Ok(page) => {
					retries_left = pc.step1retries;
					for tx_block in page.data {
						if let Some(changes) = tx_block.object_changes {
							for change in changes {
								let Some((object_id, version, deleted)) = parse_change(change) else {
									continue;
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
					tokio::time::sleep(Duration::from_millis(pc.step1retrytimeoutms)).await;
				}
			}
		}
	}
}

// TODO use first configured rpc source instead of RR, assuming that's our lowest-latency one
async fn do_poll(
	mut sui: ClientPool,
	stop: Arc<AtomicBool>,
	pause: Arc<AtomicU16>,
	observed_checkpoints_tx: UnboundedSender<CheckpointSequenceNumber>,
	tx: tokio::sync::mpsc::Sender<ObjectItem>,
) {
	let q = SuiTransactionBlockResponseQuery::new(
		None,
		Some(SuiTransactionBlockResponseOptions::new().with_object_changes()),
	);

	let mut cursor = None;
	let mut retry_count = 0;
	let mut desc = true;
	let mut checkpoints = HashSet::with_capacity(64);
	const MIN_POLL_INTERVAL_MS: u64 = 16;
	let mut last_poll = Instant::now().checked_sub(Duration::from_millis(MIN_POLL_INTERVAL_MS)).unwrap();

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
			let wait_ms = MIN_POLL_INTERVAL_MS - last_poll.elapsed().as_millis() as u64;
			if wait_ms > 0 {
				tokio::time::sleep(Duration::from_millis(wait_ms)).await;
			}
		}
		let call_start = Instant::now();
		match sui.query_transaction_blocks(q.clone(), cursor, Some(SUI_QUERY_MAX_RESULT_LIMIT), desc).await {
			Ok(mut page) => {
				// we want to throttle only on successful responses, otherwise we'd rather try again immediately
				last_poll = call_start;
				retry_count = 0;
				if page.data.is_empty() {
					info!("no new txs when querying with desc={} cursor={:?}, retrying immediately", desc, cursor);
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
					let Some(changes) = block.object_changes else { continue };
					for (id, version, deletion) in changes.into_iter().filter_map(parse_change) {
						if tx
							.send(ObjectItem { cp: 0, deletion, id, version, bytes: Default::default() })
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
				warn!(error = ?err, "error polling tx blocks; retry #{} after {}ms timeout", retry_count, timeout_ms);
				retry_count += 1;
				tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
			}
		}
	}
}

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
		// TODO perhaps we want to do some arena-based allocation for all of the objs in a batch together
		let mut bytes = Vec::with_capacity(4096);
		let bson = bson::to_bson(&obj).unwrap();
		bson.as_document().unwrap().to_writer(&mut bytes).unwrap();
		return Some((obj.version, bytes))
	}
	warn!(object_id = ?id, "neither .data nor .error was set in get_object response!");
	return None
}

async fn transform<'a, S: Stream<Item = ObjectItem> + 'a>(
	stream: S,
	sui: ClientPool,
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

async fn load_batched<'a, S: Stream<Item = Vec<ObjectItem>> + 'a>(
	cfg: AppConfig,
	pc: PipelineConfig,
	stream: S,
	db: Database,
	last_tx: TSender<(StepStatus, ObjectItem)>,
) {
	// e.g. prod_testnet_objects
	let collection = mongo_collection_name(&cfg, "");

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
						panic!(
							"failed to execute at least one of the upserts: {:#?}",
							res.get_array("writeErrors").unwrap()
						);
					}
					let inserted = if let Ok(upserted) = res.get_array("upserted") { upserted.len() } else { 0 };
					let modified = res.get_i32("nModified").unwrap();
					let missing = n - (inserted + modified as usize);
					let missing_info =
						if missing > 0 { format!(" // {} items without effect!", missing) } else { String::new() };
					info!("|> mongo: {} total / {} updated / {} created{}", n, modified, inserted, missing_info);
					for item in chunk {
						last_tx.send((StepStatus::Ok, item)).await.unwrap();
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
