use std::{
	fmt::{Display, Formatter},
	io::Cursor,
	iter::zip,
};

use anyhow::Result;
use async_stream::stream;
use bson::{doc, Document};
use futures::Stream;
use futures_batch::ChunksTimeoutStreamExt;
use macros::with_client_rotation;
use mongodb::Database;
use sui_sdk::{
	apis::ReadApi,
	error::SuiRpcResult,
	rpc_types::{
		ObjectChange as SuiObjectChange, SuiGetPastObjectRequest, SuiObjectData, SuiObjectDataOptions,
		SuiPastObjectResponse, SuiTransactionBlockResponseOptions, SuiTransactionBlockResponseQuery,
		TransactionBlocksPage,
	},
	SuiClient, SuiClientBuilder,
};
use sui_types::{
	base_types::{ObjectID, SequenceNumber},
	digests::TransactionDigest,
};

use crate::_prelude::*;

// sui now allows a max of 1000 objects to be queried for at once (used to be 50), at least on the
// endpoints we're using (try_multi_get_parsed_past_object, query_transaction_blocks)
const SUI_QUERY_MAX_RESULT_LIMIT: usize = 1000;
// we can easily do batches of 256 (which is the max I tested), but then we're working so fast
// that the try_multi_get_parsed_past_object() endpoint is complaining that we're working it too hard!
// 64 seems more or less fine, though
// TODO cycle through multiple RPC providers for that call, so we can crank up the batch size here!
const MONGODB_BATCH_SIZE: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Change {
	Created,
	Mutated,
	Deleted,
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
	urls:    Vec<String>,
	clients: Vec<Client>,
}

#[derive(Clone)]
pub struct Client {
	sui:     SuiClient,
	backoff: Option<(Instant, u8)>,
	reqs:    u64,
}

impl Client {
	fn read_api(&self) -> &ReadApi {
		self.sui.read_api()
	}
}

impl ClientPool {
	pub async fn new(urls: Vec<String>) -> Result<Self> {
		let mut clients = Vec::with_capacity(urls.len());
		clients.push(Self::make_client(&urls[0]).await?);
		Ok(Self { urls, clients })
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

	async fn make_client(url: &str) -> Result<Client> {
		let sui = SuiClientBuilder::default().build(url).await?;
		Ok(Client { sui, backoff: None, reqs: 0 })
	}
}

pub async fn extract<'a, P: Fn(Option<TransactionDigest>, TransactionDigest) + 'a>(
	mut sui: ClientPool,
	mut rx_term: tokio::sync::oneshot::Receiver<()>,
	start_from: Option<TransactionDigest>,
	on_next_page: P,
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
											if let Some((change, object_type)) = ObjectSnapshot::from(change) {
												// skip objects of type "coin"
												if object_type.starts_with("0x2::coin::Coin") {
													continue
												}
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

pub async fn transform<'a, S: Stream<Item = ObjectSnapshot> + 'a>(
	stream: S,
	mut sui: ClientPool,
) -> impl Stream<Item = (StepStatus, ObjectSnapshot)> + 'a {
	// batch incoming items so we can amortize the cost of sui api calls,
	// but send them off one by one, so any downstream consumer (e.g. Pulsar client) can apply their
	// own batching logic, if necessary (e.g. Pulsar producer will auto-batch transparently, if configured)

	let stream = stream.chunks_timeout(50, Duration::from_millis(1_000));

	let query_opts = SuiObjectDataOptions {
		show_type:                 true,
		show_owner:                true,
		show_previous_transaction: true,
		show_display:              false,
		show_content:              true,
		show_bcs:                  true,
		show_storage_rebate:       true,
	};

	fn parse_past_object_response(res: SuiPastObjectResponse) -> Option<SuiObjectData> {
		use SuiPastObjectResponse::*;
		match res {
			VersionFound(obj) => return Some(obj),
			ObjectDeleted(o) => {
				// TODO this can't be right (at least the message needs fixing, but I suspect more than that)
				warn!(object_id = ?o.object_id, version = ?o.version, "object not available: object has been deleted");
			}
			ObjectNotExists(object_id) => {
				warn!(object_id = ?object_id, "object not available: object doesn't exist");
			}
			VersionNotFound(object_id, version) => {
				warn!(object_id = ?object_id, version = ?version, "object not available: version not found");
			}
			VersionTooHigh { object_id, asked_version, latest_version } => {
				warn!(object_id = ?object_id, asked_version = ?asked_version, latest_version = ?latest_version, "object not available: version too high");
			}
		};
		None
	}

	stream! {
		for await mut chunk in stream {
			// skip loading objects for 'delete' type changes, as we're just going to delete them from our working set anyway
			let deleted = chunk.drain_filter(|o| if let Change::Deleted {..} = o.change { true } else { false }).collect::<Vec<_>>();
			let query_objs = chunk.iter().map(|o| o.get_past_object_request()).collect::<Vec<_>>();
			match sui.try_multi_get_parsed_past_object(query_objs, query_opts.clone()).await {
				Err(err) => {
					warn!(error = format!("{err:?}"), "cannot fetch object data for one or more objects, retrying them individually");
					// try one by one
					// TODO this should be super easy to do in parallel, firing off the reqs on some tokio thread pool executor
					for mut snapshot in chunk {
						match sui.try_get_parsed_past_object(snapshot.object_id, snapshot.version, query_opts.clone()).await {
							Err(err) => {
								// TODO add info about object to log
								error!(error = format!("{err:?}"), "individual fetch also failed");
								yield (StepStatus::Err, snapshot);
							},
							Ok(res) => {
								if let Some(obj) = parse_past_object_response(res) {
									snapshot.object = Some(bson::to_vec(&obj).unwrap());
									yield (StepStatus::Ok, snapshot);
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
						panic!("sui.try_multi_get_parsed_past_object() mismatch between input and result len!");
					}
					for (mut snapshot, res) in zip(chunk, objs) {
						// TODO if we can't get object info, do we really want to skip indexing this change? or is there something more productive we can do?
						if let Some(obj) = parse_past_object_response(res) {
							snapshot.object = Some(bson::to_vec(&obj).unwrap());
							yield (StepStatus::Ok, snapshot);
						}
					}
				}
			}
			// (the following note applies if we're not equipped to handle out-of-order changes/deletes, but at least at this point we are)
			// send in deleted items last, to ensure we don't re-insert any object that was just deleted
			// just because they were processed as part of the same chunk of our stream here
			// ideally we would execute these exactly in order, but that would require more implementation
			// effort, and this simplification should retain logical correctness of effects
			for item in deleted {
				yield (StepStatus::Ok, item);
			}
		}
	}
}

pub async fn load<'a, S: Stream<Item = ObjectSnapshot> + 'a>(
	stream: S,
	db: &'a Database,
	collection_name: &'a str,
) -> impl Stream<Item = (StepStatus, ObjectSnapshot)> + 'a {
	let stream = stream.chunks_timeout(MONGODB_BATCH_SIZE, Duration::from_millis(1_000));

	stream! {
		for await chunk in stream {
			let mut retries_left = 2;
			loop {
				// for now mongo's rust driver doesn't offer a way to directly do bulk updates / batching
				// there's a high-level API only for inserting many, but not for updating or deleting many,
				// and neither for mixing all of those easily
				// but what it does provide is the generic run_command() method,
				let updates = chunk.iter().map(|item| {
					let v = item.version.to_string();
					// FIXME our value range here is u64, but I can't figure out how to get a BSON repr of a u64?!
					let v_ = i64::from_str_radix(&v[2..], 16).unwrap();
					match item.change {
						Change::Deleted => {
							// we're assuming each object id will ever exist only once, so when deleting
							// we don't check for previous versions
							// we execute the delete, whenever it may come in, and it's final
							doc! {
								"q": doc! { "_id": item.object_id.to_string() },
								"u": doc! {
									"$set": {
										"_id": item.object_id.to_string(),
										"version": v,
										"version_": v_,
										"deleted": true,
									},
								},
								"upsert": true,
								"multi": false,
							}
						}
						Change::Created | Change::Mutated => {
							// we will only upsert and object if this current version is higher than any previously stored one
							// (if the object has already been deleted, we still allow setting any other fields, including
							// any previously valid full object state... probably not needed, but also not incorrect)
							let mut c = Cursor::new(item.object.as_ref().unwrap());
							doc! {
								"q": doc! { "_id": item.object_id.to_string() },
								// use an aggregation pipeline in our update, so that we can conditionally update
								// the version and object only if the previous version was lower than our current one
								"u": vec![doc! {
									"$set": {
										"_id": item.object_id.to_string(),
										"version": {"$cond": { "if": { "$lt": [ "$version_", v_ ] }, "then": v.clone(), "else": "$version" }},
										"version_": {"$cond": { "if": { "$lt": [ "$version_", v_ ] }, "then": v_, "else": "$version_" }},
										"object": {"$cond": { "if": { "$lt": [ "$version_", v_ ] }, "then": Document::from_reader(&mut c).unwrap(), "else": "$object" }},
									},
								}],
								"upsert": true,
								"multi": false,
							}
						}
					}
				}).collect::<Vec<_>>();
				let n = updates.len();
				let res = db.run_command(doc! {
					"update": collection_name,
					"updates": updates,
				}, None).await;
				match res {
					Ok(res) => {
						// res: {n: i32, upserted: [{index: i32, _id: String}, ...], nModified: i32, writeErrors: [{index: i32, code: i32}, ...]}
						if res.get_i32("n").unwrap() != n as i32 {
							panic!("failed to execute at least one of the upserts: {:#?}", res.get_array("writeErrors").unwrap());
						}
						info!("executed mongo batch with {} items, resulting in {} modified documents", n, res.get_i32("nModified").unwrap());
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
