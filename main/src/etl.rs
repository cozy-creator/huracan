use std::iter::zip;

use anyhow::Result;
use async_stream::stream;
use futures::Stream;
use futures_batch::ChunksTimeoutStreamExt;
use sui_sdk::{
	apis::ReadApi,
	rpc_types::{
		ObjectChange as SuiObjectChange, SuiGetPastObjectRequest, SuiObjectData, SuiObjectDataOptions,
		SuiPastObjectResponse, SuiTransactionBlockResponseOptions, SuiTransactionBlockResponseQuery,
	},
};
use sui_types::base_types::SequenceNumber;

use crate::_prelude::*;

// sui allows a max of 50 objects to be queried for at once, at least on some endpoints
// (e.g. on `try_multi_get_parsed_past_object`)
const SUI_QUERY_MAX_RESULT_LIMIT: usize = 50;

#[derive(Clone, Debug, Serialize, Deserialize, PulsarMessage)]
pub struct ObjectSnapshot {
	pub change: SuiObjectChange,
	pub object: Option<SuiObjectData>,
}

impl ObjectSnapshot {
	pub fn new(change: SuiObjectChange) -> Self {
		Self { change, object: None }
	}
}

impl ObjectSnapshot {
	pub fn get_change_version(&self) -> SequenceNumber {
		use SuiObjectChange::*;
		match &self.change {
			Published { version, .. }
			| Created { version, .. }
			| Mutated { version, .. }
			| Transferred { version, .. }
			| Deleted { version, .. }
			| Wrapped { version, .. } => *version,
		}
	}

	pub fn get_past_object_request(&self) -> SuiGetPastObjectRequest {
		SuiGetPastObjectRequest { object_id: self.change.object_id(), version: self.get_change_version() }
	}

	// TODO is this exactly what we want? skip fetching objects for anything but these 3 types of changes?
	pub fn skip_fetching_object(&self) -> bool {
		use SuiObjectChange::*;
		match &self.change {
			Published { .. } | Created { .. } | Mutated { .. } => false,
			_ => true,
		}
	}
}

pub async fn extract<'a>(
	sui: &'a ReadApi,
	mut rx_term: tokio::sync::oneshot::Receiver<()>,
) -> Result<impl Stream<Item = ObjectSnapshot> + 'a> {
	let q = SuiTransactionBlockResponseQuery::new(
		None,
		Some(SuiTransactionBlockResponseOptions::new().with_object_changes()),
	);
	let mut cursor = None;

	Ok(stream! {
		loop {
			let start = Instant::now();
			tokio::select! {
				page = sui.query_transaction_blocks(q.clone(), cursor, Some(SUI_QUERY_MAX_RESULT_LIMIT), false) => {
					match page {
						Ok(page) => {
							// if we're working faster than sui has new data for us, sleep a little
							// (as long as a query usually takes seems like a good duration, but at least 250ms)
							if page.data.len() == 0 {
								let dur = std::cmp::max(Duration::from_millis(250), start.elapsed());
								info!("no new results from sui, will wait {}ms before trying again", dur.as_millis());
								tokio::time::sleep(dur).await;
								continue;
							}
							for item in page.data {
								if let Some(changes) = item.object_changes {
									for change in changes {
										yield ObjectSnapshot::new(change);
									}
								}
							}
							cursor = page.next_cursor;
						},
						Err(err) => {
							warn!(error = ?err, "There was an error reading object changes... retrying");
						}
					}
				}
				_ = &mut rx_term => {
					info!("shutting down extract()");
					break
				}
			}
		}
	})
}

pub enum StepStatus {
	Ok,
	Err,
}

pub async fn transform<'a, S: Stream<Item = ObjectSnapshot> + 'a>(
	stream: S,
	sui: &'a ReadApi,
) -> impl Stream<Item = (StepStatus, ObjectSnapshot)> + 'a {
	// batch incoming items so we can amortize the cost of sui api calls,
	// but send them off one by one, so any downstream consumer (e.g. Pulsar client) can apply their
	// own batching logic, if necessary (e.g. Pulsar producer will auto-batch transparently, if configured)

	let stream = stream.chunks_timeout(SUI_QUERY_MAX_RESULT_LIMIT, Duration::from_millis(1_000));

	let query_opts = SuiObjectDataOptions {
		show_type:                 true,
		show_owner:                true,
		show_previous_transaction: false,
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
				info!(object_id = ?o.object_id, version = ?o.version, digest = ?o.digest, "object is in some further object change, skipping for now");
			}
			ObjectNotExists(object_id) => {
				info!(object_id = ?object_id, "object doesn't exist");
			}
			VersionNotFound(object_id, version) => {
				info!(object_id = ?object_id, version = ?version, "object not found");
			}
			VersionTooHigh { object_id, asked_version, latest_version } => {
				info!(object_id = ?object_id, asked_version = ?asked_version, latest_version = ?latest_version, "object version too high");
			}
		};
		None
	}

	stream! {
		for await mut chunk in stream {
			// filter and remove changes that we shouldn't fetch objects for, and stream them as is
			let skip = chunk.drain_filter(|o| o.skip_fetching_object()).collect::<Vec<_>>();
			for item in skip {
				yield (StepStatus::Ok, item);
			}
			let query_objs = chunk.iter().map(|o| o.get_past_object_request()).collect::<Vec<_>>();
			match sui.try_multi_get_parsed_past_object(query_objs, query_opts.clone()).await {
				Err(err) => {
					warn!(error = format!("{err:?}"), "cannot fetch object data for one or more objects, retrying them individually");
					// try one by one
					// TODO this should be super easy to do in parallel, firing off the reqs on some tokio thread pool executor
					for mut snapshot in chunk {
						match sui.try_get_parsed_past_object(snapshot.change.object_id(), snapshot.get_change_version(), query_opts.clone()).await {
							Err(err) => {
								// TODO add info about object to log
								error!(error = format!("{err:?}"), "individual fetch also failed");
								yield (StepStatus::Err, snapshot);
							},
							Ok(res) => {
								if let Some(obj) = parse_past_object_response(res) {
									snapshot.object = Some(obj);
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
							snapshot.object = Some(obj);
							yield (StepStatus::Ok, snapshot);
						}
					}
				}
			}
		}
	}
}
