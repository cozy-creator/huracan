use std::time::Duration;
use macros::with_client_rotation;
use sui_sdk::{
	apis::ReadApi,
	error::SuiRpcResult,
	rpc_types::{
		ObjectChange as SuiObjectChange, SuiGetPastObjectRequest, SuiObjectDataOptions, SuiObjectResponse,
		SuiPastObjectResponse, SuiTransactionBlockResponseQuery, TransactionBlocksPage,
	},
	SuiClient, SuiClientBuilder,
};
use sui_types::{
	base_types::{ObjectID, SequenceNumber, TransactionDigest, VersionNumber},
	messages_checkpoint::CheckpointSequenceNumber,
};
use sui_types::error::SuiObjectResponseError::*;
use tokio::time::Instant;
use crate::{_prelude::*, conf::RpcProviderConfig, utils::check_obj_type_from_string_vec};
use crate::conf::get_config_singleton;
use crate::influx::{write_metric_ingest_error, get_influx_timestamp_as_milliseconds, write_metric_rpc_request};


#[derive(Clone)]
pub struct ClientPool {
	pub configs: Vec<RpcProviderConfig>,
	clients:     Vec<Client>,
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
	pub async fn new(configs: Vec<RpcProviderConfig>) -> anyhow::Result<Self> {
		let clients = Vec::with_capacity(configs.len());
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

	async fn make_client(&self, id: usize) -> anyhow::Result<Client> {
		let config = self.configs[id].clone();
		let sui = SuiClientBuilder::default().build(&config.url).await?;
		Ok(Client { id, config, sui, backoff: None, reqs: 0 })
	}
}

pub async fn parse_get_object_response(id: &ObjectID, res: SuiObjectResponse) -> Option<(VersionNumber, Vec<u8>)> {
	if let Some(err) = res.error {
		match err {
			Deleted { object_id, version, digest: _ } => {
				warn!(object_id = ?object_id, version = ?version, "SuiObjectResponseError : Deleted");
				write_metric_ingest_error(object_id.to_string(), "sui_object_deleted".to_string()).await;
			}
			NotExists { object_id } => {
				warn!(object_id = ?object_id, "SuiObjectResponseError : NotExists");
				write_metric_ingest_error(object_id.to_string(), "sui_object_not_exists".to_string()).await;
			}
			Unknown => {
				warn!("SuiObjectResponseError : Unknown");
				write_metric_ingest_error("unknown".to_string(), "sui_object_unknown".to_string()).await;
			}
			DisplayError { error } => {
				warn!("SuiObjectResponseError : DisplayError : {}", error);
				write_metric_ingest_error("unknown".to_string(), "sui_object_display_error".to_string()).await;
			}
			ref _e @ DynamicFieldNotFound { parent_object_id } => {
				warn!(parent_object_id = ?parent_object_id, "DynamicFieldNotFound error.");
				write_metric_ingest_error("unknown".to_string(), "sui_object_dynamic_field_not_found".to_string()).await;
			}
		};
		return None
	}
	if let Some(obj) = res.data {
		let obj_type = obj.object_type().ok()?;
		let whitelist_enabled = get_config_singleton().whitelist.clone().enabled;
		let whitelist_packages = get_config_singleton().whitelist.clone().packages;
		let blacklist_enabled = get_config_singleton().blacklist.clone().enabled;
		let blacklist_packages = get_config_singleton().blacklist.clone().packages;
		// Index all objects.
		if whitelist_enabled == false && blacklist_enabled == false {
			let mut bytes = Vec::with_capacity(4096);
			let bson = bson::to_bson(&obj).unwrap();
			bson.as_document().unwrap().to_writer(&mut bytes).unwrap();
			return Some((obj.version, bytes))
		}
		// Index only whitelisted objects.
		if whitelist_packages != None && whitelist_enabled == true && check_obj_type_from_string_vec(&obj_type, whitelist_packages.unwrap()) == true {
			let mut bytes = Vec::with_capacity(4096);
			let bson = bson::to_bson(&obj).unwrap();
			bson.as_document().unwrap().to_writer(&mut bytes).unwrap();
			return Some((obj.version, bytes))
		}
		// Index everything except blacklisted objects.
		if blacklist_packages != None && blacklist_enabled == true && check_obj_type_from_string_vec(&obj_type, blacklist_packages.unwrap()) == false {
			let mut bytes = Vec::with_capacity(4096);
			let bson = bson::to_bson(&obj).unwrap();
			bson.as_document().unwrap().to_writer(&mut bytes).unwrap();
			return Some((obj.version, bytes))
		}
	}
	// TODO: Determine root cause of this error.
	info!(object_id = ?id, "ExtractionError : neither .data nor .error was set in get_object response.");
	write_metric_ingest_error(id.to_string(), "sui_object_no_data_and_no_error".to_string()).await;
	return None
}

pub fn parse_change(change: SuiObjectChange) -> Option<(ObjectID, SequenceNumber, bool)> {
	use sui_sdk::rpc_types::ObjectChange::*;
	Some(match change {
		// TODO what about Wrapped and Transferred? at least when walking towards genesis we want to know
		//		about an object asap for indexing its latest state or ignoring it for the rest of the walk
		// TODO here we can also get the struct tag and filter out some types we already know we're not interested in (e.g. Clock)
		// TODO Wrapped + Transferred: I think we can infer some situations where the obj is no longer accessible
		//		externally and can mark it as such, without having to make another query
		Created { object_id, version, .. } | Mutated { object_id, version, .. } => (object_id, version, false),
		Deleted { object_id, version, .. } => (object_id, version, true),
		_ => return None,
	})
}
