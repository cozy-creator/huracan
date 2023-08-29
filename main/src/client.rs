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
use tokio::time::Instant;
use crate::{_prelude::*, conf::RpcProviderConfig, utils::check_string_against_regex, main::APPCONFIG};
use crate::conf::get_config_singleton;

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

pub fn parse_get_object_response(id: &ObjectID, res: SuiObjectResponse) -> Option<(VersionNumber, Vec<u8>)> {
	if let Some(err) = res.error {
		use sui_types::error::SuiObjectResponseError::*;
		match err {
			Deleted { object_id, version, digest: _ } => {
				warn!(object_id = ?object_id, version = ?version, "SuiObjectResponseError : Deleted");
			}
			NotExists { object_id } => {
				warn!(object_id = ?object_id, "SuiObjectResponseError : NotExists");
			}
			Unknown => {
				warn!("SuiObjectResponseError : Unknown");
			}
			DisplayError { error } => {
				warn!("SuiObjectResponseError : DisplayError : {}", error);
			}
			ref _e @ DynamicFieldNotFound { parent_object_id } => {
				warn!(parent_object_id = ?parent_object_id, "DynamicFieldNotFound error.");
			}
		};
		return None
	}
	if let Some(obj) = res.data {
		let obj_type = obj.object_type().ok()?;
		info!("Outside whitelist conditional object type is: {}", &obj_type);
		let str_obj_type = serde_json::to_string(&obj_type).ok()?;
		let whitelist_enabled = get_config_singleton().whitelist.clone().enabled;
		let whitelist_packages = get_config_singleton().whitelist.clone().packages;
		if whitelist_packages != None && whitelist_enabled == true && check_string_against_regex(&str_obj_type, whitelist_packages.unwrap()) == true {
			info!("Inside whitelist conditional type is: {}", &str_obj_type);
			let mut bytes = Vec::with_capacity(4096);
			let bson = bson::to_bson(&obj).unwrap();
			bson.as_document().unwrap().to_writer(&mut bytes).unwrap();
			return Some((obj.version, bytes))
		}
	}
	warn!(object_id = ?id, "ExtractionError : neither .data nor .error was set in get_object response!");
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
