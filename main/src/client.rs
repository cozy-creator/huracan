use std::time::Duration;

use macros::with_client_rotation;
use sui_sdk::{
	apis::ReadApi,
	error::SuiRpcResult,
	rpc_types::{
		SuiGetPastObjectRequest, SuiObjectDataOptions, SuiObjectResponse, SuiPastObjectResponse,
		SuiTransactionBlockResponseQuery, TransactionBlocksPage,
	},
	SuiClient, SuiClientBuilder,
};
use sui_types::{
	base_types::{ObjectID, SequenceNumber, TransactionDigest},
	messages_checkpoint::CheckpointSequenceNumber,
};
use tokio::time::Instant;

use crate::{_prelude::*, conf::RpcProviderConfig};

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
