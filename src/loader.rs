use anyhow::Result;
use bson::{self, Document};
use mongodb::{bson::doc, options::ClientOptions, Client, Collection};
use pulsar::message::proto::MessageIdData;
use relabuf::{ExponentialBackoff, RelaBuf, RelaBufConfig};
use sui_sdk::rpc_types::ObjectChange;

use crate::{
	_prelude::*,
	conf::{LoaderConfig, MongoConfig, PulsarConfig},
	transformer::EnrichedObjectChange,
	utils,
};

#[allow(dead_code)]
pub struct Loader {
	cfg:       LoaderConfig,
	mongo_cfg: MongoConfig,

	rx:            Receiver<(EnrichedObjectChange, MessageIdData)>,
	tx_confirm:    Sender<MessageIdData>,
	rx_force_term: Receiver<()>,
}

impl Loader {
	pub fn new(
		cfg: &LoaderConfig,
		mongo_cfg: &MongoConfig,
		rx: Receiver<(EnrichedObjectChange, MessageIdData)>,
		tx_confirm: Sender<MessageIdData>,
		rx_force_term: &Receiver<()>,
	) -> Self {
		Self { cfg: cfg.clone(), mongo_cfg: mongo_cfg.clone(), rx, tx_confirm, rx_force_term: rx_force_term.clone() }
	}

	async fn process_object_change(collection: &mut Collection<Document>, change: &EnrichedObjectChange) -> Result<()> {
		match change.object_change.change {
			ObjectChange::Deleted { object_id, version, .. } => {
				let filter = doc! { "_id": object_id.to_string(), "version": version.to_string() };
				info!(object_id = ?object_id, version = ?version, "deleting object");
				collection.delete_one(filter, None).await?;
			}
			ObjectChange::Created { object_id, version, .. } => {
				if let Some(object) = &change.object {
					info!(object_id = ?object_id, version = ?version, "inserting object");
					let serialized_change = bson::to_bson(object)?;
					let document = serialized_change.as_document().context("cannot convert to bson document")?;

					collection
						.insert_one(
							doc! {
								"_id": object_id.to_string(),
								 "version": version.to_string(),
								 "object": document,
							},
							None,
						)
						.await?;
				} else {
					warn!(object_id = ?object_id, version = ?version, "cannot insert object, - missing object data")
				}
			}
			ObjectChange::Mutated { object_id, version, .. } => {
				if let Some(object) = &change.object {
					let serialized_change = bson::to_bson(object)?;
					let document = serialized_change.as_document().context("cannot convert to bson document")?;

					info!(object_id = ?object_id, version = ?version, "mutating object");
					let filter = doc! { "_id": object_id.to_string(), "version": version.to_string() };

					collection
						.update_one(
							filter,
							doc! {
								"$set": {
									"object": document,
								}
							},
							None,
						)
						.await?;
				} else {
					warn!(object_id = ?object_id, version = ?version, "cannot mutate object, - missing object data")
				}
			}
			_ => {}
		}
		Ok(())
	}

	pub async fn go(self) -> Result<()> {
		let client_options = ClientOptions::parse(self.mongo_cfg.uri).await?;

		let client = Client::with_options(client_options)?;
		let db = client.database(&self.mongo_cfg.database);
		let mut objects_collection = db.collection(&self.mongo_cfg.objects.collection);

		let opts = RelaBufConfig {
			soft_cap:      self.cfg.batcher.soft_cap,
			hard_cap:      self.cfg.batcher.hard_cap,
			release_after: *self.cfg.batcher.release_after,
			backoff:       Some(ExponentialBackoff { max_elapsed_time: None, ..ExponentialBackoff::default() }),
		};

		let (buf, proxy) = RelaBuf::new(opts, move || {
			let rx = self.rx.clone();
			Box::pin(async move { rx.recv_async().await.context("cannot read sui object change") })
		});

		tokio::spawn(proxy.go());

		loop {
			tokio::select! {
				consumed = buf.next() => {
					if let Ok(consumed) = consumed {
						let mut is_err = false;
						for (change, _) in &consumed.items {
							if let Err(err) = Self::process_object_change(&mut objects_collection, change).await {
								warn!(err = ?err, "cannot process object change");
								is_err = true;
								break
							}
						}

						if is_err {
							consumed.return_on_err();
						} else {
							for (_, message_id) in consumed.items {
								let _ = self.tx_confirm.send_async(message_id).await;
							}
						}
					} else {
						break
					}
				},

				_ = &mut self.rx_force_term.recv_async() => {
					info!("Enriched object change producer is terminated by a force signal...");
					return Ok(())
				},
			}
		}

		info!("Mongo loader is going down...");
		Ok(())
	}
}

pub struct PulsarConfirmer {
	pulsar_cfg: PulsarConfig,
	rx_confirm: Receiver<MessageIdData>,
	rx_term:    Receiver<()>,
}

impl PulsarConfirmer {
	pub fn new(
		pulsar_cfg: &PulsarConfig,
		loader_cfg: &LoaderConfig,
		rx_term: &Receiver<()>,
	) -> (Self, Sender<MessageIdData>) {
		let (tx_confirm, rx_confirm) = bounded_ch(loader_cfg.buffer_size);
		(Self { pulsar_cfg: pulsar_cfg.clone(), rx_confirm, rx_term: rx_term.clone() }, tx_confirm)
	}

	pub async fn go(self) -> Result<()> {
		let topic = self.pulsar_cfg.object_changes.topic.clone();

		let mut consumer = utils::create_pulsar_consumer::<EnrichedObjectChange>(&utils::PulsarConsumerOptions {
			uri:          self.pulsar_cfg.uri,
			topic:        self.pulsar_cfg.enriched_object_changes.topic,
			consumer:     self.pulsar_cfg.enriched_object_changes.consumer,
			subscription: self.pulsar_cfg.enriched_object_changes.subscription,
			token:        self.pulsar_cfg.token,
		})
		.await?;

		loop {
			tokio::select! {
				id = &mut self.rx_confirm.recv_async() => {
					if let Ok(id) = id {
						info!(id = ?id, "acking object change");
						consumer.ack_with_id(&topic, id).await.context("cannot acknowledge message")?;
					} else {
						break
					}
				},
				_ = &mut self.rx_term.recv_async() => {
					info!("Pulsar confirmer is terminated by a signal...");
					return Ok(())
				},
			}
		}

		info!("Pulsar confirmer is terminated normally...");
		Ok(())
	}
}

#[allow(dead_code)]
pub struct PulsarConsumer {
	pulsar_cfg:       PulsarConfig,
	tx_object_change: Sender<(EnrichedObjectChange, MessageIdData)>,
	rx_term:          Receiver<()>,
}

impl PulsarConsumer {
	pub fn new(
		pulsar_cfg: &PulsarConfig,
		loader_cfg: &LoaderConfig,
		rx_term: &Receiver<()>,
	) -> (Self, Receiver<(EnrichedObjectChange, MessageIdData)>) {
		let (tx_object_change, rx_object_change) = bounded_ch(loader_cfg.buffer_size);

		(Self { pulsar_cfg: pulsar_cfg.clone(), tx_object_change, rx_term: rx_term.clone() }, rx_object_change)
	}

	pub async fn go(self) -> Result<()> {
		let mut consumer = utils::create_pulsar_consumer::<EnrichedObjectChange>(&utils::PulsarConsumerOptions {
			uri:          self.pulsar_cfg.uri,
			topic:        self.pulsar_cfg.enriched_object_changes.topic,
			consumer:     self.pulsar_cfg.enriched_object_changes.consumer,
			subscription: self.pulsar_cfg.enriched_object_changes.subscription,
			token:        self.pulsar_cfg.token,
		})
		.await?;

		info!("Starting to consume enriched sui object changes...");
		loop {
			tokio::select! {
				consumed = &mut consumer.try_next() => {
					if let Ok(Some(consumed)) = consumed {
						let message_id = consumed.message_id.id.clone();
						let object_change = consumed.deserialize().context("cannot deserialize enriched sui object change")?;

						let _ = self.tx_object_change.send_async((object_change, message_id)).await;
					} else {
						break
					}
				},
				_ = &mut self.rx_term.recv_async() => {
					info!("Object changes consumer is terminated by a signal...");
					return Ok(())
				},
			}
		}

		info!("Object changes consumer is terminated normally...");
		Ok(())
	}
}
