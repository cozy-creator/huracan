use anyhow::Result;
use pulsar::message::proto::MessageIdData;

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

	pub async fn go(self) -> Result<()> {
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
