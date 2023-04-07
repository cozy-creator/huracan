use {
    crate::_prelude::*,
    crate::conf::{LoaderConfig, PulsarConfig, SuiConfig},
    crate::object_changes_loader::ExtractedObjectChange,
    crate::utils,
};

use anyhow::Result;
use futures::TryStreamExt;
use pulsar::{
    message::proto::MessageIdData, producer, DeserializeMessage, Error as PulsarError,
    SerializeMessage,
};
use relabuf::{ExponentialBackoff, RelaBuf, RelaBufConfig};
use sui_sdk::rpc_types::{ObjectChange, SuiObjectData, SuiObjectDataOptions};
use sui_sdk::SuiClientBuilder;
use sui_types::{base_types::ObjectID, error::SuiObjectResponseError};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrichedObjectChange {
    pub object_change: ExtractedObjectChange,
    pub object: Option<SuiObjectData>,
}

impl From<ExtractedObjectChange> for EnrichedObjectChange {
    fn from(value: ExtractedObjectChange) -> Self {
        Self {
            object_change: value,
            object: None,
        }
    }
}

impl SerializeMessage for EnrichedObjectChange {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl DeserializeMessage for EnrichedObjectChange {
    type Output = Result<Self>;

    fn deserialize_message(payload: &pulsar::Payload) -> Self::Output {
        Ok(
            serde_json::from_slice(&payload.data)
                .map_err(|e| PulsarError::Custom(e.to_string()))?,
        )
    }
}

pub struct ObjectProducer {
    cfg: LoaderConfig,
    pulsar_cfg: PulsarConfig,

    rx_produce: Receiver<(EnrichedObjectChange, MessageIdData)>,
    tx_confirm: Sender<MessageIdData>,
    rx_force_term: Receiver<()>,
}

impl ObjectProducer {
    pub fn new(
        cfg: &LoaderConfig,
        pulsar_cfg: &PulsarConfig,
        rx_produce: Receiver<(EnrichedObjectChange, MessageIdData)>,
        tx_confirm: Sender<MessageIdData>,
        rx_force_term: &Receiver<()>,
    ) -> Self {
        Self {
            cfg: cfg.clone(),
            pulsar_cfg: pulsar_cfg.clone(),
            rx_produce,
            tx_confirm,
            rx_force_term: rx_force_term.clone(),
        }
    }

    pub async fn go(self) -> Result<()> {
        let mut producer = utils::create_pulsar_producer(&utils::PulsarProducerOptions {
            uri: self.pulsar_cfg.uri,
            topic: self.pulsar_cfg.enriched_object_changes.topic,
            producer: self.pulsar_cfg.enriched_object_changes.producer,
            token: self.pulsar_cfg.token,
            batch_size: self.cfg.batcher.soft_cap as u32,
        })
        .await
        .context("cannot create pulsar producer")?;

        let opts = RelaBufConfig {
            soft_cap: self.cfg.batcher.soft_cap,
            hard_cap: self.cfg.batcher.hard_cap,
            release_after: *self.cfg.batcher.release_after,
            backoff: Some(ExponentialBackoff {
                max_elapsed_time: None,
                ..ExponentialBackoff::default()
            }),
        };

        let (buf, proxy) = RelaBuf::new(opts, move || {
            let rx = self.rx_produce.clone();
            Box::pin(async move {
                rx.recv_async()
                    .await
                    .context("cannot read sui object change")
            })
        });

        tokio::spawn(proxy.go());

        loop {
            tokio::select! {
                consumed = buf.next() => {
                    if let Ok(consumed) = consumed {
                        for (ee, _) in consumed.items.clone() {
                            info!(object_change = ?ee, "sending enriched object change to pulsar");
                            let _ = producer.send(ee).await; // we do not track individual pushes, only batch as a whole
                        }

                        if let Err(err) = producer.send_batch().await {
                            error!(error = format!("{err:?}"), "cannot send batch of enriched object changes to pulsar");
                            consumed.return_on_err();
                        } else {
                            consumed.confirm();

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

        info!("Enriched object change producer is going down...");
        Ok(())
    }
}

pub struct ObjectFetcher {
    cfg: LoaderConfig,
    sui_cfg: SuiConfig,
    rx_object_change: Receiver<(ExtractedObjectChange, MessageIdData)>,
    tx_produce: Sender<(EnrichedObjectChange, MessageIdData)>,
    rx_force_term: Receiver<()>,
}

impl ObjectFetcher {
    pub fn new(
        cfg: &LoaderConfig,
        sui_cfg: &SuiConfig,
        rx_object_change: Receiver<(ExtractedObjectChange, MessageIdData)>,
        rx_force_term: &Receiver<()>,
    ) -> (Self, Receiver<(EnrichedObjectChange, MessageIdData)>) {
        let (tx_produce, rx_produce) = bounded_ch(100);
        (
            Self {
                cfg: cfg.clone(),
                sui_cfg: sui_cfg.clone(),
                rx_object_change,
                tx_produce,
                rx_force_term: rx_force_term.clone(),
            },
            rx_produce,
        )
    }

    fn map_object_change_to_object_id(
        c: ExtractedObjectChange,
    ) -> Option<(ObjectID, EnrichedObjectChange)> {
        match c.change {
            ObjectChange::Published { package_id, .. } => Some((package_id, c.into())),
            ObjectChange::Created { object_id, .. } => Some((object_id, c.into())),
            ObjectChange::Mutated { object_id, .. } => Some((object_id, c.into())),
            _ => None,
        }
    }

    pub async fn go(self) -> Result<()> {
        let sui = SuiClientBuilder::default()
            .build(&self.sui_cfg.api.http)
            .await
            .context("cannot create sui client")?;

        let sui_read_api = sui.read_api();

        let opts = RelaBufConfig {
            soft_cap: self.cfg.batcher.soft_cap,
            hard_cap: self.cfg.batcher.hard_cap,
            release_after: *self.cfg.batcher.release_after,
            backoff: Some(ExponentialBackoff {
                max_elapsed_time: None,
                ..ExponentialBackoff::default()
            }),
        };

        let (buf, proxy) = RelaBuf::new(opts, move || {
            let rx = self.rx_object_change.clone();
            Box::pin(async move {
                rx.recv_async()
                    .await
                    .context("cannot read sui object change")
            })
        });

        tokio::spawn(proxy.go());

        let multi_get_object_options = SuiObjectDataOptions {
            show_type: true,
            show_owner: true,
            show_previous_transaction: false,
            show_display: false,
            show_content: true,
            show_bcs: true,
            show_storage_rebate: true,
        };

        loop {
            tokio::select! {
                consumed = buf.next() => {
                    if let Ok(consumed) = consumed {
                        let mut objects_to_get = HashMap::new();
                        let mut objects_to_skip = Vec::new();
                        for (c, message_id) in consumed.items.iter() {
                            if let Some((object_id, c)) = Self::map_object_change_to_object_id(c.clone()) {
                                objects_to_get.insert(object_id, (c, message_id.clone()));
                            } else {
                                objects_to_skip.push((c.clone().into(), message_id.clone()));
                            }
                        }
                        match sui_read_api.multi_get_object_with_options(objects_to_get.keys().cloned().collect(), multi_get_object_options.clone()).await {
                            Err(err) => {
                                error!(error = format!("{err:?}"), "cannot fetch object data for one or more objects");
                                consumed.return_on_err();
                            },
                            Ok(objects) => {
                                let mut one_err = false;

                                for obj in objects {
                                    if let Some(err) = obj.error {
                                        error!(error = format!("{err:?}"), "cannot fetch object data for one object");
                                        match err {
                                            SuiObjectResponseError::Deleted{ object_id, digest, version } => {
                                                info!(object_id = ?object_id, version = ?version, digest = ?digest, object = ?obj.data, "object is in some further object change, skipping for now");
                                                continue;
                                            },
                                            _ => {
                                                error!(error = format!("{err:?}"), "cannot fetch object data for one object");
                                                one_err = true;
                                                break
                                            }
                                        }
                                    }

                                    if let Some(data) = obj.data {
                                        let entry = objects_to_get.get_mut(&data.object_id);
                                        entry.unwrap().0.object = Some(data);
                                    }
                                }

                                if one_err {
                                    consumed.return_on_err();
                                } else {
                                    consumed.confirm();
                                    for (_, c) in objects_to_get {
                                        let _ = self.tx_produce.send_async(c).await;
                                    }
                                    for c in objects_to_skip {
                                        let _ = self.tx_produce.send_async(c).await;
                                    }
                                }
                            }
                        }
                    } else {
                        break
                    }
                },
                _ = &mut self.rx_force_term.recv_async() => {
                    info!("Object fetcher is terminated by a force signal...");
                    return Ok(())
                },
            }
        }

        info!("Object fetcher is going down...");
        Ok(())
    }
}

pub struct PulsarConsumer {
    pulsar_cfg: PulsarConfig,
    tx_object_change: Sender<(ExtractedObjectChange, MessageIdData)>,
    rx_confirm: Receiver<MessageIdData>,
    rx_term: Receiver<()>,
}

impl PulsarConsumer {
    pub fn new(
        pulsar_cfg: &PulsarConfig,
        rx_term: &Receiver<()>,
    ) -> (
        Self,
        Receiver<(ExtractedObjectChange, MessageIdData)>,
        Sender<MessageIdData>,
    ) {
        let (tx_object_change, rx_object_change) = bounded_ch(100);
        let (tx_confirm, rx_confirm) = bounded_ch(100);

        (
            Self {
                pulsar_cfg: pulsar_cfg.clone(),
                tx_object_change,
                rx_confirm,
                rx_term: rx_term.clone(),
            },
            rx_object_change,
            tx_confirm,
        )
    }

    pub async fn go(self) -> Result<()> {
        let topic = self.pulsar_cfg.object_changes.topic.clone();

        let mut consumer =
            utils::create_pulsar_consumer::<ExtractedObjectChange>(&utils::PulsarConsumerOptions {
                uri: self.pulsar_cfg.uri,
                topic: self.pulsar_cfg.object_changes.topic,
                consumer: self.pulsar_cfg.object_changes.consumer,
                subscription: self.pulsar_cfg.object_changes.subscription,
                token: self.pulsar_cfg.token,
            })
            .await
            .context("cannot create pulsar producer")?;

        let rx_term = self.rx_term.clone();

        info!("Starting to consume sui object changes...");
        loop {
            tokio::select! {
                consumed = &mut consumer.try_next() => {
                    if let Ok(Some(consumed)) = consumed {
                        let message_id = consumed.message_id.id.clone();
                        let object_change = consumed.deserialize().context("cannot deserialize extracted sui object change")?;

                        let _ = self.tx_object_change.send_async((object_change, message_id)).await;
                    } else {
                        break
                    }
                },
                id = &mut self.rx_confirm.recv_async() => {
                    let id = id.expect("to be able to always read from confirmation channel");
                    info!(id = ?id, "acking object change");
                    consumer.ack_with_id(&topic, id).await.context("cannot acknowledge message")?;
                },
                _ = &mut rx_term.recv_async() => {
                    info!("Object changes consumer is terminated by a signal...");
                    return Ok(())
                },
            }
        }

        info!("Object changes consumer is terminated normally...");
        Ok(())
    }
}
