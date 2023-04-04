use {
    crate::_prelude::*,
    crate::conf::{LoaderConfig, PulsarConfig, SuiConfig},
    crate::event_loader::ExtractedEvent,
    crate::utils,
};

use anyhow::Result;
use futures::TryStreamExt;
use pulsar::{
    message::proto::MessageIdData, producer, DeserializeMessage, Error as PulsarError,
    SerializeMessage,
};
use relabuf::{ExponentialBackoff, RelaBuf, RelaBufConfig};
use sui_sdk::rpc_types::{SuiObjectData, SuiObjectDataOptions};
use sui_sdk::SuiClientBuilder;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrichedEvent {
    pub event: ExtractedEvent,
    pub object: Option<SuiObjectData>,
}

impl SerializeMessage for EnrichedEvent {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl DeserializeMessage for EnrichedEvent {
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

    rx_produce: Receiver<(EnrichedEvent, MessageIdData)>,
    tx_confirm: Sender<MessageIdData>,
    rx_force_term: Receiver<()>,
}

impl ObjectProducer {
    pub fn new(
        cfg: &LoaderConfig,
        pulsar_cfg: &PulsarConfig,
        rx_produce: Receiver<(EnrichedEvent, MessageIdData)>,
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
            topic: self.pulsar_cfg.enriched_events.topic,
            producer: self.pulsar_cfg.enriched_events.producer,
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
            Box::pin(async move { rx.recv_async().await.context("cannot read sui event") })
        });

        tokio::spawn(proxy.go());

        loop {
            tokio::select! {
                consumed = buf.next() => {
                    if let Ok(consumed) = consumed {
                        for (ee, _) in consumed.items.clone() {
                            let _ = producer.send(ee).await; // we do not track individual pushes, only batch as a whole
                        }

                        if let Err(err) = producer.send_batch().await {
                            error!(error = format!("{err:?}"), "cannot send batch of enriched events to pulsar");
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
                    info!("Enriched event producer is terminated by a force signal...");
                    return Ok(())
                },
            }
        }

        Ok(())
    }
}

pub struct ObjectFetcher {
    cfg: LoaderConfig,
    sui_cfg: SuiConfig,
    rx_event: Receiver<(ExtractedEvent, MessageIdData)>,
    tx_produce: Sender<(EnrichedEvent, MessageIdData)>,
    rx_force_term: Receiver<()>,
}

impl ObjectFetcher {
    pub fn new(
        cfg: &LoaderConfig,
        sui_cfg: &SuiConfig,
        rx_event: Receiver<(ExtractedEvent, MessageIdData)>,
        rx_force_term: &Receiver<()>,
    ) -> (Self, Receiver<(EnrichedEvent, MessageIdData)>) {
        let (tx_produce, rx_produce) = bounded_ch(100);
        (
            Self {
                cfg: cfg.clone(),
                sui_cfg: sui_cfg.clone(),
                rx_event,
                tx_produce,
                rx_force_term: rx_force_term.clone(),
            },
            rx_produce,
        )
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
            let rx = self.rx_event.clone();
            Box::pin(async move { rx.recv_async().await.context("cannot read sui event") })
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
                        match sui_read_api.multi_get_object_with_options(consumed.items.iter().map(|(ev, _)| ev.object_id).collect(), multi_get_object_options.clone()).await {
                            Err(err) => {
                                error!(error = format!("{err:?}"), "cannot fetch object data for one or more objects");
                                consumed.return_on_err();
                            },
                            Ok(objects) => {
                                let mut one_err = false;
                                for res in &objects {
                                    if let Some(err) = &res.error {
                                        error!(error = format!("{err:?}"), "cannot fetch object data for one object");
                                        one_err = true;
                                        break
                                    }
                                }

                                if one_err {
                                    consumed.return_on_err();
                                } else {
                                    consumed.confirm();

                                    let zip = consumed.items.into_iter().zip(objects.into_iter().map(|obj|obj.data));

                                    for ((event, message_id), object) in zip {
                                        self.tx_produce.send_async((EnrichedEvent{
                                            event,
                                            object,
                                        }, message_id)).await.expect("to always send enriched event");
                                    }
                                }
                            }
                        }

                    } else {
                        break
                    }
                },
                _ = &mut self.rx_force_term.recv_async() => {
                    info!("Event loader is terminated by a force signal...");
                    return Ok(())
                },
            }
        }

        Ok(())
    }
}

pub struct PulsarConsumer {
    pulsar_cfg: PulsarConfig,
    tx_event: Sender<(ExtractedEvent, MessageIdData)>,
    rx_confirm: Receiver<MessageIdData>,
    rx_force_term: Receiver<()>,
}

impl PulsarConsumer {
    pub fn new(
        pulsar_cfg: &PulsarConfig,
        rx_force_term: &Receiver<()>,
    ) -> (
        Self,
        Receiver<(ExtractedEvent, MessageIdData)>,
        Sender<MessageIdData>,
    ) {
        let (tx_event, rx_event) = bounded_ch(100);
        let (tx_confirm, rx_confirm) = bounded_ch(100);

        (
            Self {
                pulsar_cfg: pulsar_cfg.clone(),
                tx_event,
                rx_confirm,
                rx_force_term: rx_force_term.clone(),
            },
            rx_event,
            tx_confirm,
        )
    }

    pub async fn go(self) -> Result<()> {
        let topic = self.pulsar_cfg.events.topic.clone();

        let mut consumer =
            utils::create_pulsar_consumer::<ExtractedEvent>(&utils::PulsarConsumerOptions {
                uri: self.pulsar_cfg.uri,
                topic: self.pulsar_cfg.events.topic,
                consumer: self.pulsar_cfg.events.consumer,
                subscription: self.pulsar_cfg.events.subscription,
                token: self.pulsar_cfg.token,
            })
            .await
            .context("cannot create pulsar producer")?;

        let rx_force_term = self.rx_force_term.clone();

        info!("Starting to consume sui events...");
        loop {
            tokio::select! {
                consumed = consumer.try_next() => {
                    if let Ok(Some(consumed)) = consumed {
                        let message_id = consumed.message_id.id.clone();
                        let event = consumed.deserialize().context("cannot deserialize extracted sui event")?;

                        info!(event = ?event, "working on event");
                        self.tx_event.send_async((event, message_id)).await.expect("to always be able to send an event");
                    } else {
                        break
                    }
                },
                id = self.rx_confirm.recv_async() => {
                    let id = id.expect("to be able to always read from confirmation channel");
                    consumer.ack_with_id(&topic, id).await.context("cannot acknowledge message")?;
                },
                _ = rx_force_term.recv_async() => {
                    info!("Event consumer is terminated by a force signal...");
                    return Ok(())
                },
            }
        }

        info!("Event consumer is terminated normally...");
        Ok(())
    }
}
