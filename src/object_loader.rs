use {
    crate::_prelude::*,
    crate::conf::{LoaderConfig, PulsarConfig, SuiConfig},
};

use anyhow::Result;
use relabuf::{ExponentialBackoff, RelaBuf, RelaBufConfig};
use sui_sdk::rpc_types::{SuiObjectData, SuiObjectDataOptions};
use sui_sdk::SuiClientBuilder;

use pulsar::{
    consumer, message::proto::MessageIdData, producer, Authentication, Consumer,
    DeserializeMessage, Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
};

use crate::event_loader::ExtractedEvent;

pub struct ObjectProducer {
    _rx_produce: Receiver<EnrichedEvent>,
    _tx_confirm: Sender<MessageIdData>,
}

impl ObjectProducer {
    pub fn new(rx_produce: Receiver<EnrichedEvent>, tx_confirm: Sender<MessageIdData>) -> Self {
        Self {
            _rx_produce: rx_produce,
            _tx_confirm: tx_confirm,
        }
    }

    pub async fn go(self) -> Result<()> {
        Ok(())
    }
}
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

pub struct ObjectFetcher {
    cfg: LoaderConfig,
    sui_cfg: SuiConfig,
    rx_event: Receiver<(ExtractedEvent, MessageIdData)>,
    _tx_produce: Sender<EnrichedEvent>,
    rx_force_term: Receiver<()>,
}

impl ObjectFetcher {
    pub fn new(
        cfg: &LoaderConfig,
        sui_cfg: &SuiConfig,
        rx_event: Receiver<(ExtractedEvent, MessageIdData)>,
        rx_force_term: &Receiver<()>,
    ) -> (Self, Receiver<EnrichedEvent>) {
        let (tx_produce, rx_produce) = bounded_ch(100);
        (
            Self {
                cfg: cfg.clone(),
                sui_cfg: sui_cfg.clone(),
                rx_event,
                _tx_produce: tx_produce,
                rx_force_term: rx_force_term.clone(),
            },
            rx_produce,
        )
    }

    pub async fn go(self) -> Result<()> {
        let sui = SuiClientBuilder::default()
            .ws_url(&self.sui_cfg.api.ws)
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
                                for res in objects {
                                    if let Some(err) = res.error {
                                        error!(error = format!("{err:?}"), "cannot fetch object data for one object");
                                        one_err = true;
                                        break
                                    }
                                }

                                /*let zip = consumed.items.iter().zip(objects.iter().map(|obj|obj.data));

                                for res in zip {
                                    self.tx_produce.send_async(EnrichedEvent{
                                        event: zip.0,
                                        object: zip.1,
                                    }).await.expected("to always send enriched event");
                                }*/

                                if one_err {
                                    consumed.return_on_err();
                                } else {
                                    consumed.confirm();
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

    async fn create_pulsar_consumer(&self) -> Result<Consumer<ExtractedEvent, TokioExecutor>> {
        let mut builder = Pulsar::builder(&self.pulsar_cfg.uri, TokioExecutor);

        if let Some(token) = &self.pulsar_cfg.token {
            let auth = Authentication {
                name: "token".to_string(),
                data: token.clone().into_bytes(),
            };
            builder = builder.with_auth(auth);
        }

        let pulsar: Pulsar<_> = builder.build().await?;

        let mut consumer = pulsar
            .consumer()
            .with_topic(&self.pulsar_cfg.events.topic)
            .with_consumer_name(&self.pulsar_cfg.events.consumer)
            .with_options(consumer::ConsumerOptions {
                durable: Some(true),
                ..Default::default()
            })
            .build()
            .await
            .context("cannot create apache pulsar producer")?;

        consumer
            .check_connection()
            .await
            .context("cannot check apache pulsar connection")?;

        Ok(consumer)
    }

    pub async fn go(self) -> Result<()> {
        let mut consumer = self
            .create_pulsar_consumer()
            .await
            .context("cannot create pulsar producer")?;

        let rx_force_term = self.rx_force_term.clone();

        loop {
            tokio::select! {
                consumed = consumer.next() => {
                    if let Some(Ok(consumed)) = consumed {
                        let message_id = consumed.message_id.id.clone();
                        let event = consumed.deserialize().context("cannot deserialize extracted sui event")?;

                        self.tx_event.send_async((event, message_id)).await.expect("to always be able to send an event");
                    } else {
                        break
                    }
                },
                id = self.rx_confirm.recv_async() => {
                    if let Ok(id) = id {
                        consumer.cumulative_ack_with_id(&self.pulsar_cfg.events.topic, id).await.context("cannot acknowledge message")?;
                    }
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
