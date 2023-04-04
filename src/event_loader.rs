use {
    crate::_prelude::*,
    crate::conf::{LoaderConfig, PulsarConfig, SuiConfig},
    crate::utils,
};

use anyhow::Result;
use pulsar::{producer, DeserializeMessage, Error as PulsarError, SerializeMessage};
use relabuf::{ExponentialBackoff, RelaBuf, RelaBufConfig};
use sui_sdk::rpc_types::SuiEvent;
use sui_sdk::SuiClientBuilder;
use sui_types::base_types::ObjectID;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExtractedEvent {
    pub event: SuiEvent,
    pub object_id: ObjectID,
}

impl SerializeMessage for ExtractedEvent {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl DeserializeMessage for ExtractedEvent {
    type Output = Result<Self>;

    fn deserialize_message(payload: &pulsar::Payload) -> Self::Output {
        Ok(
            serde_json::from_slice(&payload.data)
                .map_err(|e| PulsarError::Custom(e.to_string()))?,
        )
    }
}

pub struct PulsarProducer {
    cfg: LoaderConfig,
    pulsar_cfg: PulsarConfig,

    rx: Receiver<ExtractedEvent>,
    rx_force_term: Receiver<()>,
}

pub struct SuiExtractor {
    rx_term: Receiver<()>,

    tx: Sender<ExtractedEvent>,
    cfg: SuiConfig,
}

impl PulsarProducer {
    pub fn new(
        cfg: &LoaderConfig,
        pulsar_cfg: &PulsarConfig,
        rx_extractor: Receiver<ExtractedEvent>,
        rx_force_term: Receiver<()>,
    ) -> Self {
        Self {
            cfg: cfg.clone(),
            pulsar_cfg: pulsar_cfg.clone(),
            rx: rx_extractor,
            rx_force_term,
        }
    }

    pub async fn go(self) -> Result<()> {
        let mut producer = utils::create_pulsar_producer(&utils::PulsarProducerOptions {
            uri: self.pulsar_cfg.uri,
            topic: self.pulsar_cfg.events.topic,
            producer: self.pulsar_cfg.events.producer,
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
            let rx = self.rx.clone();
            Box::pin(async move { rx.recv_async().await.context("cannot read") })
        });

        tokio::spawn(proxy.go());

        let rx_force_term = self.rx_force_term.clone();

        loop {
            tokio::select! {
                consumed = buf.next() => {
                    if let Ok(consumed) = consumed {

                        for event in consumed.items.clone() {
                            let _ = producer.send(event).await; // we do not track individual pushes, only batch as a whole
                        }

                        if let Err(err) = producer.send_batch().await {
                            error!(error = format!("{err:?}"), "cannot send batch to pulsar");
                            consumed.return_on_err();
                        } else {
                            info!(n = consumed.items.len(), "pushed events to pulsar...");
                            consumed.confirm();
                        }
                    } else {
                        break
                    }
                },
                _ = rx_force_term.recv_async() => {
                    info!("Event producer is terminated by a force signal...");
                    return Ok(())
                },
            }
        }

        info!("Event producer is terminated normally...");
        Ok(())
    }
}

impl SuiExtractor {
    pub fn new(cfg: &SuiConfig, rx_term: Receiver<()>) -> (Self, Receiver<ExtractedEvent>) {
        let (tx, rx) = bounded_ch(cfg.buffer_size);
        (
            Self {
                rx_term,
                tx,
                cfg: cfg.clone(),
            },
            rx,
        )
    }

    fn map_event(event: SuiEvent) -> Result<Option<ExtractedEvent>> {
        let object_id = &event.parsed_json.get("object_id").cloned();
        if let Some(object_id) = object_id {
            let object_id = format!("{object_id}");
            let object_id = ObjectID::from_hex_literal(object_id.trim_matches('"'))
                .context(format!("cannot read sui object id: {}", &object_id))?;
            return Ok(Some(ExtractedEvent { event, object_id }));
        }
        Ok(None)
    }

    pub async fn go(self) -> Result<()> {
        let mut skipped = 0;
        loop {
            let sui = SuiClientBuilder::default()
                .ws_url(&self.cfg.api.ws)
                .build(&self.cfg.api.http)
                .await
                .context("cannot create sui client")?;

            // todo: think about resubscription, some events will be missed and it's NOT okay
            let mut subscription = sui
                .event_api()
                .subscribe_event(self.cfg.event_filter.clone())
                .await
                .context("cannot subscribe to sui event stream")?;

            info!("Starting event consumption...");
            loop {
                tokio::select! {
                    item = subscription.next() => {
                        if let Some(event) = item {
                            let event = event?; // todo: we cannot just quit on error here

                            let pretty_event =  serde_json::to_string_pretty(&event).expect("valid event");
                            if let Some(event) = Self::map_event(event)? {
                                debug!(object_id = format!("{}", event.object_id), event = pretty_event, "consumed SUI event");
                                self.tx.send_async(event).await.expect("sends sui event for processing");
                            } else {
                                skipped += 1;
                                debug!(skipped, event = pretty_event, "Skipped an event...")
                            }
                        } else {
                            info!("Subscription is exhausted, resubscribing...");
                            break
                        }
                    },
                    _ = &mut self.rx_term.recv_async() => {
                        info!("SUI event consumer is terminating by a signal...");
                        return Ok(())
                    },
                }
            }
        }
    }
}
