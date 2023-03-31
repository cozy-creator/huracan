use {
    crate::_prelude::*,
    crate::conf::{LoaderConfig, PulsarConfig, SuiConfig},
};

use anyhow::Result;
use relabuf::{ExponentialBackoff, RelaBuf, RelaBufConfig};
use sui_sdk::rpc_types::SuiEvent;
use sui_sdk::SuiClientBuilder;

use pulsar::{
    compression::*, producer, Authentication, Error as PulsarError, Producer, Pulsar,
    SerializeMessage, TokioExecutor,
};

pub struct Loader {
    cfg: LoaderConfig,
    pulsar_cfg: PulsarConfig,

    rx: Receiver<ExtractedEvent>,
    rx_force_term: Receiver<()>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExtractedEvent {
    event: SuiEvent,
    object_id: String,
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

pub struct Extractor {
    rx_term: Receiver<()>,
    pub rx: Receiver<ExtractedEvent>,

    tx: Sender<ExtractedEvent>,
    cfg: SuiConfig,
}

impl Loader {
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

    async fn create_pulsar_producer(&self) -> Result<Producer<TokioExecutor>> {
        let mut builder = Pulsar::builder(&self.pulsar_cfg.uri, TokioExecutor);

        if let Some(token) = &self.pulsar_cfg.token {
            let auth = Authentication {
                name: "token".to_string(),
                data: token.clone().into_bytes(),
            };
            builder = builder.with_auth(auth);
        }

        let pulsar: Pulsar<_> = builder.build().await?;
        let producer = pulsar
            .producer()
            .with_topic(&self.pulsar_cfg.topic)
            .with_name(&self.pulsar_cfg.producer)
            .with_options(producer::ProducerOptions {
                batch_size: Some(self.cfg.batcher.soft_cap as u32),
                compression: Some(Compression::Snappy(CompressionSnappy::default())),
                ..Default::default()
            })
            .build()
            .await
            .context("cannot create apache pulsar producer")?;

        producer
            .check_connection()
            .await
            .context("cannot check apache pulsar connection")?;

        Ok(producer)
    }

    pub async fn load(&self) -> Result<()> {
        let mut producer = self
            .create_pulsar_producer()
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

        let rx = self.rx.clone();
        let (buf, proxy) = RelaBuf::new(opts, move || {
            let rx = rx.clone();
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
                            consumed.confirm();
                        }
                    } else {
                        break
                    }
                },
                _ = rx_force_term.recv_async() => {
                    info!("Event loader is terminating by a force signal...");
                    return Ok(())
                },
            }
        }

        info!("Event loader is terminated normally...");
        Ok(())
    }
}

impl Extractor {
    pub fn new(cfg: &SuiConfig, rx_term: Receiver<()>) -> Self {
        let (tx, rx) = bounded_ch(cfg.buffer_size);
        Self {
            rx_term,
            tx,
            rx,
            cfg: cfg.clone(),
        }
    }

    fn map_event(event: SuiEvent) -> Option<ExtractedEvent> {
        if let Some(object_id) = event.parsed_json.clone().get("object_id") {
            return Some(ExtractedEvent {
                event,
                object_id: format!("{object_id}"),
            });
        }
        None
    }

    pub async fn extract(self) -> Result<()> {
        let sui = SuiClientBuilder::default()
            .ws_url(&self.cfg.api.ws)
            .build(&self.cfg.api.http)
            .await
            .context("cannot create sui client")?;

        let mut subscription = sui
            .event_api()
            .subscribe_event(self.cfg.event_filter)
            .await
            .context("cannot subscribe to sui event stream")?;

        info!("Starting event consumption...");
        let rx_term = self.rx_term.clone();
        loop {
            tokio::select! {
                Some(event) = subscription.next() => {
                    if let Some(event) = Self::map_event(event?) {
                        self.tx.send_async(event).await.expect("always expected to send sui event for processing");
                    }
                },
                _ = rx_term.recv_async() => {
                    info!("Event consumer is terminating by a signal...");
                    break
                },
            }
        }

        Ok(())
    }
}
