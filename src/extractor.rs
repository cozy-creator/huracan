use anyhow::Result;
use pulsar::{producer, DeserializeMessage, Error as PulsarError, SerializeMessage};
use relabuf::{ExponentialBackoff, RelaBuf, RelaBufConfig};
use sui_sdk::{
	rpc_types::{
		ObjectChange as SuiObjectChange, SuiTransactionBlockResponseOptions, SuiTransactionBlockResponseQuery,
	},
	SuiClientBuilder,
};

use crate::{
	_prelude::*,
	conf::{LoaderConfig, PulsarConfig, SuiConfig},
	utils,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExtractedObjectChange {
	pub change: SuiObjectChange,
}

impl SerializeMessage for ExtractedObjectChange {
	fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
		let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
		Ok(producer::Message { payload, ..Default::default() })
	}
}

impl DeserializeMessage for ExtractedObjectChange {
	type Output = Result<Self>;

	fn deserialize_message(payload: &pulsar::Payload) -> Self::Output {
		Ok(serde_json::from_slice(&payload.data).map_err(|e| PulsarError::Custom(e.to_string()))?)
	}
}

pub struct PulsarProducer {
	cfg:        LoaderConfig,
	pulsar_cfg: PulsarConfig,

	rx:            Receiver<ExtractedObjectChange>,
	rx_force_term: Receiver<()>,
}

pub struct Extractor {
	rx_term: Receiver<()>,

	tx:  Sender<ExtractedObjectChange>,
	cfg: SuiConfig,
}

impl PulsarProducer {
	pub fn new(
		cfg: &LoaderConfig,
		pulsar_cfg: &PulsarConfig,
		rx_extractor: Receiver<ExtractedObjectChange>,
		rx_force_term: Receiver<()>,
	) -> Self {
		Self { cfg: cfg.clone(), pulsar_cfg: pulsar_cfg.clone(), rx: rx_extractor, rx_force_term }
	}

	pub async fn go(self) -> Result<()> {
		let mut producer = utils::create_pulsar_producer(&utils::PulsarProducerOptions {
			uri:        self.pulsar_cfg.uri,
			topic:      self.pulsar_cfg.object_changes.topic,
			producer:   self.pulsar_cfg.object_changes.producer,
			token:      self.pulsar_cfg.token,
			batch_size: self.cfg.batcher.soft_cap as u32,
		})
		.await
		.context("cannot create pulsar producer")?;

		let opts = RelaBufConfig {
			soft_cap:      self.cfg.batcher.soft_cap,
			hard_cap:      self.cfg.batcher.hard_cap,
			release_after: *self.cfg.batcher.release_after,
			backoff:       Some(ExponentialBackoff { max_elapsed_time: None, ..ExponentialBackoff::default() }),
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

impl Extractor {
	pub fn new(
		cfg: &SuiConfig,
		loader_cfg: &LoaderConfig,
		rx_term: Receiver<()>,
	) -> (Self, Receiver<ExtractedObjectChange>) {
		let (tx, rx) = bounded_ch(loader_cfg.buffer_size);
		(Self { rx_term, tx, cfg: cfg.clone() }, rx)
	}

	async fn handle_object_change(&self, change: SuiObjectChange) -> Result<()> {
		let pretty_object_change = serde_json::to_string_pretty(&change).expect("valid object change");

		self.tx.send_async(ExtractedObjectChange { change }).await?;

		debug!(change = ?pretty_object_change, "object change");

		Ok(())
	}

	pub async fn go(self) -> Result<()> {
		let mut read_blocks = 0;
		let mut read_object_changes = 0;

		let sui = SuiClientBuilder::default().build(&self.cfg.api.http).await.context("cannot create sui client")?;

		let sui_read = sui.read_api();

		let mut cursor = None;
		let options = Some(SuiTransactionBlockResponseOptions {
			show_input:           false,
			show_raw_input:       false,
			show_effects:         false,
			show_events:          false,
			show_object_changes:  true,
			show_balance_changes: false,
		});
		let query = SuiTransactionBlockResponseQuery { filter: self.cfg.transaction_filter.clone(), options };

		info!("Reading object changes...");
		loop {
			tokio::select! {
				page = sui_read.query_transaction_blocks(query.clone(), cursor, Some(self.cfg.page_size), false) => {
					match page {
						Ok(page) => {
							cursor = page.next_cursor;
							for item in page.data {
								read_blocks += 1;
								if let Some(object_changes) = item.object_changes {
									for change in object_changes {
										read_object_changes += 1;
										self.handle_object_change(change).await?;
									}
								}
							}

							info!(read_blocks, read_object_changes, "processed another page of transaction blocks");
						},
						Err(err) => {
							warn!(error = ?err, "There was an error reading object changes... retrying");
						}
					}
				}
				_ = &mut self.rx_term.recv_async() => {
					info!("SUI event consumer is terminating by a signal...");
					return Ok(())
				},
			}
		}
	}
}
