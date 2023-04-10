use pulsar::message::proto::MessageIdData;

use crate::{
	_prelude::*,
	conf::{LoaderConfig, PulsarConfig},
	transformer::EnrichedObjectChange,
};

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
}
