#[macro_use]
extern crate serde;

mod _prelude;
mod cli;
mod conf;
mod etl;
mod extractor;
mod loader;
mod transformer;
mod utils;

use clap::Parser;
use cli::{Args, Commands, ExtractArgs, LoadArgs, TransformArgs};
use conf::AppConfig;
use dotenv::dotenv;
use extractor::{Extractor, PulsarProducer as PulsarObjectChangeProducer};
use loader::{Loader, PulsarConfirmer as LoaderPulsarConfirmer, PulsarConsumer as LoaderPulsarConsumer};
use sui_sdk::SuiClientBuilder;
use tracing_subscriber::filter::EnvFilter;
use transformer::{
	ObjectProducer as PulsarObjectProducer, PulsarConfirmer as PulsarObjectChangeConfirmer,
	PulsarConsumer as PulsarObjectChangeConsumer, Transformer,
};

use crate::_prelude::*;

fn setup_tracing(cfg: &AppConfig) -> anyhow::Result<()> {
	let mut filter = EnvFilter::from_default_env().add_directive((*cfg.log.level).into());
	if let Some(filters) = &cfg.log.filter {
		for filter_str in filters {
			filter = filter.add_directive(filter_str.parse()?);
		}
	}

	let collector =
		tracing_subscriber::fmt().with_env_filter(filter).with_target(false).with_ansi(cfg.log.ansi).finish();

	tracing::subscriber::set_global_default(collector)?;
	Ok(())
}

fn setup_signal_handlers(cfg: &AppConfig) -> (Receiver<()>, Receiver<()>) {
	let (tx_sig_term, rx_sig_term) = bounded_ch(0);
	let (tx_force_term, rx_force_term) = bounded_ch(0);

	let graceful_timeout = *cfg.shutdown.timeout;

	enum SigTerm {
		Tx(Sender<()>),
		Instant(Instant),
	}

	tokio::spawn(async move {
		let mut sig_term = SigTerm::Tx(tx_sig_term);

		loop {
			let timeout = time::sleep(Duration::from_millis(100));

			tokio::select! {
				_ = tokio::signal::ctrl_c() => {
					if graceful_timeout.as_millis() < 1 {
						warn!("Ctrl-C detected, but graceful_timeout is zero - switching to immediate shutdown mode");
						break
					}

					warn!("Ctrl-C detected - stopping reading new object changes, awaiting graceful termination for {}s.", graceful_timeout.as_secs());
					if let SigTerm::Instant(_) = &sig_term {
						warn!("Ctrl-C detected while awaiting for graceful termination - switching to immediate shutdown mode");
						break
					}
					sig_term = SigTerm::Instant(Instant::now());
				}
				_ = timeout => {}
			}

			match &sig_term {
				SigTerm::Tx(tx_sig_term) => {
					if tx_sig_term.is_disconnected() {
						break
					}
				}
				SigTerm::Instant(sig_term_instant) => {
					if sig_term_instant.elapsed() > graceful_timeout {
						warn!(
							"Failed to exit within graceful timeout({}s.) - switching to immediate shutdown mode",
							graceful_timeout.as_secs()
						);
						break
					}
				}
			}
		}

		drop(tx_force_term);
	});

	(rx_sig_term, rx_force_term)
}

async fn extract(
	cfg: &AppConfig,
	_args: ExtractArgs,
	rx_term: Receiver<()>,
	rx_force_term: Receiver<()>,
) -> anyhow::Result<()> {
	let (extractor, rx_sui_changes) = Extractor::new(&cfg.sui, &cfg.loader, rx_term);
	let producer = PulsarObjectChangeProducer::new(&cfg.loader, &cfg.pulsar, rx_sui_changes, rx_force_term);

	tokio::try_join!(
		tokio::spawn(async move { extractor.go().await }),
		tokio::spawn(async move { producer.go().await }),
	)?;

	Ok(())
}

async fn transform(
	cfg: &AppConfig,
	_args: TransformArgs,
	rx_term: Receiver<()>,
	rx_force_term: Receiver<()>,
) -> anyhow::Result<()> {
	let (consumer, rx_pulsar_changes) = PulsarObjectChangeConsumer::new(&cfg.pulsar, &cfg.loader, &rx_term);
	let (confirmer, tx_pulsar_acks) = PulsarObjectChangeConfirmer::new(&cfg.pulsar, &cfg.loader, &rx_force_term);
	let (fetcher, rx_enriched_events) = Transformer::new(&cfg.loader, &cfg.sui, rx_pulsar_changes, &rx_force_term);
	let producer =
		PulsarObjectProducer::new(&cfg.loader, &cfg.pulsar, rx_enriched_events, tx_pulsar_acks, &rx_force_term);

	tokio::try_join!(
		tokio::spawn(async move { consumer.go().await }),
		tokio::spawn(async move { confirmer.go().await }),
		tokio::spawn(async move { fetcher.go().await }),
		tokio::spawn(async move { producer.go().await }),
	)?;

	Ok(())
}

async fn load(
	cfg: &AppConfig,
	_args: LoadArgs,
	rx_term: Receiver<()>,
	rx_force_term: Receiver<()>,
) -> anyhow::Result<()> {
	let (consumer, rx_pulsar_objs) = LoaderPulsarConsumer::new(&cfg.pulsar, &cfg.loader, &rx_term);
	let (confirmer, tx_pulsar_acks) = LoaderPulsarConfirmer::new(&cfg.pulsar, &cfg.loader, &rx_force_term);
	let loader = Loader::new(&cfg.loader, &cfg.mongo, rx_pulsar_objs, tx_pulsar_acks, &rx_force_term);

	tokio::try_join!(
		tokio::spawn(async move { consumer.go().await }),
		tokio::spawn(async move { loader.go().await }),
		tokio::spawn(async move { confirmer.go().await }),
	)?;

	Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	dotenv().ok();

	let args: Args = Args::parse();

	let cfg = AppConfig::new(args.config_path)?;

	setup_tracing(&cfg).context("cannot setup tracing")?;

	info!("Starting SUI data loader...");
	info!("Log system configured...: {} with filtering: {:?}", *cfg.log.level, cfg.log.filter);
	if args.print_config {
		info!("{:#?}", &cfg);
	}

	let (rx_term, rx_force_term) = setup_signal_handlers(&cfg);

	let _sui = SuiClientBuilder::default().build(cfg.sui.api.http.clone()).await?.read_api();

	match args.command {
		Commands::Extract(cmd) => extract(&cfg, cmd, rx_term, rx_force_term).await.context("error during extraction"),
		Commands::Transform(cmd) => {
			transform(&cfg, cmd, rx_term, rx_force_term).await.context("error during transforming")
		}
		Commands::Load(cmd) => load(&cfg, cmd, rx_term, rx_force_term).await.context("error during loading"),
	}?;

	info!("Bye bye!");

	Ok(())
}
