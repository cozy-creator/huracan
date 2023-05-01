#![feature(drain_filter)]
#![feature(slice_group_by)]
#![feature(let_chains)]

#[macro_use]
extern crate serde;

use async_stream::stream;
use clap::Parser;
use cli::Args;
use conf::AppConfig;
use dotenv::dotenv;
use mongodb::{
	options::{ClientOptions, Compressor, ServerApi, ServerApiVersion},
	Client,
};
use sui_types::digests::TransactionDigest;
use tokio::pin;
use tracing_subscriber::filter::EnvFilter;

use crate::{
	_prelude::*,
	cli::Commands,
	etl::{ClientPool, StepStatus},
};

mod _prelude;
mod cli;
mod conf;
mod etl;

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

fn setup_ctrl_c_listener() -> tokio::sync::oneshot::Receiver<()> {
	let (tx_sig_term, rx_sig_term) = tokio::sync::oneshot::channel();
	tokio::spawn(async move {
		tokio::signal::ctrl_c().await.unwrap();
		let _ = tx_sig_term.send(());
	});
	rx_sig_term
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	dotenv().ok();

	let args: Args = Args::parse();

	let cfg = AppConfig::new(args.config_path)?;

	setup_tracing(&cfg).context("cannot setup tracing")?;

	if args.print_config {
		info!("{:#?}", &cfg);
	}

	let rx_term = setup_ctrl_c_listener();

	let sui = ClientPool::new(cfg.sui.api.urls).await?;

	match args.command {
		Commands::Extract(_) => {
			panic!("only 'all' command is currently implemented, executing all steps in a single process pipeline!")
		}
		Commands::Transform(_) => {
			panic!("only 'all' command is currently implemented, executing all steps in a single process pipeline!")
		}
		Commands::Load(_) => {
			panic!("only 'all' command is currently implemented, executing all steps in a single process pipeline!")
		}
		Commands::All(aargs) => {
			let start_from = aargs.start_from.map(|s| TransactionDigest::from_str(&s).unwrap());
			let items = etl::extract(sui.clone(), rx_term, start_from, |completed, next| {
				info!(
					"page done: {}, next page: {}",
					completed.map(|d| d.to_string()).unwrap_or("(initial)".into()),
					next
				);
			})
			.await?;

			let items = etl::transform(items, sui.clone()).await;

			// filter out any failures and stop there, at least for now, so we can debug + fix if needed
			// or else add handling for "normal" error conditions afterwards
			let items = async move {
				stream! {
					for await (status, item) in items {
						if let StepStatus::Ok = status {
							// keep going with next step
							yield item;
						} else {
							// stop and debug
							error!(
								?item,
								"failed to fetch item! stopping stream, please investigate if there's a bug that needs fixing!"
							);
							break
						}
					}
				}
			}
			.await;

			if !aargs.no_mongo {
				let mut client_options = ClientOptions::parse(&cfg.mongo.uri).await?;
				// use zstd compression for messages
				client_options.compressors = Some(vec![Compressor::Zstd { level: Some(1) }]);
				client_options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
				let client = Client::with_options(client_options)?;
				let db = client.database(&cfg.mongo.database);

				let items = etl::load(items, &db, &cfg.mongo.objects.collection).await;

				pin!(items);
				while let Some((status, item)) = items.next().await {
					if let StepStatus::Ok = status {
						// ok
					} else {
						// stop and debug
						error!(
								?item,
								"failed to execute mongo action! stopping stream, please investigate if there's a bug that needs fixing!"
							);
						break
					}
				}
			} else {
				// iterate over items to drive stream to completion
				pin!(items);
				while let Some(item) = items.next().await {
					info!(?item, "completed in-memory processing for {:#?}", item);
				}
			};
		}
	}

	Ok(())
}
