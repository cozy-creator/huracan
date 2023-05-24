#![feature(drain_filter)]
#![feature(slice_group_by)]
#![feature(let_chains)]

#[macro_use]
extern crate serde;

use std::sync::atomic::{AtomicBool, Ordering};

use _prelude::*;
use conf::AppConfig;
use dotenv::dotenv;
use tracing_subscriber::filter::EnvFilter;

mod _prelude;
mod client;
mod conf;
mod etl;
mod mongo;
mod pulsar;
mod utils;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	dotenv().ok();

	let cfg = AppConfig::new()?;

	setup_tracing(&cfg).context("cannot setup tracing")?;

	etl::start(&cfg).await.unwrap();

	// let start_from = aargs.start_from.or(cfg.extract.from).map(|s| TransactionDigest::from_str(&s).unwrap());
	// let items = etl::extract(sui.clone(), rx_term, start_from, {
	// 	let mut n = 0u64;
	// 	move |completed, next| {
	// 		n += 1;
	// 		if n % 1000 == 0 {
	// 			info!(
	// 				"{} pages done! completed page: {}, next page: {}",
	// 				n,
	// 				completed.map(|d| d.to_string()).unwrap_or("(initial)".into()),
	// 				next
	// 			);
	// 		}
	// 	}
	// })
	// .await?;
	//
	// let items = etl::transform(items, sui.clone()).await;
	//
	// // filter out any failures and stop there, at least for now, so we can debug + fix if needed
	// // or else add handling for "normal" error conditions afterwards
	// let items = async move {
	// 	stream! {
	// 		for await (status, item) in items {
	// 			if let StepStatus::Ok = status {
	// 				// keep going with next step
	// 				yield item;
	// 			} else {
	// 				// stop and debug
	// 				error!(
	// 					?item,
	// 					"failed to fetch item! stopping stream, please investigate if there's a bug that needs fixing!"
	// 				);
	// 				break
	// 			}
	// 		}
	// 	}
	// }
	// .await;
	//
	// if !aargs.no_mongo {
	// 	let mut client_options = ClientOptions::parse(&cfg.mongo.uri).await?;
	// 	// use zstd compression for messages
	// 	client_options.compressors = Some(vec![Compressor::Zstd { level: Some(1) }]);
	// 	client_options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
	// 	let client = Client::with_options(client_options)?;
	// 	let db = client.database(&cfg.mongo.database);
	//
	// 	let items = etl::load(items, &db, &cfg.mongo.objects.collection).await;
	//
	// 	pin!(items);
	// 	while let Some((status, item)) = items.next().await {
	// 		if let StepStatus::Ok = status {
	// 			// ok
	// 		} else {
	// 			// stop and debug
	// 			error!(
	// 				?item,
	// 				"failed to execute mongo action! stopping stream, please investigate if there's a bug that needs fixing!"
	// 			);
	// 			break
	// 		}
	// 	}
	// } else {
	// 	// iterate over items to drive stream to completion
	// 	pin!(items);
	// 	while let Some(item) = items.next().await {
	// 		info!(?item, "completed in-memory processing for {:#?}", item);
	// 	}
	// };

	Ok(())
}

// -- helpers

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

pub fn ctrl_c_bool() -> Arc<AtomicBool> {
	let stop = Arc::new(AtomicBool::new(false));
	tokio::spawn({
		let stop = stop.clone();
		async move {
			tokio::signal::ctrl_c().await.unwrap();
			stop.store(true, Ordering::Relaxed);
		}
	});
	stop
}
