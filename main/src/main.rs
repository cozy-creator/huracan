#![feature(drain_filter)]
#![feature(btree_drain_filter)]
#![feature(slice_group_by)]
#![feature(let_chains)]
#![feature(iter_advance_by)]
#![feature(int_roundings)]
#![feature(map_try_insert)]

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

	if std::env::var("FULLSCAN_ONLY").is_ok() {
		etl::run_fullscan_only(&cfg).await?;
	} else {
		etl::run(&cfg).await.unwrap();
	}

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
