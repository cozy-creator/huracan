#![feature(drain_filter)]
#![feature(btree_drain_filter)]
#![feature(slice_group_by)]
#![feature(let_chains)]
#![feature(iter_advance_by)]
#![feature(int_roundings)]
#![feature(map_try_insert)]

#[macro_use]
extern crate serde;

use std::fs::File;
use std::sync::atomic::{AtomicBool, Ordering};

use _prelude::*;
use conf::AppConfig;
use dotenv::dotenv;
use tracing_subscriber::filter::EnvFilter;
use tokio::sync::OnceCell;

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

	if cfg.log.tokioconsole == true {
		setup_console_tracing(&cfg).context("cannot setup tracing")?;
	}
	else {
		setup_tracing(&cfg).context("cannot setup tracing")?;
	}

	if cfg.backfillonly == true {
		let start_checkpoint = cfg.backfillstartcheckpoint;
		etl::run_backfill_only(&cfg, start_checkpoint).await?;
	} else {
		etl::run(&cfg).await.unwrap();
	}

	setup_config_singleton(&cfg).context("cannot setup config singleton")?;
	Ok(())
}

// Setup default tracing mode, which does not enable tokio-console
fn setup_tracing(cfg: &AppConfig) -> anyhow::Result<()> {

	// Configure tracing collector with file output.
	if cfg.log.output == "logfile" {
		// Create filters based on config.
		let mut filter = EnvFilter::from_default_env().add_directive((*cfg.log.level).into());
		if let Some(filters) = &cfg.log.filter {
			for filter_str in filters {
				filter = filter.add_directive(filter_str.parse()?);
			}
		}
		let log_file = File::create(&cfg.log.logfilepath)?;
		let collector =
			tracing_subscriber::fmt()
				.with_env_filter(filter)
				.with_target(false)
				.with_line_number(true)
				.with_file(true)
				.with_writer(Mutex::new(log_file))
				.with_thread_ids(true)
				.with_thread_names(true)
				.json()
				.finish();
		tracing::subscriber::set_global_default(collector)?;
	}

	if cfg.log.output == "stdout" {
		// Create filters based on config.
		let mut filter = EnvFilter::from_default_env().add_directive((*cfg.log.level).into());
		if let Some(filters) = &cfg.log.filter {
			for filter_str in filters {
				filter = filter.add_directive(filter_str.parse()?);
			}
		}
		let collector =
			tracing_subscriber::fmt()
				.with_env_filter(filter)
				.with_target(false)
				.with_ansi(true)
				.with_line_number(true)
				.with_file(true)
				.finish();
		tracing::subscriber::set_global_default(collector)?;
	}
	Ok(())

}

// Setup tracing with tokio-console enabled.
// See: https://tokio.rs/tokio/topics/tracing-next-steps
fn setup_console_tracing(cfg: &AppConfig) -> anyhow::Result<()> {
	console_subscriber::init();
	Ok(())
}

// Ensure Tokio threads are drained on a smooth shutdown
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
