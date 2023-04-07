#[macro_use]
extern crate serde;

mod _prelude;
mod cli;
mod conf;
mod object_changes_loader;
mod object_loader;
mod utils;

use crate::_prelude::*;
use cli::{Args, Commands, LoadObjectChangesArgs, LoadObjectsArgs};
use conf::AppConfig;
use dotenv::dotenv;
use object_changes_loader::{
    PulsarProducer as PulsarObjectChangeProducer, SuiExtractor as SuiObjectChangeExtractor,
};
use object_loader::{
    ObjectFetcher as SuiObjectFetcher, ObjectProducer as PulsarObjectProducer,
    PulsarConsumer as PulsarObjectChangeConsumer,
};

use clap::Parser;
use tracing_subscriber::filter::EnvFilter;

fn setup_tracing(cfg: &AppConfig) -> anyhow::Result<()> {
    let mut filter = EnvFilter::from_default_env().add_directive((*cfg.log.level).into());
    if let Some(filters) = &cfg.log.filter {
        for filter_str in filters {
            filter = filter.add_directive(filter_str.parse()?);
        }
    }

    let collector = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(cfg.log.ansi)
        .finish();

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

    tokio::task::spawn(async move {
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
                        break;
                    }
                }
                SigTerm::Instant(sig_term_instant) => {
                    if sig_term_instant.elapsed() > graceful_timeout {
                        warn!("Failed to exit within graceful timeout({}s.) - switching to immediate shutdown mode", graceful_timeout.as_secs());
                        break;
                    }
                }
            }
        }

        drop(tx_force_term);
    });

    (rx_sig_term, rx_force_term)
}

async fn load_object_changes(
    cfg: &AppConfig,
    _args: LoadObjectChangesArgs,
    rx_term: Receiver<()>,
    rx_force_term: Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (extractor, rx) = SuiObjectChangeExtractor::new(&cfg.sui, rx_term);
    let producer = PulsarObjectChangeProducer::new(&cfg.loader, &cfg.pulsar, rx, rx_force_term);

    let producer_task = tokio::task::spawn(async move { producer.go().await });

    extractor.go().await?;
    producer_task
        .await
        .context("cannot execute producer")?
        .context("error returned from a producer")?;

    Ok(())
}

async fn load_objects(
    cfg: &AppConfig,
    _args: LoadObjectsArgs,
    rx_term: Receiver<()>,
    rx_force_term: Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (consumer, rx_events, tx_confirm) = PulsarObjectChangeConsumer::new(&cfg.pulsar, &rx_term);
    let (fetcher, rx_enriched_events) =
        SuiObjectFetcher::new(&cfg.loader, &cfg.sui, rx_events, &rx_force_term);
    let producer = PulsarObjectProducer::new(
        &cfg.loader,
        &cfg.pulsar,
        rx_enriched_events,
        tx_confirm,
        &rx_force_term,
    );

    let fetcher_task = tokio::task::spawn(async move { fetcher.go().await });
    let producer_task = tokio::task::spawn(async move { producer.go().await });

    consumer.go().await?;

    fetcher_task
        .await
        .context("cannot execute fetcher")?
        .context("error returned from a fetcher")?;

    producer_task
        .await
        .context("cannot execute producer")?
        .context("error returned from a producer")?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let args: Args = Args::parse();

    let cfg = AppConfig::new(args.config_path)?;

    setup_tracing(&cfg).context("cannot setup tracing")?;

    info!("Starting SUI data loader...");
    info!(
        "Log system configured...: {} with filtering: {:?}",
        *cfg.log.level, cfg.log.filter
    );
    if args.print_config {
        info!("{:#?}", &cfg);
    }

    let (rx_term, rx_force_term) = setup_signal_handlers(&cfg);

    match args.command {
        Commands::LoadObjectChanges(cmd) => {
            load_object_changes(&cfg, cmd, rx_term, rx_force_term).await
        }
        Commands::LoadObjects(cmd) => load_objects(&cfg, cmd, rx_term, rx_force_term).await,
    }?;

    info!("Bye bye!");

    Ok(())
}
