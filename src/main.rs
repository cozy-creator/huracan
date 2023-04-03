#[macro_use]
extern crate serde;

mod _prelude;
mod cli;
mod conf;
mod event_loader;

use crate::_prelude::*;
use cli::{Args, Commands, LoadEventsArgs};
use conf::AppConfig;
use dotenv::dotenv;
use event_loader::{PulsarLoader as PulsarEventLoader, SuiExtractor as SuiEventExtractor};

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

                    warn!("Ctrl-C detected - stopping reading new events, awaiting graceful termination for {}s.", graceful_timeout.as_secs());
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

async fn load_events(
    cfg: &AppConfig,
    _args: LoadEventsArgs,
    rx_term: Receiver<()>,
    rx_force_term: Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (extractor, rx) = SuiEventExtractor::new(&cfg.sui, rx_term);
    let loader = PulsarEventLoader::new(&cfg.loader, &cfg.pulsar, rx, rx_force_term);

    let loader_task = tokio::task::spawn(async move { loader.go().await });

    extractor.go().await?;
    loader_task
        .await
        .context("cannot execute loader")?
        .context("error returned from a loader")?;

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
        Commands::LoadEvents(cmd) => load_events(&cfg, cmd, rx_term, rx_force_term).await,
    }?;

    info!("Bye bye!");

    Ok(())
}
