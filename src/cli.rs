use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
pub enum Commands {
    LoadEvents(LoadEventsArgs),
}

#[derive(Parser)]
#[command(name = "sdl")]
#[command(bin_name = "sui-data-loader")]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
    #[arg(long)]
    pub config_path: Option<std::path::PathBuf>,
    #[arg(long)]
    pub print_config: bool,
}

#[derive(Debug, clap::Args)]
#[command(version, long_about = None)]
#[command(about = "Starts SUI Data Loader")]
pub struct LoadEventsArgs {}
