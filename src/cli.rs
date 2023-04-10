use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
pub enum Commands {
    Extract(ExtractArgs),
    Transform(TransformArgs),
    Load(LoadArgs),
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
#[command(about = "Starts extractor - extract object changes via SUI API and submit to Pulsar")]
pub struct ExtractArgs {}

#[derive(Debug, clap::Args)]
#[command(version, long_about = None)]
#[command(
    about = "Starts transformer - read from Pulsar enrich with object data and submit to Pulsar"
)]
pub struct TransformArgs {}

#[derive(Debug, clap::Args)]
#[command(version, long_about = None)]
#[command(about = "Starts loader - read transformed entities from Pulsar and write to Mongo")]
pub struct LoadArgs {}
