use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
pub enum Commands {
	Extract(ExtractArgs),
	Transform(TransformArgs),
	Load(LoadArgs),
	All(AllArgs),
}

#[derive(Parser)]
#[command(name = "sdl")]
#[command(bin_name = "sui-data-loader")]
pub struct Args {
	#[command(subcommand)]
	pub command:      Commands,
	#[arg(long, default_value = "./config.yaml")]
	pub config_path:  String,
	#[arg(long)]
	pub print_config: bool,
}

#[derive(Debug, clap::Args)]
#[command(version, long_about = None)]
#[command(about = "Starts extractor - extract object changes via SUI API and submit to Pulsar")]
pub struct ExtractArgs {
	#[arg(long, help = "transaction digest to start from")]
	pub start_from: Option<String>,
}

#[derive(Debug, clap::Args)]
#[command(version, long_about = None)]
#[command(about = "Starts transformer - read from Pulsar enrich with object data and submit to Pulsar")]
pub struct TransformArgs {}

#[derive(Debug, clap::Args)]
#[command(version, long_about = None)]
#[command(about = "Starts loader - read transformed entities from Pulsar and write to Mongo")]
pub struct LoadArgs {}

#[derive(Debug, clap::Args)]
#[command(version, long_about = None)]
#[command(about = "Starts integrated ETL loop without Pulsar or MongoDB persistence, mostly useful for development")]
pub struct AllArgs {
	#[arg(long, help = "transaction digest to start from")]
	pub start_from: Option<String>,
	#[arg(
		long,
		help = "disable connecting + saving to mongo, thus also disabling 'load' step",
		default_value_t = false
	)]
	pub no_mongo:   bool,
}
