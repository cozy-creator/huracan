use clap::Parser;

#[derive(Parser)]
#[command(name = "sdl")]
#[command(bin_name = "sui-data-loader")]
pub enum SuiDataLoaderCli {
    Start(StartArgs),
}

#[derive(clap::Args)]
#[command(version, long_about = None)]
#[command(about = "Starts SUI Data Loader")]
pub struct StartArgs {
    #[arg(long)]
    pub config_path: Option<std::path::PathBuf>,
    #[arg(long)]
    pub print_config: bool,
}
