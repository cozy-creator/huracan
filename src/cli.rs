use clap::Parser;

#[derive(Parser)] // requires `derive` feature
#[command(name = "sdl")]
#[command(bin_name = "sui-data-loader")]
pub enum SuiDataLoaderCli {
    Start(StartArgs),
}

#[derive(clap::Args)]
#[command(author, version, about, long_about = None)]
pub struct StartArgs {
    #[arg(long)]
    pub config_path: Option<std::path::PathBuf>,
}
