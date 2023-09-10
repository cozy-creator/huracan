use figment::{
	providers::{Env, Format, Yaml},
	Figment,
};
use influxdb::Client;
use mongodb::{
	options::{ClientOptions, Compressor, ServerApi, ServerApiVersion},
	Database,
};
use tokio::sync::OnceCell;

use crate::{_prelude::*, client::ClientPool};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
	#[serde(default)]
	pub name:                     String,
	pub queuebuffers:             QueueBuffersConfig,
	pub workers:                  WorkersConfig,
	pub objectqueries:            ObjectQueriesConfig,
	pub mongo:                    MongoPipelineStepConfig,
	pub checkpointretries:        usize,
	pub checkpointretrytimeoutms: u64,
	pub tracklatency:             bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectQueriesConfig {
	pub batchsize:          usize,
	pub batchwaittimeoutms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MongoPipelineStepConfig {
	pub batchsize:          usize,
	pub batchwaittimeoutms: u64,
	pub retries:            usize,
	pub zstdlevel:          i32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueBuffersConfig {
	pub checkpointout: usize,
	pub cpcompletions: usize,
	pub mongoinfactor: usize,
	pub last:          usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkersConfig {
	pub checkpoint: Option<usize>,
	pub object:     Option<usize>,
	pub mongo:      Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MongoConfig {
	pub uri:            String,
	pub db:             String,
	pub collectionbase: String,
}

impl MongoConfig {
	pub async fn client(&self, pc: &MongoPipelineStepConfig) -> anyhow::Result<Database> {
		let mut client_options = ClientOptions::parse(&self.uri).await?;
		// use zstd compression for messages
		client_options.compressors = Some(vec![Compressor::Zstd { level: Some(pc.zstdlevel) }]);
		client_options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
		let client = mongodb::Client::with_options(client_options)?;
		Ok(client.database(&self.db))
	}
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarConfig {
	pub url:         String,
	pub issuer:      String,
	pub credentials: String,
	pub audience:    String,
	pub topicbase:   String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InfluxConfig {
	pub database: String,
	pub token: String,
	pub url: String,
	pub transactiontopicbase: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
	pub level:        CLevel,
	pub filter:       Option<Vec<String>>,
	// Must be either "stdout" or "logfile"
	pub output:       String,
	// Ignored unless output == "logfile".
	// Please declare as absolute path, example: "/var/log/indexer.log"
	pub logfilepath:  String,
	pub tokioconsole: bool,
}

impl Default for LogConfig {
	fn default() -> LogConfig {
		LogConfig {
			level:        CLevel(Level::INFO),
			filter:       None,
			output:       "logfile".to_string(),
			logfilepath:  "/var/log/indexer.log".to_string(),
			tokioconsole: false,
		}
	}
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcProviderConfig {
	pub url:               String,
	pub name:              String,
	pub objectsquerylimit: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiConfig {
	pub testnet:  Vec<RpcProviderConfig>,
	pub mainnet:  Vec<RpcProviderConfig>,
	pub localnet: Vec<RpcProviderConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Whitelist {
	pub enabled:  bool,
	pub packages: Option<Vec<String>>,
}

impl Default for Whitelist {
	fn default() -> Whitelist {
		Whitelist { enabled: false, packages: None }
	}
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Blacklist {
	pub enabled:  bool,
	pub packages: Option<Vec<String>>,
}

impl Default for Blacklist {
	fn default() -> Blacklist {
		Blacklist { enabled: false, packages: None }
	}
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
	pub env:                     String,
	pub net:                     String,
	pub rocksdbfile:             String,
	pub backfill:                PipelineConfig,
	pub livescan:                PipelineConfig,
	pub backfillthreshold:       usize,
	pub pausepollonbackfill:     bool,
	pub pollintervalms:          u64,
	pub mongo:                   MongoConfig,
	pub pulsar:                  PulsarConfig,
	pub influx:                  InfluxConfig,
	pub sui:                     SuiConfig,
	pub log:                     LogConfig,
	pub backfillonly:            bool,
	pub livescanonly:            bool,
	pub backfillstartcheckpoint: Option<u64>,
	pub whitelist:               Whitelist,
	pub blacklist:               Blacklist,
}

impl AppConfig {
	pub fn new() -> anyhow::Result<Self> {
		let mut config: AppConfig =
			Figment::new().merge(Yaml::file("config.yaml")).merge(Env::prefixed("APP_").split("_")).extract()?;
		config.backfill.name = "backfill".into();
		config.livescan.name = "livescan".into();

		// FIXME validate that the directory is either empty, doesn't exist or contains ONLY rocksDB data files
		//			this is because we automatically remove the dir at runtime without further checks
		//			so if you've misconfigured this
		if config.rocksdbfile == "" || config.rocksdbfile == "/" {
			panic!("please set config.rocksdbfile to a new or empty or existing RocksDB data dir; it can and will be deleted at runtime, as needed!");
		}

		Ok(config)
	}

	pub async fn sui(&self) -> anyhow::Result<ClientPool> {
		let providers = if self.net == "testnet" {
			&self.sui.testnet
		} else if self.net == "mainnet" {
			&self.sui.mainnet
		} else if self.net == "localnet" {
			&self.sui.localnet
		} else {
			panic!("unknown net configuration: {} (expected: mainnet | testnet | localnet)", self.net);
		};
		if providers.is_empty() {
			panic!("no RPC providers configured for {}!", self.net);
		}
		Ok(ClientPool::new(providers.clone()).await?)
	}
}

// Singleton for config
pub(crate) static APPCONFIG: OnceCell<AppConfig> = OnceCell::const_new();

// Setup config singleton
pub async fn setup_config_singleton(cfg: &AppConfig) -> &'static AppConfig {
	APPCONFIG.get_or_init(|| async { cfg.clone() }).await
}

pub fn get_config_singleton() -> &'static AppConfig {
	APPCONFIG.get().expect("ConfigError: config singleton could not be loaded.")
}

pub(crate) static INFLUXCLIENT: OnceCell<influxdb::Client> = OnceCell::const_new();

pub async fn setup_influx_singleton() -> &'static influxdb::Client {
	INFLUXCLIENT.get_or_init(|| async {
		let influxconfig = get_config_singleton().influx.clone();
		let client = Client::new(influxconfig.url, influxconfig.database).with_token(influxconfig.token);
		return client;
	}).await
}

pub fn get_influx_singleton() -> &'static influxdb::Client {
	INFLUXCLIENT.get().expect("ConfigError: Influx client could not be loaded.")
}

#[derive(Clone, Debug)]
pub struct CLevel(pub Level);

impl Deref for CLevel {
	type Target = Level;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<'de> Deserialize<'de> for CLevel {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<CLevel, D::Error> {
		let s: String = Deserialize::deserialize(deserializer)?;
		Level::from_str(&s).map(CLevel).map_err(de::Error::custom)
	}
}
