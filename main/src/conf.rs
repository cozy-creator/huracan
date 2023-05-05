use figment::{
	providers::{Env, Format, Yaml},
	Figment,
};

use crate::_prelude::*;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueBuffersConfig {
	pub step1out:        usize,
	pub cpcontrolfactor: usize,
	pub mongoinfactor:   usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkersConfig {
	pub step1: Option<usize>,
	pub step2: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MongoConfig {
	pub uri:                String,
	pub db:                 String,
	pub collectionbase:     String,
	pub batchsize:          usize,
	pub batchwaittimeoutms: u64,
	pub retries:            usize,
	pub zstdlevel:          i32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarConfig {
	pub issuer:      String,
	pub credentials: String,
	pub audience:    String,
	pub topicbase:   String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
	pub level:  CLevel,
	pub ansi:   bool,
	pub filter: Option<Vec<String>>,
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
	pub testnet:             Vec<RpcProviderConfig>,
	pub mainnet:             Vec<RpcProviderConfig>,
	pub step1retries:        usize,
	pub step1retrytimeoutms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
	pub env:          String,
	pub net:          String,
	pub rocksdbfile:  String,
	pub queuebuffers: QueueBuffersConfig,
	pub workers:      WorkersConfig,
	pub mongo:        MongoConfig,
	pub pulsar:       PulsarConfig,
	pub sui:          SuiConfig,
	pub log:          LogConfig,
}

impl AppConfig {
	pub fn new() -> anyhow::Result<Self> {
		Ok(Figment::new().merge(Yaml::file("config.yaml")).merge(Env::prefixed("APP_").split("_")).extract()?)
	}
}

// -- helpers

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
