use figment::{
	providers::{Env, Format, Yaml},
	Figment,
};

use crate::_prelude::*;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarObjectChangesConfig {
	pub topic:        String,
	pub consumer:     String,
	pub producer:     String,
	pub subscription: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarEnrichedObjectChangesConfig {
	pub topic:        String,
	pub consumer:     String,
	pub producer:     String,
	pub subscription: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MongoObjectsConfig {
	pub collection: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MongoConfig {
	pub uri:      String,
	pub database: String,
	pub objects:  MongoObjectsConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarConfig {
	pub uri:                     String,
	pub token:                   Option<String>,
	#[serde(alias = "objectchanges")]
	pub object_changes:          PulsarObjectChangesConfig,
	#[serde(alias = "enrichedobjectchanges")]
	pub enriched_object_changes: PulsarEnrichedObjectChangesConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoaderConfig {
	pub batcher:     BatchingConfig,
	#[serde(alias = "buffersize")]
	pub buffer_size: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchingConfig {
	#[serde(alias = "softcap")]
	pub soft_cap:      usize,
	#[serde(alias = "hardcap")]
	pub hard_cap:      usize,
	#[serde(alias = "releaseafter")]
	pub release_after: CDuration,
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
pub struct SuiApiConfig {
	pub http: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiConfig {
	pub api:                SuiApiConfig,
	#[serde(alias = "eventfilter")]
	pub transaction_filter: Option<sui_types::query::TransactionFilter>,
	#[serde(alias = "pagesize")]
	pub page_size:          usize,
}

#[derive(Clone, Debug)]
pub struct CLevel(pub tracing::Level);

impl Deref for CLevel {
	type Target = tracing::Level;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<'de> Deserialize<'de> for CLevel {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<CLevel, D::Error> {
		let s: String = Deserialize::deserialize(deserializer)?;
		tracing::Level::from_str(&s).map(CLevel).map_err(de::Error::custom)
	}
}

#[derive(Clone, Debug)]
pub struct CDuration(Duration);

impl Deref for CDuration {
	type Target = Duration;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<'de> Deserialize<'de> for CDuration {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<CDuration, D::Error> {
		let s: String = Deserialize::deserialize(deserializer)?;
		let s = s.replace('_', "");
		let v = humanize_rs::duration::parse(&s);
		let r = v.map_err(de::Error::custom)?;
		Ok(CDuration(r))
	}
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
	pub log:    LogConfig,
	pub loader: LoaderConfig,
	pub pulsar: PulsarConfig,
	pub mongo:  MongoConfig,

	pub sui: SuiConfig,
}

impl AppConfig {
	pub fn new(path: String) -> anyhow::Result<Self> {
		let cfg = Figment::new().merge(Yaml::file(path)).merge(Env::prefixed("APP_").split("_")).extract()?;

		Ok(cfg)
	}
}
