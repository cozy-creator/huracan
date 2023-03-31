use crate::_prelude::*;

use std::fs;

use base64::Engine;
use config::{Config, ConfigError, Environment, File, FileFormat};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarConfig {
    pub uri: String,
    pub topic: String,
    pub producer: String,
    pub token: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoaderConfig {
    pub batcher: BatchingConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchingConfig {
    #[serde(alias = "softcap")]
    pub soft_cap: usize,
    #[serde(alias = "hardcap")]
    pub hard_cap: usize,
    #[serde(alias = "releaseafter")]
    pub release_after: CDuration,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShutdownConfig {
    pub timeout: CDuration,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    pub level: CLevel,
    pub ansi: bool,
    pub filter: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiApiConfig {
    pub http: String,
    pub ws: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiConfig {
    pub api: SuiApiConfig,
    #[serde(alias = "eventfilter")]
    pub event_filter: sui_sdk::rpc_types::EventFilter,
}

#[derive(Clone, Debug)]
pub struct CBase64(pub Vec<u8>);

impl<'de> Deserialize<'de> for CBase64 {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<CBase64, D::Error> {
        let s: String = Deserialize::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(s)
            .map(CBase64)
            .map_err(de::Error::custom)
    }
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
        tracing::Level::from_str(&s)
            .map(CLevel)
            .map_err(de::Error::custom)
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
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<CDuration, D::Error> {
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
    pub log: LogConfig,
    pub shutdown: ShutdownConfig,
    pub loader: LoaderConfig,
    pub pulsar: PulsarConfig,

    pub sui: SuiConfig,
}

impl AppConfig {
    fn new_from_content(content: &str) -> std::result::Result<Self, ConfigError> {
        let x = Config::builder()
            .add_source(File::from_str(content, FileFormat::Yaml))
            .add_source(
                Environment::with_prefix("APP")
                    .separator("_")
                    .list_separator(" "),
            )
            .build()?;
        x.try_deserialize()
    }

    pub fn new() -> std::result::Result<Self, ConfigError> {
        let default_config_str = fs::read_to_string("./config.yaml")
            .map_err(|err| ConfigError::Message(err.to_string()))?;
        Self::new_from_content(&default_config_str)
    }
}
