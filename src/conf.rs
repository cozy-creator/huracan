use crate::_prelude::*;

use figment::{
    providers::{Env, Format, Yaml},
    Figment,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarEventsConfig {
    pub topic: String,
    pub consumer: String,
    pub producer: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarObjectsConfig {
    pub topic: String,
    pub producer: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarConfig {
    pub uri: String,
    pub token: Option<String>,
    pub events: PulsarEventsConfig,
    pub objects: PulsarObjectsConfig,
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
    #[serde(alias = "buffersize")]
    pub buffer_size: usize,
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
    pub fn new(path: Option<std::path::PathBuf>) -> anyhow::Result<Self> {
        let cfg = Figment::new()
            .merge(Yaml::file(path.unwrap_or("./config.yaml".into())))
            .merge(Env::prefixed("APP_").split("_"))
            .extract()?;

        Ok(cfg)
    }
}
