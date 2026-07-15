use std::{
    collections::{BTreeMap, HashSet},
    fs,
    net::SocketAddr,
    path::Path,
};

use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    #[serde(default = "default_listen")]
    pub(crate) listen: SocketAddr,
    pub(crate) tuners: Vec<TunerConfig>,
    pub(crate) channels: Vec<ChannelConfig>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TunerConfig {
    pub(crate) name: String,
    pub(crate) types: Vec<String>,
    pub(crate) command: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ChannelConfig {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) channel_type: String,
    pub(crate) channel: String,
    #[serde(default)]
    pub(crate) service_id: Option<u16>,
    #[serde(default)]
    pub(crate) command_vars: BTreeMap<String, String>,
}

fn default_listen() -> SocketAddr {
    "0.0.0.0:40772".parse().expect("static address is valid")
}

impl Config {
    pub(crate) fn load(path: &Path) -> Result<Self, String> {
        let source = fs::read_to_string(path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        let config: Self = serde_yml::from_str(&source)
            .map_err(|error| format!("failed to parse {}: {error}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        if self.tuners.is_empty() {
            return Err("at least one tuner is required".into());
        }
        if self.channels.is_empty() {
            return Err("at least one channel is required".into());
        }

        for tuner in &self.tuners {
            if tuner.name.is_empty() || tuner.command.is_empty() || tuner.types.is_empty() {
                return Err("each tuner requires name, command, and at least one type".into());
            }
            if tuner.types.iter().any(String::is_empty) {
                return Err("tuner types must not be empty".into());
            }
        }

        let mut channels = HashSet::new();
        for channel in &self.channels {
            if channel.name.is_empty()
                || channel.channel_type.is_empty()
                || channel.channel.is_empty()
            {
                return Err("each channel requires name, type, and channel".into());
            }
            if !channels.insert((&channel.channel_type, &channel.channel, channel.service_id)) {
                return Err(format!(
                    "duplicate channel: {}/{} (serviceId {:?})",
                    channel.channel_type, channel.channel, channel.service_id
                ));
            }
            if !self
                .tuners
                .iter()
                .any(|tuner| tuner.types.contains(&channel.channel_type))
            {
                return Err(format!(
                    "no tuner supports channel type {}",
                    channel.channel_type
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_channels_without_a_matching_tuner() {
        let config = Config {
            listen: default_listen(),
            tuners: vec![TunerConfig {
                name: "terrestrial".into(),
                types: vec!["GR".into()],
                command: "record <channel>".into(),
            }],
            channels: vec![ChannelConfig {
                name: "satellite".into(),
                channel_type: "BS".into(),
                channel: "BS01_0".into(),
                service_id: None,
                command_vars: BTreeMap::new(),
            }],
        };
        assert_eq!(
            config.validate().unwrap_err(),
            "no tuner supports channel type BS"
        );
    }

    #[test]
    fn accepts_arbitrary_channel_types() {
        let config = Config {
            listen: default_listen(),
            tuners: vec![TunerConfig {
                name: "custom".into(),
                types: vec!["CUSTOM".into()],
                command: "record <channel>".into(),
            }],
            channels: vec![ChannelConfig {
                name: "custom".into(),
                channel_type: "CUSTOM".into(),
                channel: "1".into(),
                service_id: None,
                command_vars: BTreeMap::new(),
            }],
        };

        assert!(config.validate().is_ok());
    }
}
