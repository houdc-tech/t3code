use serde::Deserialize;
use std::{
    collections::HashSet,
    net::{Ipv6Addr, SocketAddr},
};

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub heartbeat_interval_secs: u64,
    pub stream_open_timeout_secs: u64,
    pub max_concurrent_streams_per_device: usize,
    pub devices: Vec<DeviceConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceConfig {
    pub id: String,
    pub host: String,
    pub token: String,
    #[serde(default = "https")]
    pub public_scheme: String,
}

fn https() -> String {
    "https".to_owned()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: ([0, 0, 0, 0], 8080).into(),
            heartbeat_interval_secs: 25,
            stream_open_timeout_secs: 10,
            max_concurrent_streams_per_device: 64,
            devices: Vec::new(),
        }
    }
}

impl Config {
    pub fn parse(input: &str) -> Result<Self, String> {
        let mut config: Self = toml::from_str(input).map_err(|error: toml::de::Error| {
            // TOML error messages can reproduce the entire line, including a bearer token.
            match error.span() {
                Some(span) => {
                    let line = input.as_bytes()[..span.start]
                        .iter()
                        .filter(|&&byte| byte == b'\n')
                        .count()
                        + 1;
                    format!("invalid relay configuration on line {line}")
                }
                None => "invalid relay configuration".to_owned(),
            }
        })?;
        if !(1..=u64::from(u32::MAX)).contains(&config.heartbeat_interval_secs)
            || !(1..=u64::from(u32::MAX)).contains(&config.stream_open_timeout_secs)
        {
            return Err("relay intervals must be between 1 and 4294967295 seconds".to_owned());
        }
        if !(1..=tokio::sync::Semaphore::MAX_PERMITS)
            .contains(&config.max_concurrent_streams_per_device)
        {
            return Err("max_concurrent_streams_per_device is out of range".to_owned());
        }
        if config.devices.is_empty() {
            return Err("at least one [[devices]] entry is required".to_owned());
        }

        let mut ids = HashSet::new();
        let mut hosts = HashSet::new();
        let mut tokens = HashSet::new();
        for device in &mut config.devices {
            if device.id.is_empty()
                || device.id.trim() != device.id
                || device.id.chars().any(char::is_control)
                || !ids.insert(device.id.as_str())
            {
                return Err("device IDs must be valid and unique".to_owned());
            }
            device.host = canonical_host(&device.host).ok_or("invalid device host")?;
            if !hosts.insert(device.host.as_str()) {
                return Err("duplicate device host".to_owned());
            }
            if !matches!(device.public_scheme.as_str(), "https" | "http") {
                return Err("public_scheme must be https or http".to_owned());
            }
            let token = device.token.trim_end_matches('=');
            if token.is_empty()
                || !token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._~+/".contains(&b))
                || !tokens.insert(device.token.as_str())
            {
                return Err("device bearer tokens must be valid and unique".to_owned());
            }
        }
        Ok(config)
    }
}

pub(crate) fn canonical_host(input: &str) -> Option<String> {
    let (host, port) = if let Some(ipv6) = input.strip_prefix('[') {
        let (address, suffix) = ipv6.split_once(']')?;
        let address: Ipv6Addr = address.parse().ok()?;
        let port = if suffix.is_empty() {
            None
        } else {
            Some(suffix.strip_prefix(':')?)
        };
        (format!("[{address}]"), port)
    } else {
        let (name, port) = input
            .split_once(':')
            .map_or((input, None), |(n, p)| (n, Some(p)));
        let labels = name.strip_suffix('.').unwrap_or(name);
        if labels.len() > 253
            || !labels.split('.').all(|label| {
                (1..=63).contains(&label.len())
                    && !label.starts_with('-')
                    && !label.ends_with('-')
                    && label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
        {
            return None;
        }
        (name.to_ascii_lowercase(), port)
    };
    match port {
        None => Some(host),
        Some(port) if port.bytes().all(|b| b.is_ascii_digit()) => {
            let port: u16 = port.parse().ok()?;
            (port != 0).then(|| format!("{host}:{port}"))
        }
        Some(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAC: &str = "[[devices]]\nid='studio'\nhost='Studio.Example:8443'\ntoken='secret-123'";

    #[test]
    fn default_values_and_authority() {
        let config = Config::parse(MAC).unwrap();
        assert_eq!(config.listen_addr, "0.0.0.0:8080".parse().unwrap());
        assert_eq!(config.heartbeat_interval_secs, 25);
        assert_eq!(config.stream_open_timeout_secs, 10);
        assert_eq!(config.max_concurrent_streams_per_device, 64);
        assert_eq!(config.devices[0].public_scheme, "https");
        assert_eq!(config.devices[0].host, "studio.example:8443");
        assert_eq!(
            canonical_host("[2001:0DB8::1]:8080").as_deref(),
            Some("[2001:db8::1]:8080")
        );
        assert_ne!(canonical_host("host:443"), canonical_host("host"));
    }

    #[test]
    fn rejects_ambiguous_or_invalid_authorities() {
        for host in [
            "",
            "host:0",
            "host:",
            "host:65536",
            "host:+80",
            "one..two",
            "a@host",
            "https://host",
            "host/path",
            "host#x",
            "host?x",
            "host,other",
            "-bad.example",
            "host\r\n",
            "[2001::1]x",
            "::1",
            "host:80:80",
        ] {
            assert!(canonical_host(host).is_none(), "{host:?}");
        }
    }

    #[test]
    fn validates_independent_device_bindings() {
        let second = "[[devices]]\nid='mini'\nhost='mini.example'\ntoken='different-token'";
        for duplicate in [
            second.replace("mini'", "studio'"),
            second.replace("mini.example", "STUDIO.example:8443"),
            second.replace("different-token", "secret-123"),
        ] {
            assert!(Config::parse(&format!("{MAC}\n{duplicate}")).is_err());
        }
        assert!(Config::parse(&format!("{MAC}\n{second}")).is_ok());
    }

    #[test]
    fn rejects_unusable_values_without_echoing_tokens() {
        for invalid in [
            format!("heartbeat_interval_secs=0\n{MAC}"),
            format!("stream_open_timeout_secs=9223372036854775807\n{MAC}"),
            format!("max_concurrent_streams_per_device=0\n{MAC}"),
            format!("{MAC}\npublic_scheme='ftp'"),
            MAC.replace("secret-123", "secret-123 bad"),
            MAC.replace("'secret-123'", "secret-123"),
        ] {
            let error = Config::parse(&invalid).err().unwrap();
            assert!(!error.contains("secret-123"));
        }
    }
}
