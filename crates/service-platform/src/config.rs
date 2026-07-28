//! Strict runtime configuration with YAML defaults and environment overrides.

use std::{
    env, fs,
    net::IpAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use url::Url;

use crate::errors::{AppError, ErrorType, Result};

/// Reference runtime wiring configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    pub admin: AdminConfig,
    pub inputs: InputsConfig,
    pub adapters: AdaptersConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdminConfig {
    pub address: String,
    #[serde(with = "duration_serde")]
    pub read_header_timeout: Duration,
    #[serde(with = "duration_serde")]
    pub read_timeout: Duration,
    #[serde(with = "duration_serde")]
    pub write_timeout: Duration,
    #[serde(with = "duration_serde")]
    pub idle_timeout: Duration,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InputsConfig {
    pub websocket: WebSocketInputConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebSocketInputConfig {
    pub enabled: bool,
    pub server_url: String,
    pub path: String,
    #[serde(with = "duration_serde")]
    pub reconnect_interval: Duration,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdaptersConfig {
    pub confluence: ConfluenceAdapterConfig,
    pub mattermost: MattermostAdapterConfig,
    pub badgedb: BadgeDbAdapterConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfluenceAdapterConfig {
    pub enabled: bool,
    pub api_endpoint: String,
    pub settings_page_id: String,
    #[serde(with = "duration_serde")]
    pub refresh_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MattermostAdapterConfig {
    pub enabled: bool,
    pub server_url: String,
    pub websocket_url: String,
    pub api_token: String,
    pub channel: String,
    #[serde(skip)]
    pub username: String,
    #[serde(skip)]
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BadgeDbAdapterConfig {
    pub enabled: bool,
    pub path: PathBuf,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:18080".to_owned(),
            read_header_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(120),
        }
    }
}

impl Default for WebSocketInputConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            server_url: "ws://localhost:8093".to_owned(),
            path: "/ws".to_owned(),
            reconnect_interval: Duration::from_secs(5),
        }
    }
}

impl Default for ConfluenceAdapterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_endpoint: "http://localhost:8090".to_owned(),
            settings_page_id: "settings-page-1".to_owned(),
            refresh_interval: Duration::from_secs(300),
        }
    }
}

impl Default for MattermostAdapterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            server_url: "http://localhost:8091".to_owned(),
            websocket_url: "ws://localhost:8092".to_owned(),
            api_token: "test-token-123".to_owned(),
            channel: "test-channel-1".to_owned(),
            username: String::new(),
            password: String::new(),
        }
    }
}

impl Default for BadgeDbAdapterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: PathBuf::from("/tmp/service-workflow-badges.db"),
        }
    }
}

impl RuntimeConfig {
    /// Loads one strict YAML document, applies environment overrides, and validates it.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let data = fs::read_to_string(path).map_err(|error| {
            AppError::wrap(
                ErrorType::InvalidInput,
                format!("read runtime config {}", path.display()),
                error,
            )
        })?;
        if data.trim().is_empty() {
            return invalid("runtime config is empty");
        }
        let mut documents = serde_yaml::Deserializer::from_str(&data);
        let first = documents
            .next()
            .ok_or_else(|| AppError::new(ErrorType::InvalidInput, "runtime config is empty"))?;
        let mut config = Self::deserialize(first).map_err(|error| {
            AppError::wrap(ErrorType::InvalidInput, "parse runtime config", error)
        })?;
        if documents.next().is_some() {
            return Err(AppError::new(
                ErrorType::InvalidInput,
                "parse runtime config: multiple YAML documents are not supported",
            ));
        }
        config.apply_env()?;
        config.validate()?;
        Ok(config)
    }

    /// Applies the operational override layer used by the reference runtime.
    pub fn apply_env(&mut self) -> Result<()> {
        self.apply_lookup(|key| env::var(key).ok())
    }

    fn apply_lookup(&mut self, mut lookup: impl FnMut(&str) -> Option<String>) -> Result<()> {
        apply_string(lookup("ADMIN_ADDR"), &mut self.admin.address);
        apply_duration(
            "ADMIN_READ_HEADER_TIMEOUT",
            lookup("ADMIN_READ_HEADER_TIMEOUT"),
            &mut self.admin.read_header_timeout,
        )?;
        apply_duration(
            "ADMIN_READ_TIMEOUT",
            lookup("ADMIN_READ_TIMEOUT"),
            &mut self.admin.read_timeout,
        )?;
        apply_duration(
            "ADMIN_WRITE_TIMEOUT",
            lookup("ADMIN_WRITE_TIMEOUT"),
            &mut self.admin.write_timeout,
        )?;
        apply_duration(
            "ADMIN_IDLE_TIMEOUT",
            lookup("ADMIN_IDLE_TIMEOUT"),
            &mut self.admin.idle_timeout,
        )?;
        apply_string(
            lookup("WEBSOCKET_SERVER_URL"),
            &mut self.inputs.websocket.server_url,
        );
        apply_string(lookup("WEBSOCKET_PATH"), &mut self.inputs.websocket.path);
        apply_string(
            lookup("CONFLUENCE_API_ENDPOINT"),
            &mut self.adapters.confluence.api_endpoint,
        );
        apply_string(
            lookup("CONFLUENCE_SETTINGS_PAGE_ID"),
            &mut self.adapters.confluence.settings_page_id,
        );
        apply_string(
            lookup("MATTERMOST_SERVER_URL"),
            &mut self.adapters.mattermost.server_url,
        );
        apply_string(
            lookup("MATTERMOST_WS_URL"),
            &mut self.adapters.mattermost.websocket_url,
        );
        apply_string(
            lookup("MATTERMOST_API_TOKEN"),
            &mut self.adapters.mattermost.api_token,
        );
        apply_string(
            lookup("MATTERMOST_CHANNEL"),
            &mut self.adapters.mattermost.channel,
        );
        apply_string(
            lookup("MATTERMOST_USERNAME"),
            &mut self.adapters.mattermost.username,
        );
        apply_string(
            lookup("MATTERMOST_PASSWORD"),
            &mut self.adapters.mattermost.password,
        );
        if let Some(value) = lookup("BADGEDB_PATH").filter(|value| !value.is_empty()) {
            self.adapters.badgedb.path = PathBuf::from(value);
        }
        Ok(())
    }

    /// Validates required values and settings for enabled components.
    pub fn validate(&self) -> Result<()> {
        validate_listen_address("admin.address", &self.admin.address)?;
        for (name, duration) in [
            ("admin.read_header_timeout", self.admin.read_header_timeout),
            ("admin.read_timeout", self.admin.read_timeout),
            ("admin.write_timeout", self.admin.write_timeout),
            ("admin.idle_timeout", self.admin.idle_timeout),
        ] {
            require_positive_duration(name, duration)?;
        }

        if self.inputs.websocket.enabled {
            validate_url(
                "inputs.websocket.server_url",
                &self.inputs.websocket.server_url,
                &["ws", "wss"],
            )?;
            if !self.inputs.websocket.path.starts_with('/') {
                return invalid("inputs.websocket.path must start with /");
            }
            require_positive_duration(
                "inputs.websocket.reconnect_interval",
                self.inputs.websocket.reconnect_interval,
            )?;
        }

        if self.adapters.confluence.enabled {
            validate_url(
                "adapters.confluence.api_endpoint",
                &self.adapters.confluence.api_endpoint,
                &["http", "https"],
            )?;
            if self.adapters.confluence.settings_page_id.is_empty() {
                return invalid("adapters.confluence.settings_page_id is required when enabled");
            }
            require_positive_duration(
                "adapters.confluence.refresh_interval",
                self.adapters.confluence.refresh_interval,
            )?;
        }

        if self.adapters.mattermost.enabled {
            validate_url(
                "adapters.mattermost.server_url",
                &self.adapters.mattermost.server_url,
                &["http", "https"],
            )?;
            if !self.adapters.mattermost.websocket_url.is_empty() {
                validate_url(
                    "adapters.mattermost.websocket_url",
                    &self.adapters.mattermost.websocket_url,
                    &["ws", "wss"],
                )?;
            }
            if self.adapters.mattermost.channel.is_empty() {
                return invalid("adapters.mattermost.channel is required when enabled");
            }
            if self.adapters.mattermost.api_token.is_empty()
                && (self.adapters.mattermost.username.is_empty()
                    || self.adapters.mattermost.password.is_empty())
            {
                return invalid(
                    "adapters.mattermost requires api_token or both username and password when enabled",
                );
            }
        }

        if self.adapters.badgedb.enabled && self.adapters.badgedb.path.as_os_str().is_empty() {
            return invalid("adapters.badgedb.path is required when enabled");
        }
        Ok(())
    }
}

fn apply_string(value: Option<String>, target: &mut String) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        *target = value;
    }
}

fn apply_duration(key: &str, value: Option<String>, target: &mut Duration) -> Result<()> {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        *target = parse_go_duration(&value).map_err(|message| {
            AppError::new(
                ErrorType::InvalidInput,
                format!("{key} is invalid: {message}"),
            )
        })?;
    }
    Ok(())
}

fn validate_listen_address(name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return invalid(format!("{name} is required"));
    }
    if value.starts_with('[') {
        return value
            .parse::<std::net::SocketAddr>()
            .map(|_| ())
            .map_err(|error| {
                AppError::wrap(ErrorType::InvalidInput, format!("{name} is invalid"), error)
            });
    }
    let Some((host, port)) = value.rsplit_once(':') else {
        return invalid(format!("{name} is invalid: missing port"));
    };
    if host.contains(':') || port.parse::<u16>().is_err() {
        return invalid(format!("{name} is invalid"));
    }
    if !host.is_empty() && host.parse::<IpAddr>().is_err() && !valid_hostname(host) {
        return invalid(format!("{name} is invalid"));
    }
    Ok(())
}

fn valid_hostname(host: &str) -> bool {
    !host.is_empty()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn validate_url(name: &str, raw: &str, schemes: &[&str]) -> Result<()> {
    let parsed = Url::parse(raw).map_err(|error| {
        AppError::wrap(ErrorType::InvalidInput, format!("{name} is invalid"), error)
    })?;
    if parsed.host_str().is_none() {
        return invalid(format!("{name} is invalid: host is required"));
    }
    if !schemes.contains(&parsed.scheme()) {
        return invalid(format!(
            "{name} must use one of these schemes: {}",
            schemes.join(", ")
        ));
    }
    Ok(())
}

fn require_positive_duration(name: &str, duration: Duration) -> Result<()> {
    if duration.is_zero() {
        return invalid(format!("{name} must be greater than zero"));
    }
    Ok(())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(AppError::new(ErrorType::InvalidInput, message))
}

/// Parses the duration syntax accepted by Go's `time.ParseDuration`.
pub fn parse_go_duration(raw: &str) -> std::result::Result<Duration, String> {
    if raw.is_empty() {
        return Err("duration is empty".to_owned());
    }
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    if raw.starts_with('-') {
        return Err("negative durations are not supported".to_owned());
    }

    let bytes = raw.as_bytes();
    let mut index = usize::from(raw.starts_with('+'));
    let mut total = Duration::ZERO;
    let mut components = 0;
    while index < bytes.len() {
        let number_start = index;
        let mut dots = 0;
        while index < bytes.len() && (bytes[index].is_ascii_digit() || bytes[index] == b'.') {
            if bytes[index] == b'.' {
                dots += 1;
            }
            index += 1;
        }
        if number_start == index || dots > 1 {
            return Err(format!("invalid duration {raw:?}"));
        }
        let number: f64 = raw[number_start..index]
            .parse()
            .map_err(|_| format!("invalid duration {raw:?}"))?;

        let (unit, seconds) = [
            ("ns", 0.000_000_001_f64),
            ("us", 0.000_001_f64),
            ("µs", 0.000_001_f64),
            ("μs", 0.000_001_f64),
            ("ms", 0.001_f64),
            ("s", 1_f64),
            ("m", 60_f64),
            ("h", 3_600_f64),
        ]
        .into_iter()
        .find(|(unit, _)| raw[index..].starts_with(unit))
        .ok_or_else(|| format!("missing or invalid unit in duration {raw:?}"))?;
        index += unit.len();
        let component = Duration::try_from_secs_f64(number * seconds)
            .map_err(|_| format!("invalid duration {raw:?}"))?;
        total = total
            .checked_add(component)
            .ok_or_else(|| format!("invalid duration {raw:?}"))?;
        components += 1;
    }

    let max_go_duration = u128::try_from(i64::MAX).expect("i64::MAX is positive");
    if components == 0 || total.as_nanos() > max_go_duration {
        return Err(format!("invalid duration {raw:?}"));
    }
    Ok(total)
}

pub fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    for (suffix, unit) in [
        ("h", 3_600_000_000_000_u128),
        ("m", 60_000_000_000_u128),
        ("s", 1_000_000_000_u128),
        ("ms", 1_000_000_u128),
        ("us", 1_000_u128),
        ("ns", 1_u128),
    ] {
        if nanos >= unit && nanos % unit == 0 {
            return format!("{}{suffix}", nanos / unit);
        }
    }
    "0s".to_owned()
}

mod duration_serde {
    use super::*;

    pub fn serialize<S>(value: &Duration, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format_duration(*value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_go_duration(&value).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn error_message(mut config: RuntimeConfig, change: impl FnOnce(&mut RuntimeConfig)) -> String {
        change(&mut config);
        config.validate().unwrap_err().to_string()
    }

    #[test]
    fn repository_runtime_profiles_are_strict_and_valid() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for relative in ["config/runtime.yaml", "config/profiles/demo/runtime.yaml"] {
            RuntimeConfig::load(root.join(relative)).unwrap();
        }
    }

    #[test]
    fn duration_parser_accepts_go_units_and_components() {
        assert_eq!(parse_go_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(
            parse_go_duration("1h2m3.5s").unwrap(),
            Duration::from_millis(3_723_500)
        );
        assert!(parse_go_duration("5").is_err());
        assert!(parse_go_duration("-1s").is_err());

        assert_eq!(parse_go_duration("0").unwrap(), Duration::ZERO);
        assert_eq!(
            parse_go_duration("+1ms2us3ns").unwrap(),
            Duration::from_nanos(1_002_003)
        );
        assert_eq!(
            parse_go_duration("1µs1μs1us").unwrap(),
            Duration::from_micros(3)
        );
        for invalid in [
            "",
            "+",
            "s",
            "1..2s",
            "1xs",
            "NaNs",
            "3000000h",
            "6000000h",
            "999999999999999999999h",
        ] {
            assert!(parse_go_duration(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn duration_formatting_and_serde_cover_all_units() {
        for (duration, expected) in [
            (Duration::from_secs(7_200), "2h"),
            (Duration::from_secs(120), "2m"),
            (Duration::from_secs(2), "2s"),
            (Duration::from_millis(2), "2ms"),
            (Duration::from_micros(2), "2us"),
            (Duration::from_nanos(2), "2ns"),
            (Duration::from_nanos(1_500), "1500ns"),
            (Duration::ZERO, "0s"),
        ] {
            assert_eq!(format_duration(duration), expected);
        }

        let yaml = serde_yaml::to_string(&AdminConfig::default()).unwrap();
        assert!(yaml.contains("read_header_timeout: 5s"));
        assert_eq!(
            serde_yaml::from_str::<AdminConfig>(&yaml).unwrap(),
            AdminConfig::default()
        );
    }

    #[test]
    fn strict_yaml_rejects_unknown_fields_and_extra_documents() {
        let unknown = "admin:\n  typo: true\n";
        let error = serde_yaml::from_str::<RuntimeConfig>(unknown).unwrap_err();
        assert!(error.to_string().contains("unknown field"));

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("runtime.yaml");
        fs::write(&path, "{}\n---\n{}\n").unwrap();
        let error = RuntimeConfig::load(path).unwrap_err();
        assert!(error.to_string().contains("multiple YAML documents"));

        let empty_path = directory.path().join("empty.yaml");
        fs::write(&empty_path, "").unwrap();
        assert!(
            RuntimeConfig::load(empty_path)
                .unwrap_err()
                .to_string()
                .contains("empty")
        );
        assert!(
            RuntimeConfig::load(directory.path().join("missing.yaml"))
                .unwrap_err()
                .to_string()
                .contains("read runtime config")
        );

        let malformed = directory.path().join("malformed.yaml");
        fs::write(&malformed, "admin: 42\n").unwrap();
        assert!(
            RuntimeConfig::load(malformed)
                .unwrap_err()
                .to_string()
                .contains("parse runtime config")
        );
    }

    #[test]
    fn enabled_components_require_valid_urls_and_values() {
        let mut config = RuntimeConfig::default();
        config.inputs.websocket.server_url = "http://wrong".to_owned();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("must use")
        );

        config.inputs.websocket.enabled = false;
        config.adapters.mattermost.enabled = true;
        config.adapters.mattermost.channel.clear();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("channel")
        );
    }

    #[test]
    fn environment_lookup_applies_all_supported_overrides() {
        let values = BTreeMap::from([
            ("ADMIN_ADDR", "0.0.0.0:9000"),
            ("ADMIN_READ_HEADER_TIMEOUT", "1s"),
            ("ADMIN_READ_TIMEOUT", "2s"),
            ("ADMIN_WRITE_TIMEOUT", "3s"),
            ("ADMIN_IDLE_TIMEOUT", "4s"),
            ("WEBSOCKET_SERVER_URL", "wss://events.example.com"),
            ("WEBSOCKET_PATH", "/stream"),
            ("CONFLUENCE_API_ENDPOINT", "https://wiki.example.com"),
            ("CONFLUENCE_SETTINGS_PAGE_ID", "page-2"),
            ("MATTERMOST_SERVER_URL", "https://chat.example.com"),
            ("MATTERMOST_WS_URL", "wss://chat.example.com"),
            ("MATTERMOST_API_TOKEN", "secret"),
            ("MATTERMOST_CHANNEL", "alerts"),
            ("MATTERMOST_USERNAME", "operator"),
            ("MATTERMOST_PASSWORD", "password"),
            ("BADGEDB_PATH", "/tmp/custom.db"),
        ]);
        let mut config = RuntimeConfig::default();
        config
            .apply_lookup(|key| values.get(key).map(ToString::to_string))
            .unwrap();

        assert_eq!(config.admin.address, "0.0.0.0:9000");
        assert_eq!(config.admin.read_header_timeout, Duration::from_secs(1));
        assert_eq!(config.admin.read_timeout, Duration::from_secs(2));
        assert_eq!(config.admin.write_timeout, Duration::from_secs(3));
        assert_eq!(config.admin.idle_timeout, Duration::from_secs(4));
        assert_eq!(
            config.inputs.websocket.server_url,
            "wss://events.example.com"
        );
        assert_eq!(config.inputs.websocket.path, "/stream");
        assert_eq!(
            config.adapters.confluence.api_endpoint,
            "https://wiki.example.com"
        );
        assert_eq!(config.adapters.confluence.settings_page_id, "page-2");
        assert_eq!(
            config.adapters.mattermost.server_url,
            "https://chat.example.com"
        );
        assert_eq!(
            config.adapters.mattermost.websocket_url,
            "wss://chat.example.com"
        );
        assert_eq!(config.adapters.mattermost.api_token, "secret");
        assert_eq!(config.adapters.mattermost.channel, "alerts");
        assert_eq!(config.adapters.mattermost.username, "operator");
        assert_eq!(config.adapters.mattermost.password, "password");
        assert_eq!(
            config.adapters.badgedb.path,
            PathBuf::from("/tmp/custom.db")
        );
    }

    #[test]
    fn environment_lookup_ignores_blanks_and_rejects_invalid_durations() {
        let original = RuntimeConfig::default();
        let mut config = original.clone();
        config
            .apply_lookup(|_| Some(String::new()))
            .expect("blank overrides are ignored");
        assert_eq!(config, original);

        let mut config = RuntimeConfig::default();
        let error = config
            .apply_lookup(|key| (key == "ADMIN_READ_TIMEOUT").then(|| "soon".to_owned()))
            .unwrap_err();
        assert!(error.to_string().contains("ADMIN_READ_TIMEOUT is invalid"));
    }

    #[test]
    fn validation_covers_addresses_timeouts_and_enabled_components() {
        let default = RuntimeConfig::default();
        default.validate().unwrap();

        for message in [
            error_message(default.clone(), |config| config.admin.address.clear()),
            error_message(default.clone(), |config| {
                config.admin.address = "localhost".to_owned();
            }),
            error_message(default.clone(), |config| {
                config.admin.address = "localhost:invalid".to_owned();
            }),
            error_message(default.clone(), |config| {
                config.admin.address = "bad_host:80".to_owned();
            }),
            error_message(default.clone(), |config| {
                config.admin.address = "2001:db8::1:80".to_owned();
            }),
        ] {
            assert!(message.contains("admin.address"), "{message}");
        }
        for address in ["localhost:80", ":8080", "127.0.0.1:0", "[::1]:8080"] {
            let mut config = default.clone();
            config.admin.address = address.to_owned();
            config.validate().unwrap();
        }
        assert!(
            error_message(default.clone(), |config| {
                config.admin.address = "[broken]:80".to_owned();
            })
            .contains("admin.address")
        );

        for mutate in [
            |config: &mut RuntimeConfig| config.admin.read_header_timeout = Duration::ZERO,
            |config: &mut RuntimeConfig| config.admin.read_timeout = Duration::ZERO,
            |config: &mut RuntimeConfig| config.admin.write_timeout = Duration::ZERO,
            |config: &mut RuntimeConfig| config.admin.idle_timeout = Duration::ZERO,
        ] {
            assert!(error_message(default.clone(), mutate).contains("greater than zero"));
        }

        assert!(
            error_message(default.clone(), |config| {
                config.inputs.websocket.server_url = "not a URL".to_owned();
            })
            .contains("invalid")
        );
        assert!(
            validate_url("event_target", "mailto:user@example.com", &["mailto"])
                .unwrap_err()
                .to_string()
                .contains("host")
        );
        assert!(
            error_message(default.clone(), |config| {
                config.inputs.websocket.path = "events".to_owned();
            })
            .contains("start with")
        );
        assert!(
            error_message(default.clone(), |config| {
                config.inputs.websocket.reconnect_interval = Duration::ZERO;
            })
            .contains("greater than zero")
        );

        let mut disabled = default.clone();
        disabled.inputs.websocket.enabled = false;
        disabled.inputs.websocket.server_url = "invalid".to_owned();
        disabled.inputs.websocket.path.clear();
        disabled.inputs.websocket.reconnect_interval = Duration::ZERO;
        disabled.validate().unwrap();
    }

    #[test]
    fn validation_covers_demo_adapter_contracts() {
        let mut base = RuntimeConfig::default();
        base.inputs.websocket.enabled = false;

        for change in [
            |config: &mut RuntimeConfig| {
                config.adapters.confluence.enabled = true;
                config.adapters.confluence.api_endpoint = "invalid".to_owned();
            },
            |config: &mut RuntimeConfig| {
                config.adapters.confluence.enabled = true;
                config.adapters.confluence.api_endpoint = "ftp://wiki.example.com".to_owned();
            },
            |config: &mut RuntimeConfig| {
                config.adapters.confluence.enabled = true;
                config.adapters.confluence.settings_page_id.clear();
            },
            |config: &mut RuntimeConfig| {
                config.adapters.confluence.enabled = true;
                config.adapters.confluence.refresh_interval = Duration::ZERO;
            },
            |config: &mut RuntimeConfig| {
                config.adapters.mattermost.enabled = true;
                config.adapters.mattermost.server_url = "invalid".to_owned();
            },
            |config: &mut RuntimeConfig| {
                config.adapters.mattermost.enabled = true;
                config.adapters.mattermost.server_url = "ws://chat.example.com".to_owned();
            },
            |config: &mut RuntimeConfig| {
                config.adapters.mattermost.enabled = true;
                config.adapters.mattermost.websocket_url = "invalid".to_owned();
            },
            |config: &mut RuntimeConfig| {
                config.adapters.mattermost.enabled = true;
                config.adapters.mattermost.websocket_url = "https://chat.example.com".to_owned();
            },
            |config: &mut RuntimeConfig| {
                config.adapters.mattermost.enabled = true;
                config.adapters.mattermost.channel.clear();
            },
            |config: &mut RuntimeConfig| {
                config.adapters.mattermost.enabled = true;
                config.adapters.mattermost.api_token.clear();
            },
            |config: &mut RuntimeConfig| {
                config.adapters.mattermost.enabled = true;
                config.adapters.mattermost.api_token.clear();
                config.adapters.mattermost.username = "operator".to_owned();
            },
            |config: &mut RuntimeConfig| {
                config.adapters.badgedb.enabled = true;
                config.adapters.badgedb.path = PathBuf::new();
            },
        ] {
            assert!(error_message(base.clone(), change).contains("adapters"));
        }

        let mut enabled = base;
        enabled.adapters.confluence.enabled = true;
        enabled.adapters.mattermost.enabled = true;
        enabled.adapters.badgedb.enabled = true;
        enabled.validate().unwrap();
    }
}
