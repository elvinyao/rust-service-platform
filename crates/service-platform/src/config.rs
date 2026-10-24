//! Admin server configuration, duration serde, and validation helpers.

use std::{net::IpAddr, time::Duration};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use url::Url;

use crate::errors::{AppError, ErrorType, Result};

/// Administrative HTTP server listener and timeout settings.
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

impl AdminConfig {
    pub fn validate(&self) -> Result<()> {
        validate_listen_address("admin.address", &self.address)?;
        for (name, duration) in [
            ("admin.read_header_timeout", self.read_header_timeout),
            ("admin.read_timeout", self.read_timeout),
            ("admin.write_timeout", self.write_timeout),
            ("admin.idle_timeout", self.idle_timeout),
        ] {
            require_positive_duration(name, duration)?;
        }
        Ok(())
    }
}

pub fn validate_listen_address(name: &str, value: &str) -> Result<()> {
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

pub fn validate_url(name: &str, raw: &str, schemes: &[&str]) -> Result<()> {
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

pub fn require_positive_duration(name: &str, duration: Duration) -> Result<()> {
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

pub mod duration_serde {
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
    use super::*;

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
    fn admin_config_and_url_validation_cover_valid_and_invalid_inputs() {
        let default = AdminConfig::default();
        default.validate().unwrap();
        for address in ["localhost:80", ":8080", "127.0.0.1:0", "[::1]:8080"] {
            let mut config = default.clone();
            config.address = address.to_owned();
            config.validate().unwrap();
        }
        for address in [
            "",
            "localhost",
            "localhost:invalid",
            "bad_host:80",
            "2001:db8::1:80",
            "[broken]:80",
        ] {
            let mut config = default.clone();
            config.address = address.to_owned();
            assert!(
                config
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains("admin.address")
            );
        }
        for mutate in [
            |config: &mut AdminConfig| config.read_header_timeout = Duration::ZERO,
            |config: &mut AdminConfig| config.read_timeout = Duration::ZERO,
            |config: &mut AdminConfig| config.write_timeout = Duration::ZERO,
            |config: &mut AdminConfig| config.idle_timeout = Duration::ZERO,
        ] {
            let mut config = default.clone();
            mutate(&mut config);
            assert!(
                config
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains("greater than zero")
            );
        }
        assert!(
            validate_url("event_target", "mailto:user@example.com", &["mailto"])
                .unwrap_err()
                .to_string()
                .contains("host")
        );
    }
}
