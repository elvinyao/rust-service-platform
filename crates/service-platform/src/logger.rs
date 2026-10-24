//! Structured logging configuration and rotating file output.

use std::{
    fs::{File, OpenOptions, rename},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use chrono::Utc;
use tracing_subscriber::{
    EnvFilter, fmt,
    fmt::{time::ChronoUtc, writer::BoxMakeWriter},
};

static INITIALIZED: OnceLock<()> = OnceLock::new();
static INITIALIZE_LOCK: Mutex<()> = Mutex::new(());

/// Logging output configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggerConfig {
    pub level: String,
    pub format: LogFormat,
    pub time_format: Option<String>,
    pub caller_info: bool,
    pub ansi: bool,
    pub output: LogOutput,
    pub file_rotation: FileRotationConfig,
}

/// Supported log encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

/// Supported destinations for the global tracing subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogOutput {
    Stdout,
    Stderr,
    File(PathBuf),
}

/// Size and age thresholds for file output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileRotationConfig {
    pub max_size_bytes: u64,
    pub rotate_after: Duration,
}

impl Default for FileRotationConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 100 * 1024 * 1024,
            rotate_after: Duration::from_secs(24 * 60 * 60),
        }
    }
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
            format: LogFormat::Json,
            time_format: None,
            caller_info: true,
            ansi: false,
            output: LogOutput::Stdout,
            file_rotation: FileRotationConfig::default(),
        }
    }
}

impl LoggerConfig {
    /// Loads the operational logging override layer.
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(non_empty_env)
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, String> {
        let mut config = Self::default();
        if let Some(value) = lookup("LOG_LEVEL").filter(|value| !value.is_empty()) {
            config.level = value;
        }
        if let Some(value) = lookup("LOG_FORMAT").filter(|value| !value.is_empty()) {
            config.format = match value.to_ascii_lowercase().as_str() {
                "json" => LogFormat::Json,
                "text" | "pretty" => LogFormat::Text,
                _ => return Err(format!("LOG_FORMAT must be json or text, got {value:?}")),
            };
        }
        config.time_format = lookup("LOG_TIME_FORMAT").filter(|value| !value.is_empty());
        if let Some(value) = lookup("LOG_CALLER_INFO").filter(|value| !value.is_empty()) {
            config.caller_info = parse_bool("LOG_CALLER_INFO", &value)?;
        }
        if let Some(value) = lookup("LOG_ANSI").filter(|value| !value.is_empty()) {
            config.ansi = parse_bool("LOG_ANSI", &value)?;
        }
        if let Some(value) = lookup("LOG_OUTPUT").filter(|value| !value.is_empty()) {
            config.output = match value.as_str() {
                "stdout" => LogOutput::Stdout,
                "stderr" => LogOutput::Stderr,
                path => LogOutput::File(PathBuf::from(path)),
            };
        }
        if let Some(value) = lookup("LOG_FILE_MAX_SIZE_MB").filter(|value| !value.is_empty()) {
            let megabytes = value
                .parse::<u64>()
                .map_err(|_| "LOG_FILE_MAX_SIZE_MB must be a positive integer".to_owned())?;
            if megabytes == 0 {
                return Err("LOG_FILE_MAX_SIZE_MB must be greater than zero".to_owned());
            }
            config.file_rotation.max_size_bytes = megabytes.saturating_mul(1024 * 1024);
        }
        if let Some(value) = lookup("LOG_FILE_ROTATE_HOURS").filter(|value| !value.is_empty()) {
            let hours = value
                .parse::<u64>()
                .map_err(|_| "LOG_FILE_ROTATE_HOURS must be a positive integer".to_owned())?;
            if hours == 0 {
                return Err("LOG_FILE_ROTATE_HOURS must be greater than zero".to_owned());
            }
            config.file_rotation.rotate_after = Duration::from_secs(hours.saturating_mul(60 * 60));
        }
        Ok(config)
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

fn parse_bool(key: &str, value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("{key} must be a boolean, got {value:?}")),
    }
}

/// Initializes the global tracing subscriber once.
pub fn init(config: &LoggerConfig) -> Result<(), String> {
    if INITIALIZED.get().is_some() {
        return Ok(());
    }
    let _initialization = INITIALIZE_LOCK
        .lock()
        .map_err(|_| "logger initialization lock is poisoned".to_owned())?;
    if INITIALIZED.get().is_some() {
        return Ok(());
    }

    let filter = EnvFilter::try_new(&config.level)
        .map_err(|error| format!("invalid log filter: {error}"))?;
    let writer = make_writer(config)?;
    let timer = config
        .time_format
        .as_ref()
        .map_or_else(ChronoUtc::rfc_3339, |value| ChronoUtc::new(value.clone()));
    let builder = fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_timer(timer)
        .with_target(config.caller_info)
        .with_file(config.caller_info)
        .with_line_number(config.caller_info)
        .with_ansi(config.ansi);
    let installed = match config.format {
        LogFormat::Json => builder.json().try_init().map_err(|error| error.to_string()),
        LogFormat::Text => builder.try_init().map_err(|error| error.to_string()),
    };
    if installed.is_ok() {
        let _ = INITIALIZED.set(());
    }
    installed
}

fn make_writer(config: &LoggerConfig) -> Result<BoxMakeWriter, String> {
    match &config.output {
        LogOutput::Stdout => Ok(BoxMakeWriter::new(io::stdout)),
        LogOutput::Stderr => Ok(BoxMakeWriter::new(io::stderr)),
        LogOutput::File(path) => {
            let writer = FileRotationWriter::new(path, config.file_rotation)
                .map_err(|error| format!("open log output {}: {error}", path.display()))?;
            let writer = SharedWriter(Arc::new(Mutex::new(Box::new(writer))));
            Ok(BoxMakeWriter::new(move || writer.clone()))
        }
    }
}

#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Box<dyn Write + Send>>>);

impl Write for SharedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("log writer lock is poisoned"))?
            .write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("log writer lock is poisoned"))?
            .flush()
    }
}

/// Rotates a file when its size or age reaches the configured threshold.
pub struct FileRotationWriter {
    file: Option<File>,
    path: PathBuf,
    policy: FileRotationConfig,
    current_size: u64,
    opened_at: Instant,
}

impl FileRotationWriter {
    pub fn new(path: impl AsRef<Path>, policy: FileRotationConfig) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let current_size = file.metadata()?.len();
        Ok(Self {
            file: Some(file),
            path,
            policy,
            current_size,
            opened_at: Instant::now(),
        })
    }

    fn should_rotate(&self, incoming: usize) -> bool {
        self.current_size
            .saturating_add(u64::try_from(incoming).unwrap_or(u64::MAX))
            > self.policy.max_size_bytes
            || self.opened_at.elapsed() >= self.policy.rotate_after
    }

    fn rotate(&mut self) -> io::Result<()> {
        let mut file = self
            .file
            .take()
            .ok_or_else(|| io::Error::other("log file is not open"))?;
        file.flush()?;
        drop(file);

        let suffix = Utc::now().format("%Y%m%d-%H%M%S%.6f");
        let rotated = PathBuf::from(format!("{}.{}", self.path.display(), suffix));
        if let Err(error) = rename(&self.path, &rotated) {
            self.file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .ok();
            return Err(error);
        }

        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            Ok(file) => self.file = Some(file),
            Err(error) => {
                let _ = rename(&rotated, &self.path);
                self.file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)
                    .ok();
                return Err(error);
            }
        }
        self.current_size = 0;
        self.opened_at = Instant::now();
        Ok(())
    }

    fn file_mut(&mut self) -> io::Result<&mut File> {
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::other("log file is not open"))
    }
}

impl Write for FileRotationWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.should_rotate(buffer.len()) {
            self.rotate()?;
        }
        let written = self.file_mut()?.write(buffer)?;
        self.current_size = self
            .current_size
            .saturating_add(u64::try_from(written).unwrap_or(u64::MAX));
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file_mut()?.flush()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        io::{Cursor, ErrorKind},
        thread,
    };

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    fn config_from(values: &[(&str, &str)]) -> Result<LoggerConfig, String> {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<BTreeMap<_, _>>();
        LoggerConfig::from_lookup(|key| values.get(key).cloned())
    }

    #[test]
    fn defaults_and_environment_lookup_cover_every_logging_option() {
        assert_eq!(config_from(&[]).unwrap(), LoggerConfig::default());

        let config = config_from(&[
            ("LOG_LEVEL", "debug"),
            ("LOG_FORMAT", "pretty"),
            ("LOG_TIME_FORMAT", "%Y-%m-%d"),
            ("LOG_CALLER_INFO", "false"),
            ("LOG_ANSI", "yes"),
            ("LOG_OUTPUT", "/tmp/platform.log"),
            ("LOG_FILE_MAX_SIZE_MB", "2"),
            ("LOG_FILE_ROTATE_HOURS", "3"),
        ])
        .unwrap();
        assert_eq!(config.level, "debug");
        assert_eq!(config.format, LogFormat::Text);
        assert_eq!(config.time_format.as_deref(), Some("%Y-%m-%d"));
        assert!(!config.caller_info);
        assert!(config.ansi);
        assert_eq!(
            config.output,
            LogOutput::File(PathBuf::from("/tmp/platform.log"))
        );
        assert_eq!(config.file_rotation.max_size_bytes, 2 * 1024 * 1024);
        assert_eq!(
            config.file_rotation.rotate_after,
            Duration::from_secs(3 * 60 * 60)
        );

        assert_eq!(
            config_from(&[("LOG_FORMAT", "JSON")]).unwrap().format,
            LogFormat::Json
        );
        assert_eq!(
            config_from(&[("LOG_OUTPUT", "stderr")]).unwrap().output,
            LogOutput::Stderr
        );
        assert_eq!(
            config_from(&[("LOG_OUTPUT", "stdout")]).unwrap().output,
            LogOutput::Stdout
        );
        assert_eq!(
            config_from(&[("LOG_LEVEL", "")]).unwrap(),
            LoggerConfig::default()
        );
    }

    #[test]
    fn environment_lookup_rejects_invalid_logging_options() {
        for values in [
            vec![("LOG_FORMAT", "xml")],
            vec![("LOG_CALLER_INFO", "maybe")],
            vec![("LOG_ANSI", "sometimes")],
            vec![("LOG_FILE_MAX_SIZE_MB", "many")],
            vec![("LOG_FILE_MAX_SIZE_MB", "0")],
            vec![("LOG_FILE_ROTATE_HOURS", "soon")],
            vec![("LOG_FILE_ROTATE_HOURS", "0")],
        ] {
            assert!(config_from(&values).is_err(), "{values:?}");
        }
    }

    #[test]
    fn boolean_parser_accepts_common_forms_and_rejects_unknown_values() {
        for value in ["1", "true", "YES", "on"] {
            assert_eq!(parse_bool("TEST", value), Ok(true));
        }
        for value in ["0", "false", "no", "off"] {
            assert_eq!(parse_bool("TEST", value), Ok(false));
        }
        assert!(parse_bool("TEST", "sometimes").is_err());
    }

    #[test]
    fn shared_writer_writes_flushes_and_reports_poisoned_locks() {
        let shared = Arc::new(Mutex::new(
            Box::new(Cursor::new(Vec::new())) as Box<dyn Write + Send>
        ));
        let mut writer = SharedWriter(shared);
        assert_eq!(writer.write(b"event").unwrap(), 5);
        writer.flush().unwrap();

        let poisoned = Arc::new(Mutex::new(
            Box::new(Cursor::new(Vec::new())) as Box<dyn Write + Send>
        ));
        let poison_target = poisoned.clone();
        let _ = thread::spawn(move || {
            let _guard = poison_target.lock().unwrap();
            panic!("poison writer lock");
        })
        .join();
        let mut writer = SharedWriter(poisoned);
        assert_eq!(writer.write(b"x").unwrap_err().kind(), ErrorKind::Other);
        assert_eq!(writer.flush().unwrap_err().kind(), ErrorKind::Other);
    }

    #[test]
    fn file_writer_rotates_before_exceeding_size_limit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("service.log");
        let mut writer = FileRotationWriter::new(
            &path,
            FileRotationConfig {
                max_size_bytes: 5,
                rotate_after: Duration::from_secs(60),
            },
        )
        .unwrap();
        writer.write_all(b"first").unwrap();
        writer.write_all(b"second").unwrap();
        writer.flush().unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "second");
        assert_eq!(
            fs::read_dir(directory.path())
                .unwrap()
                .filter_map(std::result::Result::ok)
                .count(),
            2
        );
    }

    #[test]
    fn file_writer_rotates_by_age_and_recovers_after_rename_failure() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("age.log");
        fs::write(&path, "existing").unwrap();
        let mut writer = FileRotationWriter::new(
            &path,
            FileRotationConfig {
                max_size_bytes: u64::MAX,
                rotate_after: Duration::ZERO,
            },
        )
        .unwrap();
        assert_eq!(writer.current_size, 8);
        writer.write_all(b"new").unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "new");

        fs::remove_file(&path).unwrap();
        let error = writer.rotate().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        assert!(writer.file.is_some());
    }

    #[test]
    fn file_writer_reports_closed_file_and_invalid_parent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("closed.log");
        let mut writer = FileRotationWriter::new(&path, FileRotationConfig::default()).unwrap();
        writer.file = None;
        assert_eq!(writer.file_mut().unwrap_err().kind(), ErrorKind::Other);
        assert_eq!(writer.rotate().unwrap_err().kind(), ErrorKind::Other);

        let missing = directory.path().join("missing").join("service.log");
        assert!(FileRotationWriter::new(missing, FileRotationConfig::default()).is_err());
    }

    #[test]
    fn writer_factory_supports_all_destinations_and_context_spans() {
        for output in [LogOutput::Stdout, LogOutput::Stderr] {
            let config = LoggerConfig {
                output,
                ..LoggerConfig::default()
            };
            let writer = make_writer(&config).unwrap();
            writer.make_writer().flush().unwrap();
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("factory.log");
        let config = LoggerConfig {
            output: LogOutput::File(path.clone()),
            ..LoggerConfig::default()
        };
        let writer = make_writer(&config).unwrap();
        let mut output = writer.make_writer();
        output.write_all(b"factory").unwrap();
        output.flush().unwrap();
        assert_eq!(fs::read_to_string(path).unwrap(), "factory");

        let missing = LoggerConfig {
            output: LogOutput::File(directory.path().join("missing").join("service.log")),
            ..LoggerConfig::default()
        };
        assert!(make_writer(&missing).is_err());
    }

    #[test]
    fn logger_init_rejects_invalid_filters_before_global_installation() {
        let config = LoggerConfig {
            level: "not a valid filter[".to_owned(),
            ..LoggerConfig::default()
        };
        assert!(init(&config).unwrap_err().contains("invalid log filter"));
    }
}
