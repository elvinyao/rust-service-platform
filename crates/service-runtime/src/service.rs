//! Lifecycle-managed input and demonstration services.

use std::{
    any::Any,
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use service_platform::{
    config::{
        BadgeDbAdapterConfig, ConfluenceAdapterConfig, MattermostAdapterConfig,
        WebSocketInputConfig, format_duration,
    },
    errors::{AppError, ErrorType, Result},
    health::{Category, CheckResult, Level, ServiceHealth, Status},
    ruleengine::{DEFAULT_WORKFLOW_NAME, Message},
};
use tokio::{
    sync::{Mutex, RwLock, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message as WsMessage, client::IntoClientRequest, http::HeaderValue,
        http::header::AUTHORIZATION,
    },
};

pub const WEBSOCKET_INPUT_SERVICE_NAME: &str = "WebSocketInputService";
pub const BADGEDB_SERVICE_NAME: &str = "BadgeDBService";
pub const MATTERMOST_SERVICE_NAME: &str = "MattermostService";
pub const CONFLUENCE_SETTINGS_SERVICE_NAME: &str = "ConfluenceSettingsService";
const ERROR_RESPONSE_PREVIEW_LIMIT: usize = 1024;

#[async_trait]
pub trait Service: ServiceHealth + Send + Sync {
    fn service_type(&self) -> &str;
    fn workflow(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;

    async fn restart(&self) -> Result<()> {
        self.stop().await?;
        self.start().await
    }
}

pub struct BaseService {
    name: String,
    workflow: String,
    service_type: String,
    version: String,
    running: AtomicBool,
    started_at: RwLock<Option<(DateTime<Utc>, Instant)>>,
    lifecycle: Mutex<()>,
}

impl BaseService {
    pub fn new(
        name: impl Into<String>,
        workflow: impl Into<String>,
        service_type: impl Into<String>,
        version: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            workflow: workflow.into(),
            service_type: service_type.into(),
            version: version.into(),
            running: AtomicBool::new(false),
            started_at: RwLock::new(None),
            lifecycle: Mutex::new(()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn workflow(&self) -> &str {
        &self.workflow
    }

    pub fn service_type(&self) -> &str {
        &self.service_type
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub async fn set_running(&self, running: bool) {
        let previous = self.running.swap(running, Ordering::SeqCst);
        if running && !previous {
            *self.started_at.write().await = Some((Utc::now(), Instant::now()));
        }
    }

    pub async fn metrics(&self) -> BTreeMap<String, Value> {
        let mut metrics = BTreeMap::from([
            ("running".to_owned(), json!(self.is_running())),
            ("type".to_owned(), json!(self.service_type)),
            ("workflow".to_owned(), json!(self.workflow)),
            ("version".to_owned(), json!(self.version)),
        ]);
        if let Some((timestamp, instant)) = *self.started_at.read().await {
            metrics.insert("start_time".to_owned(), json!(timestamp.to_rfc3339()));
            metrics.insert(
                "uptime_seconds".to_owned(),
                json!(instant.elapsed().as_secs_f64()),
            );
        }
        metrics
    }
}

pub type MessageHandler = Arc<
    dyn Fn(Message) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync + 'static,
>;

struct WebSocketRuntime {
    connections: usize,
    last_message: Option<(DateTime<Utc>, Instant)>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

pub struct WebSocketService {
    base: BaseService,
    config: RwLock<WebSocketInputConfig>,
    runtime: Arc<Mutex<WebSocketRuntime>>,
    handler: Arc<RwLock<Option<MessageHandler>>>,
}

impl WebSocketService {
    pub fn new(config: WebSocketInputConfig, version: impl Into<String>) -> Self {
        Self {
            base: BaseService::new(
                WEBSOCKET_INPUT_SERVICE_NAME,
                DEFAULT_WORKFLOW_NAME,
                "websocket",
                version,
            ),
            config: RwLock::new(config),
            runtime: Arc::new(Mutex::new(WebSocketRuntime {
                connections: 0,
                last_message: None,
                shutdown: None,
                task: None,
            })),
            handler: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn on_message(&self, handler: MessageHandler) {
        *self.handler.write().await = Some(handler);
    }

    pub async fn configure(&self, config: WebSocketInputConfig) -> Result<()> {
        validate_websocket_config(&config)?;
        *self.config.write().await = config;
        Ok(())
    }

    pub async fn process_incoming_message(&self, message: Message) -> Result<()> {
        if !self.base.is_running() {
            return Err(AppError::new(
                ErrorType::ServiceUnavailable,
                "Service is not running",
            ));
        }
        {
            self.runtime.lock().await.last_message = Some((Utc::now(), Instant::now()));
        }
        if let Some(handler) = self.handler.read().await.clone() {
            handler(message).await;
        }
        Ok(())
    }
}

fn validate_websocket_config(config: &WebSocketInputConfig) -> Result<()> {
    let url = url::Url::parse(&config.server_url).map_err(|error| {
        AppError::wrap(
            ErrorType::InvalidInput,
            "invalid WebSocket server URL",
            error,
        )
    })?;
    if !matches!(url.scheme(), "ws" | "wss") || url.host_str().is_none() {
        return invalid("server URL must be an absolute ws or wss URL");
    }
    if !config.path.starts_with('/') {
        return invalid("path must start with /");
    }
    if config.reconnect_interval.is_zero() {
        return invalid("reconnect interval must be greater than zero");
    }
    Ok(())
}

async fn websocket_loop(
    config: WebSocketInputConfig,
    runtime: Arc<Mutex<WebSocketRuntime>>,
    handler: Arc<RwLock<Option<MessageHandler>>>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let endpoint = format!("{}{}", config.server_url.trim_end_matches('/'), config.path);
    loop {
        let connection = tokio::select! {
            _ = &mut shutdown => break,
            result = connect_async(&endpoint) => result,
        };
        let Ok((stream, _)) = connection else {
            tokio::select! {
                _ = &mut shutdown => break,
                () = sleep(config.reconnect_interval) => {}
            }
            continue;
        };
        runtime.lock().await.connections = 1;
        let (_, mut reader) = stream.split();
        let mut stopping = false;
        loop {
            let incoming = tokio::select! {
                _ = &mut shutdown => {
                    stopping = true;
                    None
                },
                message = reader.next() => message,
            };
            let Some(Ok(message)) = incoming else {
                break;
            };
            if !message.is_text() && !message.is_binary() {
                continue;
            }
            let parsed = serde_json::from_slice::<Message>(&message.into_data());
            let Ok(message) = parsed else {
                continue;
            };
            if message.kind == "system" {
                continue;
            }
            runtime.lock().await.last_message = Some((Utc::now(), Instant::now()));
            if let Some(handler) = handler.read().await.clone() {
                handler(message).await;
            }
        }
        runtime.lock().await.connections = 0;
        if stopping {
            break;
        }
        tokio::select! {
            _ = &mut shutdown => break,
            () = sleep(config.reconnect_interval) => {}
        }
    }
    runtime.lock().await.connections = 0;
}

#[async_trait]
impl Service for WebSocketService {
    fn service_type(&self) -> &str {
        self.base.service_type()
    }

    fn workflow(&self) -> &str {
        self.base.workflow()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn start(&self) -> Result<()> {
        let _lifecycle = self.base.lifecycle.lock().await;
        if self.base.is_running() {
            return Ok(());
        }
        let config = self.config.read().await.clone();
        validate_websocket_config(&config)?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(websocket_loop(
            config,
            self.runtime.clone(),
            self.handler.clone(),
            shutdown_rx,
        ));
        let mut runtime = self.runtime.lock().await;
        runtime.shutdown = Some(shutdown_tx);
        runtime.task = Some(task);
        drop(runtime);
        self.base.set_running(true).await;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let _lifecycle = self.base.lifecycle.lock().await;
        if !self.base.is_running() {
            return Ok(());
        }
        self.base.set_running(false).await;
        let (shutdown, task) = {
            let mut runtime = self.runtime.lock().await;
            runtime.connections = 0;
            (runtime.shutdown.take(), runtime.task.take())
        };
        if let Some(shutdown) = shutdown {
            let _ = shutdown.send(());
        }
        if let Some(task) = task {
            join_background_task(
                task,
                Duration::from_secs(5),
                "timed out waiting for WebSocket shutdown",
                "join WebSocket task",
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ServiceHealth for WebSocketService {
    fn name(&self) -> &str {
        self.base.name()
    }

    async fn is_running(&self) -> bool {
        self.base.is_running()
    }

    async fn metrics(&self) -> BTreeMap<String, Value> {
        let mut metrics = self.base.metrics().await;
        let config = self.config.read().await;
        let runtime = self.runtime.lock().await;
        metrics.insert("connections".to_owned(), json!(runtime.connections));
        metrics.insert("server_url".to_owned(), json!(config.server_url));
        metrics.insert("path".to_owned(), json!(config.path));
        metrics.insert(
            "reconnect_interval".to_owned(),
            json!(format_duration(config.reconnect_interval)),
        );
        if let Some((timestamp, instant)) = runtime.last_message {
            metrics.insert("last_message".to_owned(), json!(timestamp.to_rfc3339()));
            metrics.insert(
                "last_message_age_seconds".to_owned(),
                json!(instant.elapsed().as_secs_f64()),
            );
        }
        metrics
    }

    async fn custom_checks(&self) -> Vec<CheckResult> {
        let runtime = self.runtime.lock().await;
        let mut result = CheckResult::new("websocket-connections", Category::Connectivity)
            .with_level(Level::Critical)
            .with_detail("connections", json!(runtime.connections));
        if !self.base.is_running() {
            result = result.with_status(Status::Down, "WebSocket service is not running");
        } else if runtime.connections == 0 {
            result = result.with_status(Status::Down, "No active WebSocket connections");
        } else if runtime
            .last_message
            .is_some_and(|(_, instant)| instant.elapsed() > Duration::from_secs(30 * 60))
        {
            result = result.with_status(Status::Degraded, "No messages received in 30m");
        } else {
            result = result.with_status(
                Status::Up,
                format!("Active WebSocket connections: {}", runtime.connections),
            );
        }
        vec![result]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Badge {
    pub id: String,
    pub name: String,
    pub description: String,
    pub user_id: String,
    pub awarded_at: DateTime<Utc>,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
    pub revoked: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<DateTime<Utc>>,
}

pub struct BadgeDbService {
    base: BaseService,
    config: RwLock<BadgeDbAdapterConfig>,
    badges: RwLock<BTreeMap<String, Badge>>,
    last_backup: RwLock<Option<(DateTime<Utc>, Instant)>>,
}

impl BadgeDbService {
    pub fn new(config: BadgeDbAdapterConfig, version: impl Into<String>) -> Self {
        Self {
            base: BaseService::new(
                BADGEDB_SERVICE_NAME,
                DEFAULT_WORKFLOW_NAME,
                "badgedb",
                version,
            ),
            config: RwLock::new(config),
            badges: RwLock::new(BTreeMap::new()),
            last_backup: RwLock::new(None),
        }
    }

    pub async fn save_badge(&self, badge: Badge) -> Result<()> {
        if !self.base.is_running() {
            return invalid("service not running");
        }
        self.badges.write().await.insert(badge.id.clone(), badge);
        self.backup().await;
        Ok(())
    }

    pub async fn badge(&self, id: &str) -> Result<Badge> {
        if !self.base.is_running() {
            return invalid("service not running");
        }
        self.badges
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| AppError::new(ErrorType::NotFound, format!("badge not found: {id}")))
    }

    pub async fn configure(&self, config: BadgeDbAdapterConfig) -> Result<()> {
        validate_badgedb_config(&config)?;
        *self.config.write().await = config;
        Ok(())
    }

    pub fn processing_method(message_type: &str) -> Result<&'static str> {
        match message_type {
            "badge_award" => Ok("StandardAward"),
            "badge_revoke" => Ok("StandardRevoke"),
            "badge_transfer" => Ok("SecureTransfer"),
            _ => invalid(format!(
                "no processing method found for message type: {message_type}"
            )),
        }
    }

    async fn backup(&self) {
        *self.last_backup.write().await = Some((Utc::now(), Instant::now()));
    }
}

fn validate_badgedb_config(config: &BadgeDbAdapterConfig) -> Result<()> {
    if config.path.as_os_str().is_empty() {
        return invalid("database path is required");
    }
    Ok(())
}

#[async_trait]
impl Service for BadgeDbService {
    fn service_type(&self) -> &str {
        self.base.service_type()
    }

    fn workflow(&self) -> &str {
        self.base.workflow()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn start(&self) -> Result<()> {
        let _lifecycle = self.base.lifecycle.lock().await;
        {
            let config = self.config.read().await;
            validate_badgedb_config(&config)?;
        }
        self.base.set_running(true).await;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let _lifecycle = self.base.lifecycle.lock().await;
        if self.base.is_running() {
            self.backup().await;
            self.base.set_running(false).await;
        }
        Ok(())
    }
}

#[async_trait]
impl ServiceHealth for BadgeDbService {
    fn name(&self) -> &str {
        self.base.name()
    }

    async fn is_running(&self) -> bool {
        self.base.is_running()
    }

    async fn metrics(&self) -> BTreeMap<String, Value> {
        let mut metrics = self.base.metrics().await;
        metrics.insert(
            "db_entries".to_owned(),
            json!(self.badges.read().await.len()),
        );
        metrics.insert(
            "db_path".to_owned(),
            json!(self.config.read().await.path.display().to_string()),
        );
        if let Some((timestamp, _)) = *self.last_backup.read().await {
            metrics.insert("last_backup".to_owned(), json!(timestamp.to_rfc3339()));
        }
        metrics
    }

    async fn custom_checks(&self) -> Vec<CheckResult> {
        let access = CheckResult::new("badgedb-access", Category::Data)
            .with_level(Level::Critical)
            .with_status(
                if self.base.is_running() {
                    Status::Up
                } else {
                    Status::Down
                },
                if self.base.is_running() {
                    "Database access is working"
                } else {
                    "BadgeDB service is not running"
                },
            );
        let backup = match *self.last_backup.read().await {
            None => CheckResult::new("badgedb-backup", Category::Data)
                .with_level(Level::Warning)
                .with_status(Status::Degraded, "No backup has been performed"),
            Some((_, instant)) if instant.elapsed() > Duration::from_secs(24 * 60 * 60) => {
                CheckResult::new("badgedb-backup", Category::Data)
                    .with_level(Level::Warning)
                    .with_status(Status::Degraded, "Backup is more than 24h old")
            }
            Some(_) => CheckResult::new("badgedb-backup", Category::Data)
                .with_level(Level::Warning)
                .with_status(Status::Up, "Backup is current"),
        };
        vec![access, backup]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingRule {
    pub event_type: String,
    pub pattern: String,
    pub channel_id: String,
    pub message_template: String,
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
struct ConfluencePage {
    body: ConfluenceBody,
}

#[derive(Debug, Deserialize)]
struct ConfluenceBody {
    storage: ConfluenceStorage,
}

#[derive(Debug, Deserialize)]
struct ConfluenceStorage {
    value: String,
}

#[derive(Debug, Deserialize)]
struct ConfluenceSettings {
    rules: Vec<SettingRule>,
}

struct RefreshRuntime {
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

type LastRefresh = Arc<RwLock<Option<(DateTime<Utc>, Instant)>>>;

pub struct ConfluenceSettingsService {
    base: BaseService,
    config: RwLock<ConfluenceAdapterConfig>,
    settings: Arc<RwLock<Vec<SettingRule>>>,
    last_refresh: LastRefresh,
    runtime: Mutex<RefreshRuntime>,
    client: reqwest::Client,
    cache: CacheDataAccessor,
}

impl ConfluenceSettingsService {
    pub fn new(config: ConfluenceAdapterConfig, version: impl Into<String>) -> Result<Self> {
        Self::with_cache(config, version, CacheDataAccessor::default())
    }

    pub fn with_cache(
        config: ConfluenceAdapterConfig,
        version: impl Into<String>,
        cache: CacheDataAccessor,
    ) -> Result<Self> {
        validate_confluence_config(&config)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| {
                AppError::wrap(ErrorType::Internal, "build Confluence HTTP client", error)
            })?;
        Ok(Self::with_dependencies(config, version, client, cache))
    }

    pub fn with_dependencies(
        config: ConfluenceAdapterConfig,
        version: impl Into<String>,
        client: reqwest::Client,
        cache: CacheDataAccessor,
    ) -> Self {
        Self {
            base: BaseService::new(
                CONFLUENCE_SETTINGS_SERVICE_NAME,
                DEFAULT_WORKFLOW_NAME,
                "confluence-settings",
                version,
            ),
            config: RwLock::new(config),
            settings: Arc::new(RwLock::new(Vec::new())),
            last_refresh: Arc::new(RwLock::new(None)),
            runtime: Mutex::new(RefreshRuntime {
                shutdown: None,
                task: None,
            }),
            client,
            cache,
        }
    }

    pub async fn settings(&self) -> Vec<SettingRule> {
        self.settings.read().await.clone()
    }

    pub async fn match_event(&self, event_type: &str) -> Vec<SettingRule> {
        self.settings
            .read()
            .await
            .iter()
            .filter(|rule| rule.enabled && rule.event_type == event_type)
            .cloned()
            .collect()
    }

    pub async fn refresh(&self) -> Result<()> {
        let config = self.config.read().await.clone();
        let settings = fetch_confluence_settings(&self.client, &config).await?;
        let cached = serde_json::to_value(&settings).map_err(|error| {
            AppError::wrap(
                ErrorType::Internal,
                "serialize Confluence settings for cache",
                error,
            )
        })?;
        *self.settings.write().await = settings.clone();
        *self.last_refresh.write().await = Some((Utc::now(), Instant::now()));
        self.cache.set("confluence_settings", cached).await;
        Ok(())
    }

    pub async fn configure(&self, config: ConfluenceAdapterConfig) -> Result<()> {
        validate_confluence_config(&config)?;
        *self.config.write().await = config;
        Ok(())
    }
}

fn validate_confluence_config(config: &ConfluenceAdapterConfig) -> Result<()> {
    let endpoint = url::Url::parse(&config.api_endpoint).map_err(|error| {
        AppError::wrap(
            ErrorType::InvalidInput,
            "invalid Confluence API endpoint",
            error,
        )
    })?;
    if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
        return invalid("Confluence API endpoint must be an absolute HTTP or HTTPS URL");
    }
    if config.settings_page_id.is_empty() {
        return invalid("Confluence settings page ID is required");
    }
    if config.refresh_interval.is_zero() {
        return invalid("Confluence refresh interval must be greater than zero");
    }
    Ok(())
}

async fn fetch_confluence_settings(
    client: &reqwest::Client,
    config: &ConfluenceAdapterConfig,
) -> Result<Vec<SettingRule>> {
    let endpoint = format!(
        "{}/rest/api/content/{}",
        config.api_endpoint.trim_end_matches('/'),
        config.settings_page_id
    );
    let mut response = client.get(endpoint).send().await.map_err(|error| {
        AppError::wrap(
            ErrorType::ServiceUnavailable,
            "failed to fetch Confluence page",
            error,
        )
    })?;
    let status = response.status();
    if !status.is_success() {
        let body = response_preview(&mut response, ERROR_RESPONSE_PREVIEW_LIMIT).await;
        return Err(AppError::new(
            ErrorType::ServiceUnavailable,
            format!("Confluence API returned status {}: {body}", status.as_u16()),
        ));
    }
    let page = response.json::<ConfluencePage>().await.map_err(|error| {
        AppError::wrap(
            ErrorType::InvalidInput,
            "failed to decode Confluence page",
            error,
        )
    })?;
    serde_json::from_str::<ConfluenceSettings>(&page.body.storage.value)
        .map(|settings| settings.rules)
        .map_err(|error| {
            AppError::wrap(
                ErrorType::InvalidInput,
                "failed to parse settings from page body",
                error,
            )
        })
}

#[async_trait]
impl Service for ConfluenceSettingsService {
    fn service_type(&self) -> &str {
        self.base.service_type()
    }

    fn workflow(&self) -> &str {
        self.base.workflow()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn start(&self) -> Result<()> {
        let _lifecycle = self.base.lifecycle.lock().await;
        if self.base.is_running() {
            return Ok(());
        }
        let config = self.config.read().await.clone();
        validate_confluence_config(&config)?;
        if let Err(error) = self.refresh().await {
            tracing::warn!(error = %error, "Failed to fetch initial Confluence settings");
        }
        let client = self.client.clone();
        let settings = self.settings.clone();
        let last_refresh = self.last_refresh.clone();
        let cache = self.cache.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    () = sleep(config.refresh_interval) => {}
                }
                let refreshed = tokio::select! {
                    _ = &mut shutdown_rx => break,
                    result = fetch_confluence_settings(&client, &config) => result,
                };
                match refreshed {
                    Ok(next) => match serde_json::to_value(&next) {
                        Ok(cached) => {
                            *settings.write().await = next;
                            *last_refresh.write().await = Some((Utc::now(), Instant::now()));
                            cache.set("confluence_settings", cached).await;
                        }
                        Err(error) => tracing::warn!(
                            error = %error,
                            "Failed to serialize Confluence settings"
                        ),
                    },
                    Err(error) => {
                        tracing::warn!(error = %error, "Failed to refresh Confluence settings");
                    }
                }
            }
        });
        let mut runtime = self.runtime.lock().await;
        runtime.shutdown = Some(shutdown_tx);
        runtime.task = Some(task);
        drop(runtime);
        self.base.set_running(true).await;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let _lifecycle = self.base.lifecycle.lock().await;
        if !self.base.is_running() {
            return Ok(());
        }
        self.base.set_running(false).await;
        let (shutdown, task) = {
            let mut runtime = self.runtime.lock().await;
            (runtime.shutdown.take(), runtime.task.take())
        };
        if let Some(shutdown) = shutdown {
            let _ = shutdown.send(());
        }
        if let Some(task) = task {
            join_background_task(
                task,
                Duration::from_secs(5),
                "timed out waiting for periodic refresh shutdown",
                "join refresh task",
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ServiceHealth for ConfluenceSettingsService {
    fn name(&self) -> &str {
        self.base.name()
    }

    async fn is_running(&self) -> bool {
        self.base.is_running()
    }

    async fn metrics(&self) -> BTreeMap<String, Value> {
        let mut metrics = self.base.metrics().await;
        let config = self.config.read().await;
        metrics.insert(
            "settings_count".to_owned(),
            json!(self.settings.read().await.len()),
        );
        metrics.insert(
            "refresh_interval".to_owned(),
            json!(format_duration(config.refresh_interval)),
        );
        metrics.insert("api_endpoint".to_owned(), json!(config.api_endpoint));
        metrics.insert("page_id".to_owned(), json!(config.settings_page_id));
        if let Some((timestamp, instant)) = *self.last_refresh.read().await {
            metrics.insert("last_refresh".to_owned(), json!(timestamp.to_rfc3339()));
            metrics.insert(
                "last_refresh_age_seconds".to_owned(),
                json!(instant.elapsed().as_secs_f64()),
            );
        }
        metrics
    }

    async fn custom_checks(&self) -> Vec<CheckResult> {
        let config = self.config.read().await;
        let result = match *self.last_refresh.read().await {
            None => CheckResult::new("settings-refresh", Category::Connectivity)
                .with_level(Level::Warning)
                .with_status(Status::Degraded, "Settings have never been refreshed"),
            Some((_, instant)) if instant.elapsed() > config.refresh_interval * 2 => {
                CheckResult::new("settings-refresh", Category::Connectivity)
                    .with_level(Level::Warning)
                    .with_status(Status::Degraded, "Settings are stale")
            }
            Some(_) => CheckResult::new("settings-refresh", Category::Connectivity)
                .with_level(Level::Warning)
                .with_status(Status::Up, "Settings refresh is current"),
        };
        vec![result]
    }
}

struct MattermostRuntime {
    websocket_connected: bool,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    auth_token: String,
}

pub struct MattermostService {
    base: BaseService,
    config: RwLock<MattermostAdapterConfig>,
    client: reqwest::Client,
    runtime: Arc<Mutex<MattermostRuntime>>,
}

impl MattermostService {
    pub fn new(config: MattermostAdapterConfig, version: impl Into<String>) -> Result<Self> {
        validate_mattermost_config(&config)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| {
                AppError::wrap(ErrorType::Internal, "build Mattermost HTTP client", error)
            })?;
        Ok(Self::with_client(config, version, client))
    }

    pub fn with_client(
        config: MattermostAdapterConfig,
        version: impl Into<String>,
        client: reqwest::Client,
    ) -> Self {
        Self {
            base: BaseService::new(
                MATTERMOST_SERVICE_NAME,
                DEFAULT_WORKFLOW_NAME,
                "mattermost",
                version,
            ),
            runtime: Arc::new(Mutex::new(MattermostRuntime {
                websocket_connected: false,
                shutdown: None,
                task: None,
                auth_token: config.api_token.clone(),
            })),
            config: RwLock::new(config),
            client,
        }
    }

    pub async fn send_message(&self, message: &str) -> Result<()> {
        let channel = self.config.read().await.channel.clone();
        self.send_message_to_channel(&channel, message).await
    }

    pub async fn send_message_to_channel(&self, channel: &str, message: &str) -> Result<()> {
        if !self.base.is_running() {
            return invalid("mattermost service is not running");
        }
        if channel.is_empty() {
            return invalid("channel ID is required");
        }
        let config = self.config.read().await;
        let token = self.runtime.lock().await.auth_token.clone();
        let endpoint = format!("{}/api/v4/posts", config.server_url.trim_end_matches('/'));
        let mut request = self
            .client
            .post(endpoint)
            .json(&json!({"channel_id": channel, "message": message}));
        if !token.is_empty() {
            request = request.bearer_auth(token);
        }
        let mut response = request.send().await.map_err(|error| {
            AppError::wrap(ErrorType::ServiceUnavailable, "send Mattermost post", error)
        })?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response_preview(&mut response, ERROR_RESPONSE_PREVIEW_LIMIT).await;
        Err(AppError::new(
            ErrorType::ServiceUnavailable,
            format!(
                "Mattermost post returned status {}: {body}",
                status.as_u16()
            ),
        ))
    }

    pub async fn configure(&self, config: MattermostAdapterConfig) -> Result<()> {
        validate_mattermost_config(&config)?;
        *self.config.write().await = config;
        Ok(())
    }
}

async fn login_mattermost(
    client: &reqwest::Client,
    config: &MattermostAdapterConfig,
) -> Result<String> {
    let endpoint = format!(
        "{}/api/v4/users/login",
        config.server_url.trim_end_matches('/')
    );
    let mut response = client
        .post(endpoint)
        .json(&json!({
            "login_id": config.username,
            "password": config.password,
        }))
        .send()
        .await
        .map_err(|error| {
            AppError::wrap(
                ErrorType::ServiceUnavailable,
                "execute Mattermost login",
                error,
            )
        })?;
    let status = response.status();
    if !status.is_success() {
        let body = response_preview(&mut response, ERROR_RESPONSE_PREVIEW_LIMIT).await;
        return Err(AppError::new(
            ErrorType::Unauthorized,
            format!(
                "Mattermost login returned status {}: {body}",
                status.as_u16()
            ),
        ));
    }
    response
        .headers()
        .get("token")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            AppError::new(
                ErrorType::Unauthorized,
                "Mattermost login response did not include a token",
            )
        })
}

fn validate_mattermost_config(config: &MattermostAdapterConfig) -> Result<()> {
    let server = url::Url::parse(&config.server_url).map_err(|error| {
        AppError::wrap(
            ErrorType::InvalidInput,
            "invalid Mattermost server URL",
            error,
        )
    })?;
    if !matches!(server.scheme(), "http" | "https") || server.host_str().is_none() {
        return invalid("Mattermost server URL must be an absolute HTTP or HTTPS URL");
    }
    if config.channel.is_empty() {
        return invalid("Mattermost channel is required");
    }
    if config.api_token.is_empty() && (config.username.is_empty() || config.password.is_empty()) {
        return invalid("Mattermost requires an API token or both username and password");
    }
    if !config.websocket_url.is_empty() {
        let websocket = url::Url::parse(&config.websocket_url).map_err(|error| {
            AppError::wrap(
                ErrorType::InvalidInput,
                "invalid Mattermost WebSocket URL",
                error,
            )
        })?;
        if !matches!(websocket.scheme(), "ws" | "wss") || websocket.host_str().is_none() {
            return invalid("Mattermost WebSocket URL must be an absolute WS or WSS URL");
        }
    }
    Ok(())
}

fn mattermost_websocket_endpoint(base: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with("/api/v4/websocket") {
        base.to_owned()
    } else {
        format!("{base}/api/v4/websocket")
    }
}

async fn mattermost_websocket_loop(
    websocket_url: String,
    token: String,
    runtime: Arc<Mutex<MattermostRuntime>>,
    mut shutdown: oneshot::Receiver<()>,
) {
    if websocket_url.is_empty() {
        return;
    }
    let endpoint = mattermost_websocket_endpoint(&websocket_url);
    loop {
        let mut request = match endpoint.clone().into_client_request() {
            Ok(request) => request,
            Err(error) => {
                tracing::error!(error = %error, "Invalid Mattermost WebSocket request");
                break;
            }
        };
        if !token.is_empty() {
            match HeaderValue::from_str(&format!("Bearer {token}")) {
                Ok(value) => {
                    request.headers_mut().insert(AUTHORIZATION, value);
                }
                Err(error) => {
                    tracing::error!(error = %error, "Invalid Mattermost authorization header");
                    break;
                }
            }
        }
        let connection = tokio::select! {
            _ = &mut shutdown => break,
            result = timeout(Duration::from_secs(5), connect_async(request)) => result,
        };
        let Ok(Ok((mut socket, _))) = connection else {
            tokio::select! {
                _ = &mut shutdown => break,
                () = sleep(Duration::from_secs(5)) => {}
            }
            continue;
        };
        runtime.lock().await.websocket_connected = true;
        if !token.is_empty() {
            let _ = socket
                .send(WsMessage::Text(
                    json!({
                        "seq": 1,
                        "action": "authentication_challenge",
                        "data": {"token": token}
                    })
                    .to_string(),
                ))
                .await;
        }
        let mut stopping = false;
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    stopping = true;
                    break;
                },
                message = socket.next() => {
                    if message.is_none_or(|message| message.is_err()) {
                        break;
                    }
                }
            }
        }
        runtime.lock().await.websocket_connected = false;
        if stopping {
            break;
        }
        tokio::select! {
            _ = &mut shutdown => break,
            () = sleep(Duration::from_secs(5)) => {}
        }
    }
    runtime.lock().await.websocket_connected = false;
}

#[async_trait]
impl Service for MattermostService {
    fn service_type(&self) -> &str {
        self.base.service_type()
    }

    fn workflow(&self) -> &str {
        self.base.workflow()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn start(&self) -> Result<()> {
        let _lifecycle = self.base.lifecycle.lock().await;
        if self.base.is_running() {
            return Ok(());
        }
        let config = self.config.read().await.clone();
        validate_mattermost_config(&config)?;
        let token = if config.api_token.is_empty() {
            login_mattermost(&self.client, &config).await?
        } else {
            config.api_token.clone()
        };
        self.runtime.lock().await.auth_token.clone_from(&token);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(mattermost_websocket_loop(
            config.websocket_url,
            token,
            self.runtime.clone(),
            shutdown_rx,
        ));
        let mut runtime = self.runtime.lock().await;
        runtime.shutdown = Some(shutdown_tx);
        runtime.task = Some(task);
        drop(runtime);
        self.base.set_running(true).await;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let _lifecycle = self.base.lifecycle.lock().await;
        if !self.base.is_running() {
            return Ok(());
        }
        self.base.set_running(false).await;
        let (shutdown, task) = {
            let mut runtime = self.runtime.lock().await;
            runtime.websocket_connected = false;
            (runtime.shutdown.take(), runtime.task.take())
        };
        if let Some(shutdown) = shutdown {
            let _ = shutdown.send(());
        }
        if let Some(task) = task {
            join_background_task(
                task,
                Duration::from_secs(5),
                "timed out waiting for Mattermost shutdown",
                "join Mattermost task",
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ServiceHealth for MattermostService {
    fn name(&self) -> &str {
        self.base.name()
    }

    async fn is_running(&self) -> bool {
        self.base.is_running()
    }

    async fn metrics(&self) -> BTreeMap<String, Value> {
        let mut metrics = self.base.metrics().await;
        let config = self.config.read().await;
        let runtime = self.runtime.lock().await;
        metrics.insert("server_url".to_owned(), json!(config.server_url));
        metrics.insert("websocket_url".to_owned(), json!(config.websocket_url));
        metrics.insert("channel".to_owned(), json!(config.channel));
        metrics.insert(
            "api_token_configured".to_owned(),
            json!(!runtime.auth_token.is_empty()),
        );
        metrics
    }

    async fn custom_checks(&self) -> Vec<CheckResult> {
        let config = self.config.read().await;
        let connected = self.runtime.lock().await.websocket_connected;
        let result = if config.websocket_url.is_empty() || connected {
            CheckResult::new("mattermost-connection", Category::Connectivity)
                .with_level(Level::Warning)
                .with_status(Status::Up, "Mattermost HTTP actions are available")
        } else {
            CheckResult::new("mattermost-connection", Category::Connectivity)
                .with_level(Level::Warning)
                .with_status(
                    Status::Degraded,
                    "Mattermost WebSocket is unavailable; HTTP actions remain available",
                )
        };
        vec![result]
    }
}

#[derive(Clone, Default)]
pub struct CacheDataAccessor {
    values: Arc<RwLock<BTreeMap<String, Value>>>,
}

impl CacheDataAccessor {
    pub async fn get(&self, key: &str) -> Option<Value> {
        self.values.read().await.get(key).cloned()
    }

    pub async fn set(&self, key: impl Into<String>, value: Value) {
        self.values.write().await.insert(key.into(), value);
    }
}

async fn join_background_task(
    mut task: JoinHandle<()>,
    wait: Duration,
    timeout_message: &'static str,
    join_message: &'static str,
) -> Result<()> {
    match timeout(wait, &mut task).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(AppError::wrap(ErrorType::Internal, join_message, error)),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err(AppError::new(ErrorType::Timeout, timeout_message))
        }
    }
}

async fn response_preview(response: &mut reqwest::Response, limit: usize) -> String {
    let mut preview = Vec::with_capacity(limit);
    while preview.len() < limit {
        let Ok(Some(chunk)) = response.chunk().await else {
            break;
        };
        let remaining = limit - preview.len();
        preview.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    String::from_utf8_lossy(&preview).into_owned()
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(AppError::new(ErrorType::InvalidInput, message))
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use axum::{
        Router,
        http::{HeaderValue as AxumHeaderValue, StatusCode as HttpStatus},
        response::IntoResponse,
    };
    use service_platform::config::RuntimeConfig;
    use tokio::{net::TcpListener, task::JoinHandle};

    use super::*;

    async fn static_server(
        status: HttpStatus,
        body: String,
        token: Option<&str>,
    ) -> (String, JoinHandle<()>) {
        let token = token.map(str::to_owned);
        let app = Router::new().fallback(move || {
            let body = body.clone();
            let token = token.clone();
            async move {
                let mut response =
                    (status, [("content-type", "application/json")], body).into_response();
                if let Some(token) = token {
                    response.headers_mut().insert(
                        "token",
                        AxumHeaderValue::from_str(&token).expect("valid test token"),
                    );
                }
                response
            }
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{address}"), task)
    }

    async fn unused_http_url() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{address}")
    }

    fn badge(id: &str) -> Badge {
        Badge {
            id: id.to_owned(),
            name: "badge".to_owned(),
            description: "test".to_owned(),
            user_id: "u1".to_owned(),
            awarded_at: Utc::now(),
            kind: "test".to_owned(),
            attributes: BTreeMap::new(),
            revoked: false,
            revoked_at: None,
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn background_task_join_handles_success_failure_and_timeout() {
        join_background_task(
            tokio::spawn(async {}),
            Duration::from_secs(1),
            "timeout",
            "join task",
        )
        .await
        .unwrap();

        let error = join_background_task(
            tokio::spawn(async { panic!("task panic") }),
            Duration::from_secs(1),
            "timeout",
            "join task",
        )
        .await
        .unwrap_err();
        assert_eq!(error.error_type, ErrorType::Internal);
        assert!(error.to_string().contains("join task"));
        assert!(std::error::Error::source(&error).is_some());

        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let task = tokio::spawn(async move {
            let _flag = DropFlag(task_dropped);
            pending::<()>().await;
        });
        let error = join_background_task(task, Duration::from_millis(1), "timeout", "join task")
            .await
            .unwrap_err();
        assert_eq!(error.error_type, ErrorType::Timeout);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn base_service_tracks_identity_running_state_and_metrics() {
        let base = BaseService::new("name", "workflow", "type", "1.2.3");
        assert_eq!(base.name(), "name");
        assert_eq!(base.workflow(), "workflow");
        assert_eq!(base.service_type(), "type");
        assert_eq!(base.version(), "1.2.3");
        assert!(!base.is_running());
        let stopped = base.metrics().await;
        assert_eq!(stopped["running"], json!(false));
        assert!(!stopped.contains_key("start_time"));

        base.set_running(true).await;
        base.set_running(true).await;
        let running = base.metrics().await;
        assert_eq!(running["running"], json!(true));
        assert!(running.contains_key("start_time"));
        assert!(running.contains_key("uptime_seconds"));
        base.set_running(false).await;
        assert!(!base.is_running());
    }

    #[tokio::test]
    async fn websocket_service_validates_config_processes_messages_and_reports_health() {
        let default = RuntimeConfig::default().inputs.websocket;
        for mut config in [
            WebSocketInputConfig {
                server_url: "invalid".to_owned(),
                ..default.clone()
            },
            WebSocketInputConfig {
                server_url: "http://example.com".to_owned(),
                ..default.clone()
            },
            WebSocketInputConfig {
                path: "events".to_owned(),
                ..default.clone()
            },
            WebSocketInputConfig {
                reconnect_interval: Duration::ZERO,
                ..default.clone()
            },
        ] {
            assert!(validate_websocket_config(&config).is_err());
            config.enabled = false;
        }

        let service = WebSocketService::new(default.clone(), "test");
        assert_eq!(service.service_type(), "websocket");
        assert_eq!(service.workflow(), DEFAULT_WORKFLOW_NAME);
        assert!(service.as_any().is::<WebSocketService>());
        assert!(
            service
                .process_incoming_message(Message::default())
                .await
                .is_err()
        );
        assert_eq!(service.custom_checks().await[0].status, Status::Down);

        let handled = Arc::new(AtomicUsize::new(0));
        let observed = handled.clone();
        service
            .on_message(Arc::new(move |_| {
                let observed = observed.clone();
                Box::pin(async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                })
            }))
            .await;
        service.base.set_running(true).await;
        service
            .process_incoming_message(Message {
                kind: "DEPLOY".to_owned(),
                ..Message::default()
            })
            .await
            .unwrap();
        assert_eq!(handled.load(Ordering::SeqCst), 1);
        let metrics = service.metrics().await;
        assert_eq!(metrics["connections"], 0);
        assert!(metrics.contains_key("last_message"));

        {
            let mut runtime = service.runtime.lock().await;
            runtime.connections = 1;
            runtime.last_message = None;
        }
        assert_eq!(service.custom_checks().await[0].status, Status::Up);
        service.runtime.lock().await.last_message = Some((
            Utc::now(),
            Instant::now()
                .checked_sub(Duration::from_secs(31 * 60))
                .unwrap(),
        ));
        assert_eq!(service.custom_checks().await[0].status, Status::Degraded);

        service.configure(default).await.unwrap();
        let mut invalid = RuntimeConfig::default().inputs.websocket;
        invalid.path.clear();
        assert!(service.configure(invalid).await.is_err());
        service.base.set_running(false).await;
    }

    #[tokio::test]
    async fn websocket_service_start_stop_and_restart_are_idempotent() {
        let mut config = RuntimeConfig::default().inputs.websocket;
        config.server_url = "ws://127.0.0.1:1".to_owned();
        config.reconnect_interval = Duration::from_millis(1);
        let service = WebSocketService::new(config, "test");
        service.start().await.unwrap();
        service.start().await.unwrap();
        assert!(service.is_running().await);
        service.restart().await.unwrap();
        service.stop().await.unwrap();
        service.stop().await.unwrap();
        assert!(!service.is_running().await);
    }

    #[tokio::test]
    async fn badge_service_saves_and_reads_badges_concurrently() {
        let service = Arc::new(BadgeDbService::new(
            RuntimeConfig::default().adapters.badgedb,
            "test",
        ));
        service.start().await.unwrap();
        let mut tasks = Vec::new();
        for index in 0..20 {
            let service = service.clone();
            tasks.push(tokio::spawn(async move {
                service
                    .save_badge(Badge {
                        id: format!("b{index}"),
                        name: "badge".to_owned(),
                        description: "test".to_owned(),
                        user_id: "u1".to_owned(),
                        awarded_at: Utc::now(),
                        kind: "test".to_owned(),
                        attributes: BTreeMap::new(),
                        revoked: false,
                        revoked_at: None,
                    })
                    .await
                    .unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(service.badges.read().await.len(), 20);
        service.stop().await.unwrap();
    }

    #[tokio::test]
    async fn badge_service_covers_validation_lookup_restart_metrics_and_health() {
        let mut config = RuntimeConfig::default().adapters.badgedb;
        let service = BadgeDbService::new(config.clone(), "test");
        assert_eq!(service.service_type(), "badgedb");
        assert_eq!(service.workflow(), DEFAULT_WORKFLOW_NAME);
        assert!(service.as_any().is::<BadgeDbService>());
        assert!(service.save_badge(badge("before-start")).await.is_err());
        assert!(service.badge("before-start").await.is_err());
        assert_eq!(service.custom_checks().await[0].status, Status::Down);
        assert_eq!(service.custom_checks().await[1].status, Status::Degraded);

        config.path = PathBuf::new();
        assert!(service.configure(config).await.is_err());
        let invalid_service = BadgeDbService::new(
            BadgeDbAdapterConfig {
                path: PathBuf::new(),
                ..RuntimeConfig::default().adapters.badgedb
            },
            "test",
        );
        assert!(invalid_service.start().await.is_err());
        assert!(!invalid_service.is_running().await);
        service
            .configure(RuntimeConfig::default().adapters.badgedb)
            .await
            .unwrap();

        assert_eq!(
            BadgeDbService::processing_method("badge_award").unwrap(),
            "StandardAward"
        );
        assert_eq!(
            BadgeDbService::processing_method("badge_revoke").unwrap(),
            "StandardRevoke"
        );
        assert_eq!(
            BadgeDbService::processing_method("badge_transfer").unwrap(),
            "SecureTransfer"
        );
        assert!(BadgeDbService::processing_method("unknown").is_err());

        service.start().await.unwrap();
        service.save_badge(badge("known")).await.unwrap();
        assert_eq!(service.badge("known").await.unwrap().id, "known");
        assert_eq!(
            service.badge("missing").await.unwrap_err().error_type,
            ErrorType::NotFound
        );
        let metrics = service.metrics().await;
        assert_eq!(metrics["db_entries"], 1);
        assert!(metrics.contains_key("last_backup"));
        let checks = service.custom_checks().await;
        assert_eq!(checks[0].status, Status::Up);
        assert_eq!(checks[1].status, Status::Up);

        *service.last_backup.write().await = Some((
            Utc::now(),
            Instant::now()
                .checked_sub(Duration::from_secs(25 * 60 * 60))
                .unwrap(),
        ));
        assert_eq!(service.custom_checks().await[1].status, Status::Degraded);
        service.restart().await.unwrap();
        service.stop().await.unwrap();
        service.stop().await.unwrap();
    }

    #[tokio::test]
    async fn disconnected_websocket_is_critical_even_with_stale_message() {
        let service = WebSocketService::new(RuntimeConfig::default().inputs.websocket, "test");
        service.base.set_running(true).await;
        service.runtime.lock().await.last_message = Some((
            Utc::now(),
            Instant::now()
                .checked_sub(Duration::from_secs(31 * 60))
                .unwrap(),
        ));
        let checks = service.custom_checks().await;
        assert_eq!(checks[0].status, Status::Down);
        assert_eq!(checks[0].level, Level::Critical);
    }

    #[tokio::test]
    async fn confluence_dependency_constructor_revalidates_on_start() {
        let mut config = RuntimeConfig::default().adapters.confluence;
        config.enabled = true;
        config.settings_page_id.clear();
        let service = ConfluenceSettingsService::with_dependencies(
            config,
            "test",
            reqwest::Client::new(),
            CacheDataAccessor::default(),
        );

        assert!(service.start().await.is_err());
        assert!(!service.is_running().await);
    }

    #[tokio::test]
    async fn confluence_validation_fetch_and_cache_cover_success_and_errors() {
        let mut default = RuntimeConfig::default().adapters.confluence;
        default.enabled = true;
        for config in [
            ConfluenceAdapterConfig {
                api_endpoint: "invalid".to_owned(),
                ..default.clone()
            },
            ConfluenceAdapterConfig {
                api_endpoint: "ftp://wiki.example.com".to_owned(),
                ..default.clone()
            },
            ConfluenceAdapterConfig {
                settings_page_id: String::new(),
                ..default.clone()
            },
            ConfluenceAdapterConfig {
                refresh_interval: Duration::ZERO,
                ..default.clone()
            },
        ] {
            assert!(validate_confluence_config(&config).is_err());
            assert!(ConfluenceSettingsService::new(config, "test").is_err());
        }

        let rules = json!({
            "rules": [
                {
                    "event_type": "DEPLOY",
                    "pattern": ".*",
                    "channel_id": "alerts",
                    "message_template": "{{.Content}}",
                    "enabled": true
                },
                {
                    "event_type": "DEPLOY",
                    "pattern": ".*",
                    "channel_id": "disabled",
                    "message_template": "{{.Content}}",
                    "enabled": false
                }
            ]
        });
        let page = json!({"body":{"storage":{"value":rules.to_string()}}}).to_string();
        let (base, task) = static_server(HttpStatus::OK, page, None).await;
        default.api_endpoint = base;
        let cache = CacheDataAccessor::default();
        let service =
            ConfluenceSettingsService::with_cache(default.clone(), "test", cache.clone()).unwrap();
        assert_eq!(service.service_type(), "confluence-settings");
        assert!(service.as_any().is::<ConfluenceSettingsService>());
        assert!(service.settings().await.is_empty());
        assert_eq!(service.custom_checks().await[0].status, Status::Degraded);
        service.refresh().await.unwrap();
        assert_eq!(service.settings().await.len(), 2);
        assert_eq!(service.match_event("DEPLOY").await.len(), 1);
        assert!(cache.get("confluence_settings").await.is_some());
        assert_eq!(service.custom_checks().await[0].status, Status::Up);
        assert_eq!(service.metrics().await["settings_count"], 2);

        *service.last_refresh.write().await = Some((
            Utc::now(),
            Instant::now()
                .checked_sub(default.refresh_interval * 3)
                .unwrap(),
        ));
        assert_eq!(service.custom_checks().await[0].status, Status::Degraded);
        service.configure(default).await.unwrap();
        let mut invalid = RuntimeConfig::default().adapters.confluence;
        invalid.settings_page_id.clear();
        assert!(service.configure(invalid).await.is_err());
        task.abort();

        for (status, body, expected_type) in [
            (
                HttpStatus::SERVICE_UNAVAILABLE,
                "unavailable".to_owned(),
                ErrorType::ServiceUnavailable,
            ),
            (
                HttpStatus::OK,
                "not-json".to_owned(),
                ErrorType::InvalidInput,
            ),
            (
                HttpStatus::OK,
                json!({"body":{"storage":{"value":"not-json"}}}).to_string(),
                ErrorType::InvalidInput,
            ),
        ] {
            let (base, task) = static_server(status, body, None).await;
            let mut config = RuntimeConfig::default().adapters.confluence;
            config.api_endpoint = base;
            let error = fetch_confluence_settings(&reqwest::Client::new(), &config)
                .await
                .unwrap_err();
            assert_eq!(error.error_type, expected_type);
            task.abort();
        }

        let mut config = RuntimeConfig::default().adapters.confluence;
        config.api_endpoint = unused_http_url().await;
        assert_eq!(
            fetch_confluence_settings(&reqwest::Client::new(), &config)
                .await
                .unwrap_err()
                .error_type,
            ErrorType::ServiceUnavailable
        );
    }

    #[tokio::test]
    async fn confluence_lifecycle_tolerates_initial_refresh_failure() {
        let mut config = RuntimeConfig::default().adapters.confluence;
        config.api_endpoint = unused_http_url().await;
        config.refresh_interval = Duration::from_millis(1);
        let service = ConfluenceSettingsService::new(config, "test").unwrap();
        service.start().await.unwrap();
        service.start().await.unwrap();
        sleep(Duration::from_millis(5)).await;
        assert!(service.is_running().await);
        service.stop().await.unwrap();
        service.stop().await.unwrap();
    }

    #[tokio::test]
    async fn adapter_error_response_previews_are_bounded() {
        let body = format!("preview-start-{}-preview-tail", "x".repeat(2048));

        let (base, task) = static_server(HttpStatus::SERVICE_UNAVAILABLE, body.clone(), None).await;
        let mut confluence = RuntimeConfig::default().adapters.confluence;
        confluence.api_endpoint = base;
        let error = fetch_confluence_settings(&reqwest::Client::new(), &confluence)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("preview-start"));
        assert!(!error.contains("preview-tail"));
        assert!(error.len() < 1200);
        task.abort();

        let (base, task) = static_server(HttpStatus::UNAUTHORIZED, body.clone(), None).await;
        let mut mattermost = RuntimeConfig::default().adapters.mattermost;
        mattermost.server_url = base;
        mattermost.api_token.clear();
        mattermost.username = "user".to_owned();
        mattermost.password = "password".to_owned();
        let error = login_mattermost(&reqwest::Client::new(), &mattermost)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("preview-start"));
        assert!(!error.contains("preview-tail"));
        assert!(error.len() < 1200);
        task.abort();

        let (base, task) = static_server(HttpStatus::INTERNAL_SERVER_ERROR, body, None).await;
        mattermost.server_url = base;
        let service = MattermostService::new(mattermost, "test").unwrap();
        service.base.set_running(true).await;
        let error = service
            .send_message_to_channel("channel", "message")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("preview-start"));
        assert!(!error.contains("preview-tail"));
        assert!(error.len() < 1200);
        task.abort();
    }

    #[tokio::test]
    async fn mattermost_validation_login_post_lifecycle_metrics_and_health() {
        let mut default = RuntimeConfig::default().adapters.mattermost;
        default.websocket_url.clear();
        for config in [
            MattermostAdapterConfig {
                server_url: "invalid".to_owned(),
                ..default.clone()
            },
            MattermostAdapterConfig {
                server_url: "ws://chat.example.com".to_owned(),
                ..default.clone()
            },
            MattermostAdapterConfig {
                channel: String::new(),
                ..default.clone()
            },
            MattermostAdapterConfig {
                websocket_url: "invalid".to_owned(),
                ..default.clone()
            },
            MattermostAdapterConfig {
                websocket_url: "https://chat.example.com".to_owned(),
                ..default.clone()
            },
            MattermostAdapterConfig {
                api_token: String::new(),
                ..default.clone()
            },
            MattermostAdapterConfig {
                api_token: String::new(),
                username: "user".to_owned(),
                ..default.clone()
            },
        ] {
            assert!(validate_mattermost_config(&config).is_err());
            assert!(MattermostService::new(config, "test").is_err());
        }
        assert_eq!(
            mattermost_websocket_endpoint("ws://chat.example.com"),
            "ws://chat.example.com/api/v4/websocket"
        );
        assert_eq!(
            mattermost_websocket_endpoint("ws://chat.example.com/api/v4/websocket/"),
            "ws://chat.example.com/api/v4/websocket"
        );

        let (base, task) = static_server(
            HttpStatus::OK,
            r#"{"id":"user"}"#.to_owned(),
            Some("login-token"),
        )
        .await;
        default.server_url = base;
        default.api_token.clear();
        default.username = "user".to_owned();
        default.password = "password".to_owned();
        assert_eq!(
            login_mattermost(&reqwest::Client::new(), &default)
                .await
                .unwrap(),
            "login-token"
        );
        let service = MattermostService::new(default.clone(), "test").unwrap();
        assert_eq!(service.service_type(), "mattermost");
        assert!(service.as_any().is::<MattermostService>());
        assert!(service.send_message("before").await.is_err());
        service.start().await.unwrap();
        service.start().await.unwrap();
        service.send_message("default channel").await.unwrap();
        assert!(
            service
                .send_message_to_channel("", "missing channel")
                .await
                .is_err()
        );
        let metrics = service.metrics().await;
        assert_eq!(metrics["api_token_configured"], true);
        assert_eq!(service.custom_checks().await[0].status, Status::Up);
        service.configure(default.clone()).await.unwrap();
        let mut invalid = default;
        invalid.channel.clear();
        assert!(service.configure(invalid).await.is_err());
        service.restart().await.unwrap();
        service.stop().await.unwrap();
        service.stop().await.unwrap();
        task.abort();

        for (status, token, expected) in [
            (HttpStatus::UNAUTHORIZED, None, ErrorType::Unauthorized),
            (HttpStatus::OK, None, ErrorType::Unauthorized),
        ] {
            let (base, task) = static_server(status, "login failure".to_owned(), token).await;
            let mut config = RuntimeConfig::default().adapters.mattermost;
            config.server_url = base;
            config.api_token.clear();
            config.username = "user".to_owned();
            config.password = "password".to_owned();
            let error = login_mattermost(&reqwest::Client::new(), &config)
                .await
                .unwrap_err();
            assert_eq!(error.error_type, expected);
            let service = MattermostService::new(config, "test").unwrap();
            assert_eq!(service.start().await.unwrap_err().error_type, expected);
            task.abort();
        }

        let mut config = RuntimeConfig::default().adapters.mattermost;
        config.server_url = unused_http_url().await;
        assert_eq!(
            login_mattermost(&reqwest::Client::new(), &config)
                .await
                .unwrap_err()
                .error_type,
            ErrorType::ServiceUnavailable
        );

        let (base, task) = static_server(
            HttpStatus::INTERNAL_SERVER_ERROR,
            "post failure".to_owned(),
            None,
        )
        .await;
        config.server_url = base;
        config.websocket_url = "ws://chat.example.com".to_owned();
        let service = MattermostService::new(config, "test").unwrap();
        service.base.set_running(true).await;
        assert_eq!(
            service
                .send_message_to_channel("channel", "message")
                .await
                .unwrap_err()
                .error_type,
            ErrorType::ServiceUnavailable
        );
        assert_eq!(service.custom_checks().await[0].status, Status::Degraded);
        service.runtime.lock().await.websocket_connected = true;
        assert_eq!(service.custom_checks().await[0].status, Status::Up);
        task.abort();
    }

    #[tokio::test]
    async fn mattermost_websocket_loop_handles_empty_and_invalid_inputs() {
        let runtime = Arc::new(Mutex::new(MattermostRuntime {
            websocket_connected: false,
            shutdown: None,
            task: None,
            auth_token: String::new(),
        }));
        let (_shutdown, receiver) = oneshot::channel();
        mattermost_websocket_loop(String::new(), String::new(), runtime.clone(), receiver).await;
        assert!(!runtime.lock().await.websocket_connected);

        let (_shutdown, receiver) = oneshot::channel();
        mattermost_websocket_loop(
            "not a websocket URL".to_owned(),
            String::new(),
            runtime.clone(),
            receiver,
        )
        .await;
        assert!(!runtime.lock().await.websocket_connected);

        let (_shutdown, receiver) = oneshot::channel();
        mattermost_websocket_loop(
            "ws://localhost".to_owned(),
            "invalid\nheader".to_owned(),
            runtime.clone(),
            receiver,
        )
        .await;
        assert!(!runtime.lock().await.websocket_connected);
    }

    #[tokio::test]
    async fn cache_data_accessor_returns_cloned_values() {
        let cache = CacheDataAccessor::default();
        assert!(cache.get("missing").await.is_none());
        cache.set("key", json!({"value": 1})).await;
        assert_eq!(cache.get("key").await, Some(json!({"value": 1})));
    }
}
