//! Health models, service checks, caching, and HTTP endpoints.

use std::{
    collections::BTreeMap,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use reqwest::{Client, Method, StatusCode as HttpStatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sysinfo::{ProcessesToUpdate, System, get_current_pid};
use tokio::{
    net::TcpStream,
    sync::{Mutex, RwLock},
    task::JoinSet,
    time::{Instant as TokioInstant, timeout, timeout_at},
};

use crate::errors::Result;

const DEFAULT_CHECK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    #[serde(rename = "UP")]
    Up,
    #[serde(rename = "DOWN")]
    Down,
    #[serde(rename = "DEGRADED")]
    Degraded,
    #[serde(rename = "UNKNOWN")]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Level {
    #[serde(rename = "CRITICAL")]
    Critical,
    #[serde(rename = "WARNING")]
    Warning,
    #[serde(rename = "INFO")]
    Info,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Category {
    #[serde(rename = "CONNECTIVITY")]
    Connectivity,
    #[serde(rename = "RESOURCES")]
    Resources,
    #[serde(rename = "DEPENDENCY")]
    Dependency,
    #[serde(rename = "PERFORMANCE")]
    Performance,
    #[serde(rename = "DATA")]
    Data,
    #[serde(rename = "SECURITY")]
    Security,
}

#[must_use]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub name: String,
    pub status: Status,
    pub description: String,
    pub category: Category,
    pub level: Level,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub details: BTreeMap<String, Value>,
    pub timestamp: DateTime<Utc>,
    /// Check duration in nanoseconds for compatibility with Go's `time.Duration`.
    pub duration: u64,
}

impl CheckResult {
    pub fn new(name: impl Into<String>, category: Category) -> Self {
        Self {
            name: name.into(),
            status: Status::Unknown,
            description: String::new(),
            category,
            level: Level::Info,
            details: BTreeMap::new(),
            timestamp: Utc::now(),
            duration: 0,
        }
    }

    pub fn with_status(mut self, status: Status, description: impl Into<String>) -> Self {
        self.status = status;
        self.description = description.into();
        self
    }

    pub fn with_level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    pub fn complete(mut self) -> Self {
        self.duration = nanos(
            Utc::now()
                .signed_duration_since(self.timestamp)
                .to_std()
                .unwrap_or(Duration::ZERO),
        );
        self
    }

    pub fn is_healthy(&self) -> bool {
        self.status == Status::Up
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub service_name: String,
    pub status: Status,
    pub check_results: Vec<CheckResult>,
    pub start_time: DateTime<Utc>,
    pub uptime: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
    pub refreshed_at: DateTime<Utc>,
    pub refresh_elapsed: u64,
}

impl Report {
    pub fn new(
        service_name: impl Into<String>,
        start_time: DateTime<Utc>,
        version: impl Into<String>,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            status: Status::Unknown,
            check_results: Vec::new(),
            start_time,
            uptime: 0,
            version: version.into(),
            metadata: BTreeMap::new(),
            refreshed_at: Utc::now(),
            refresh_elapsed: 0,
        }
    }

    pub fn add_result(&mut self, result: CheckResult) {
        self.check_results.push(result);
        self.status = determine_status(&self.check_results);
    }

    pub fn is_healthy(&self) -> bool {
        self.status == Status::Up
    }
}

pub fn determine_status(results: &[CheckResult]) -> Status {
    if results.is_empty() {
        return Status::Unknown;
    }
    if results
        .iter()
        .any(|result| result.level == Level::Critical && result.status == Status::Down)
    {
        return Status::Down;
    }
    if results
        .iter()
        .any(|result| matches!(result.status, Status::Down | Status::Degraded))
    {
        return Status::Degraded;
    }
    if results
        .iter()
        .any(|result| result.status == Status::Unknown)
    {
        return Status::Unknown;
    }
    Status::Up
}

#[async_trait]
pub trait Checker: Send + Sync {
    async fn check(&self) -> CheckResult;
}

/// Supplies a complete dependency or subsystem health report.
#[async_trait]
pub trait Reporter: Send + Sync {
    async fn report_health(&self) -> Report;
}

#[async_trait]
pub trait ServiceHealth: Send + Sync {
    fn name(&self) -> &str;
    async fn is_running(&self) -> bool;
    async fn metrics(&self) -> BTreeMap<String, Value>;
    async fn custom_checks(&self) -> Vec<CheckResult> {
        Vec::new()
    }
}

/// Checks this process's resident memory against host memory.
#[derive(Debug, Clone)]
pub struct MemoryUsageChecker {
    pub warning_percent: f64,
    pub critical_percent: f64,
}

impl Default for MemoryUsageChecker {
    fn default() -> Self {
        Self {
            warning_percent: 70.0,
            critical_percent: 85.0,
        }
    }
}

#[async_trait]
impl Checker for MemoryUsageChecker {
    async fn check(&self) -> CheckResult {
        let warning = self.warning_percent;
        let critical = self.critical_percent;
        let measured = tokio::task::spawn_blocking(read_process_memory).await;
        let mut result =
            CheckResult::new("memory-usage", Category::Resources).with_level(Level::Warning);
        match measured {
            Ok(Some((used, total))) if total > 0 => {
                let hundredths = u128::from(used)
                    .saturating_mul(10_000)
                    .checked_div(u128::from(total))
                    .unwrap_or_default();
                let bounded_hundredths =
                    u32::try_from(hundredths.min(u128::from(u32::MAX))).unwrap_or(u32::MAX);
                let percent = f64::from(bounded_hundredths) / 100.0;
                result = result
                    .with_detail("total_memory_bytes", total)
                    .with_detail("used_memory_bytes", used)
                    .with_detail("usage_percent", percent);
                if percent >= critical {
                    result = result.with_status(
                        Status::Degraded,
                        format!("Memory usage critical: {percent:.2}%"),
                    );
                } else if percent >= warning {
                    result = result.with_status(
                        Status::Degraded,
                        format!("Memory usage high: {percent:.2}%"),
                    );
                } else {
                    result = result
                        .with_status(Status::Up, format!("Memory usage normal: {percent:.2}%"));
                }
            }
            Ok(_) => {
                result =
                    result.with_status(Status::Degraded, "Process memory could not be measured");
            }
            Err(error) => {
                result = result.with_status(
                    Status::Degraded,
                    format!("Memory check task failed: {error}"),
                );
            }
        }
        result.complete()
    }
}

fn read_process_memory() -> Option<(u64, u64)> {
    let pid = get_current_pid().ok()?;
    let mut system = System::new();
    system.refresh_memory();
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    let used = system.process(pid)?.memory();
    Some((used, system.total_memory()))
}

/// Reports the number of Tokio runtime worker threads.
#[derive(Debug, Clone)]
pub struct RuntimeWorkerChecker {
    pub warning: usize,
    pub critical: usize,
}

impl Default for RuntimeWorkerChecker {
    fn default() -> Self {
        Self {
            warning: 64,
            critical: 256,
        }
    }
}

#[async_trait]
impl Checker for RuntimeWorkerChecker {
    async fn check(&self) -> CheckResult {
        let workers = tokio::runtime::Handle::try_current()
            .map_or(0, |runtime| runtime.metrics().num_workers());
        let mut result = CheckResult::new("runtime-worker-count", Category::Resources)
            .with_level(Level::Warning)
            .with_detail("count", workers);
        if workers == 0 {
            result = result.with_status(Status::Unknown, "Tokio runtime is unavailable");
        } else if workers >= self.critical {
            result = result.with_status(
                Status::Degraded,
                format!("Runtime worker count critical: {workers}"),
            );
        } else if workers >= self.warning {
            result = result.with_status(
                Status::Degraded,
                format!("Runtime worker count high: {workers}"),
            );
        } else {
            result = result.with_status(
                Status::Up,
                format!("Runtime worker count normal: {workers}"),
            );
        }
        result.complete()
    }
}

/// Checks TCP connectivity and latency to one host and port.
#[derive(Debug, Clone)]
pub struct ConnectivityChecker {
    host: String,
    port: u16,
    timeout: Duration,
    expected_latency: Duration,
}

impl ConnectivityChecker {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            timeout: Duration::from_secs(1),
            expected_latency: Duration::from_millis(500),
        }
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_expected_latency(mut self, expected_latency: Duration) -> Self {
        self.expected_latency = expected_latency;
        self
    }
}

#[async_trait]
impl Checker for ConnectivityChecker {
    async fn check(&self) -> CheckResult {
        let address = format!("{}:{}", self.host, self.port);
        let started = Instant::now();
        let connection = timeout(self.timeout, TcpStream::connect(&address)).await;
        let elapsed = started.elapsed();
        let mut result =
            CheckResult::new(format!("connectivity-{address}"), Category::Connectivity)
                .with_level(Level::Critical)
                .with_detail("latency_ms", millis(elapsed));
        result = match connection {
            Ok(Ok(_)) if elapsed > self.expected_latency => result.with_status(
                Status::Degraded,
                format!(
                    "High latency connecting to {address}: {}ms",
                    elapsed.as_millis()
                ),
            ),
            Ok(Ok(_)) => result.with_status(
                Status::Up,
                format!(
                    "Successfully connected to {address} in {}ms",
                    elapsed.as_millis()
                ),
            ),
            Ok(Err(error)) => result.with_status(
                Status::Down,
                format!("Failed to connect to {address}: {error}"),
            ),
            Err(_) => {
                result.with_status(Status::Down, format!("Connection to {address} timed out"))
            }
        };
        result.complete()
    }
}

/// Checks one HTTP endpoint's status and latency.
#[derive(Clone)]
pub struct HttpEndpointChecker {
    url: String,
    method: Method,
    timeout: Duration,
    expected_status: HttpStatusCode,
    expected_latency: Duration,
    client: Client,
}

impl HttpEndpointChecker {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: Method::GET,
            timeout: Duration::from_secs(2),
            expected_status: HttpStatusCode::OK,
            expected_latency: Duration::from_secs(1),
            client: Client::new(),
        }
    }

    #[must_use]
    pub fn with_method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_expected_status(mut self, status: HttpStatusCode) -> Self {
        self.expected_status = status;
        self
    }

    #[must_use]
    pub fn with_expected_latency(mut self, latency: Duration) -> Self {
        self.expected_latency = latency;
        self
    }

    #[must_use]
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }
}

#[async_trait]
impl Checker for HttpEndpointChecker {
    async fn check(&self) -> CheckResult {
        let started = Instant::now();
        let response = self
            .client
            .request(self.method.clone(), &self.url)
            .timeout(self.timeout)
            .send()
            .await;
        let elapsed = started.elapsed();
        let mut result = CheckResult::new(format!("http-{}", self.url), Category::Connectivity)
            .with_level(Level::Warning)
            .with_detail("latency_ms", millis(elapsed));
        result = match response {
            Err(error) => result.with_status(
                Status::Down,
                format!("Failed to connect to {}: {error}", self.url),
            ),
            Ok(response) => {
                let status = response.status();
                let result = result.with_detail("status_code", status.as_u16());
                if status != self.expected_status {
                    result.with_status(
                        Status::Degraded,
                        format!(
                            "Unexpected status code from {}: {} (expected {})",
                            self.url, status, self.expected_status
                        ),
                    )
                } else if elapsed > self.expected_latency {
                    result.with_status(
                        Status::Degraded,
                        format!("High latency from {}: {}ms", self.url, elapsed.as_millis()),
                    )
                } else {
                    result.with_status(
                        Status::Up,
                        format!(
                            "Successfully connected to {} in {}ms",
                            self.url,
                            elapsed.as_millis()
                        ),
                    )
                }
            }
        };
        result.complete()
    }
}

/// Maps another subsystem's health report into one dependency check.
pub struct DependencyChecker {
    service_name: String,
    reporter: Arc<dyn Reporter>,
    level: Level,
}

impl DependencyChecker {
    pub fn new(service_name: impl Into<String>, reporter: Arc<dyn Reporter>, level: Level) -> Self {
        Self {
            service_name: service_name.into(),
            reporter,
            level,
        }
    }
}

#[async_trait]
impl Checker for DependencyChecker {
    async fn check(&self) -> CheckResult {
        let report = self.reporter.report_health().await;
        let mut result = CheckResult::new(
            format!("dependency-{}", self.service_name),
            Category::Dependency,
        )
        .with_level(self.level)
        .with_detail(
            "dependency_status",
            format!("{:?}", report.status).to_uppercase(),
        );
        let description = match report.status {
            Status::Up => format!("Dependency {} is healthy", self.service_name),
            Status::Degraded => format!("Dependency {} is degraded", self.service_name),
            Status::Down => format!("Dependency {} is down", self.service_name),
            Status::Unknown => format!("Dependency {} status is unknown", self.service_name),
        };
        result = result.with_status(report.status, description);
        result.complete()
    }
}

type DataAccessFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type DataAccessFn = Arc<dyn Fn() -> DataAccessFuture + Send + Sync>;

/// Checks an asynchronous data-access operation with a fixed timeout.
pub struct DataAccessChecker {
    name: String,
    description: String,
    timeout: Duration,
    access: DataAccessFn,
}

impl DataAccessChecker {
    pub fn new<F, Fut>(name: impl Into<String>, description: impl Into<String>, access: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            name: name.into(),
            description: description.into(),
            timeout: Duration::from_secs(1),
            access: Arc::new(move || Box::pin(access())),
        }
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait]
impl Checker for DataAccessChecker {
    async fn check(&self) -> CheckResult {
        let started = Instant::now();
        let outcome = timeout(self.timeout, (self.access)()).await;
        let elapsed = started.elapsed();
        let mut result = CheckResult::new(format!("data-access-{}", self.name), Category::Data)
            .with_level(Level::Critical)
            .with_detail("description", self.description.clone())
            .with_detail("latency_ms", millis(elapsed));
        result = match outcome {
            Ok(Ok(())) => result.with_status(
                Status::Up,
                format!(
                    "Data access to {} successful in {}ms",
                    self.name,
                    elapsed.as_millis()
                ),
            ),
            Ok(Err(error)) => result.with_status(
                Status::Down,
                format!("Data access to {} failed: {error}", self.name),
            ),
            Err(_) => result.with_status(
                Status::Degraded,
                format!(
                    "Data access to {} timed out after {}ms",
                    self.name,
                    self.timeout.as_millis()
                ),
            ),
        };
        result.complete()
    }
}

/// Runs checks concurrently, contains panics, and applies a batch timeout.
pub async fn run_checks_parallel(
    checks: Vec<Arc<dyn Checker>>,
    check_timeout: Duration,
) -> Vec<CheckResult> {
    if checks.is_empty() {
        return Vec::new();
    }
    let count = checks.len();
    let mut tasks = JoinSet::new();
    for (index, checker) in checks.into_iter().enumerate() {
        tasks.spawn(async move {
            let outcome = AssertUnwindSafe(checker.check()).catch_unwind().await;
            let result = outcome.unwrap_or_else(|_| {
                CheckResult::new("health-task", Category::Resources)
                    .with_level(Level::Critical)
                    .with_status(Status::Unknown, "Health task panicked")
                    .complete()
            });
            (index, result)
        });
    }
    let deadline = TokioInstant::now() + check_timeout;
    let mut results: Vec<Option<CheckResult>> = (0..count).map(|_| None).collect();
    let mut control_results = Vec::new();
    loop {
        match timeout_at(deadline, tasks.join_next()).await {
            Ok(Some(Ok((index, result)))) => results[index] = Some(result),
            Ok(Some(Err(error))) => control_results.push(
                CheckResult::new("health-task", Category::Resources)
                    .with_level(Level::Critical)
                    .with_status(Status::Unknown, format!("Health task failed: {error}"))
                    .complete(),
            ),
            Ok(None) => break,
            Err(_) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                control_results.push(
                    CheckResult::new("health-timeout", Category::Resources)
                        .with_level(Level::Critical)
                        .with_status(Status::Unknown, "Health check timed out")
                        .complete(),
                );
                break;
            }
        }
    }
    let mut ordered: Vec<_> = results.into_iter().flatten().collect();
    ordered.extend(control_results);
    ordered
}

struct CachedReport {
    generated: Instant,
    report: Report,
}

/// Concurrent health registry with readiness and shutdown semantics.
pub struct HealthManager {
    refresh_interval: Duration,
    version: String,
    start_time: DateTime<Utc>,
    started_at: Instant,
    checkers: RwLock<Vec<Arc<dyn Checker>>>,
    reporters: RwLock<Vec<Arc<dyn Reporter>>>,
    services: RwLock<BTreeMap<String, Arc<dyn ServiceHealth>>>,
    cache: RwLock<Option<CachedReport>>,
    refresh_lock: Mutex<()>,
    registration_generation: AtomicU64,
    shutting_down: AtomicBool,
}

impl HealthManager {
    pub fn new(refresh_interval: Duration, version: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            refresh_interval,
            version: version.into(),
            start_time: Utc::now(),
            started_at: Instant::now(),
            checkers: RwLock::new(Vec::new()),
            reporters: RwLock::new(Vec::new()),
            services: RwLock::new(BTreeMap::new()),
            cache: RwLock::new(None),
            refresh_lock: Mutex::new(()),
            registration_generation: AtomicU64::new(0),
            shutting_down: AtomicBool::new(false),
        })
    }

    pub async fn register_checker(&self, checker: Arc<dyn Checker>) {
        self.checkers.write().await.push(checker);
        self.invalidate_registration_cache().await;
    }

    pub async fn register_reporter(&self, reporter: Arc<dyn Reporter>) {
        self.reporters.write().await.push(reporter);
        self.invalidate_registration_cache().await;
    }

    pub async fn register_service(&self, service: Arc<dyn ServiceHealth>) {
        self.services
            .write()
            .await
            .insert(service.name().to_owned(), service);
        self.invalidate_registration_cache().await;
    }

    pub fn set_shutting_down(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    pub async fn report(&self) -> Report {
        if let Some(cached) = self.cached_report().await {
            return cached;
        }
        self.refresh().await
    }

    pub async fn refresh(&self) -> Report {
        self.refresh_with_timeout(DEFAULT_CHECK_TIMEOUT).await
    }

    async fn refresh_with_timeout(&self, check_timeout: Duration) -> Report {
        let _refresh = self.refresh_lock.lock().await;
        if let Some(cached) = self.cached_report().await {
            return cached;
        }
        let started = Instant::now();
        let registration_generation = self.registration_generation.load(Ordering::SeqCst);
        let mut checkers = vec![
            Arc::new(MemoryUsageChecker::default()) as Arc<dyn Checker>,
            Arc::new(RuntimeWorkerChecker::default()) as Arc<dyn Checker>,
        ];
        checkers.extend(self.checkers.read().await.clone());
        let reporters = self.reporters.read().await.clone();
        let services = self.services.read().await.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move { run_checks_parallel(checkers, check_timeout).await });
        for (index, reporter) in reporters.into_iter().enumerate() {
            tasks.spawn(async move {
                match AssertUnwindSafe(reporter.report_health())
                    .catch_unwind()
                    .await
                {
                    Ok(report) => reporter_results(index, report),
                    Err(_) => vec![
                        CheckResult::new(
                            format!("reporter.{index}.health-task"),
                            Category::Dependency,
                        )
                        .with_level(Level::Critical)
                        .with_status(Status::Unknown, "Health reporter panicked")
                        .complete(),
                    ],
                }
            });
        }
        for (name, service) in services {
            tasks.spawn(async move {
                let panic_name = format!("service.{name}.health-task");
                let outcome = AssertUnwindSafe(async {
                    let mut results = Vec::new();
                    let result = if service.is_running().await {
                        let mut result =
                            CheckResult::new(format!("service.{name}"), Category::Dependency)
                                .with_level(Level::Critical)
                                .with_status(Status::Up, "Service is running");
                        result.details = service.metrics().await;
                        result
                    } else {
                        CheckResult::new(format!("service.{name}"), Category::Dependency)
                            .with_level(Level::Critical)
                            .with_status(Status::Down, "Service is not running")
                    };
                    results.push(result.complete());
                    for mut custom in service.custom_checks().await {
                        custom.name = format!("service.{name}.{}", custom.name);
                        results.push(custom.complete());
                    }
                    results
                })
                .catch_unwind()
                .await;
                match outcome {
                    Ok(results) => results,
                    Err(_) => vec![
                        CheckResult::new(panic_name, Category::Dependency)
                            .with_level(Level::Critical)
                            .with_status(Status::Unknown, "Service health task panicked")
                            .complete(),
                    ],
                }
            });
        }
        let mut results = Vec::new();
        let deadline = TokioInstant::now() + check_timeout;
        loop {
            match timeout_at(deadline, tasks.join_next()).await {
                Ok(Some(Ok(mut checks))) => results.append(&mut checks),
                Ok(Some(Err(error))) => results.push(
                    CheckResult::new("health-task", Category::Resources)
                        .with_level(Level::Critical)
                        .with_status(Status::Unknown, format!("Health task failed: {error}"))
                        .complete(),
                ),
                Ok(None) => break,
                Err(_) => {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    results.push(
                        CheckResult::new("health-timeout", Category::Resources)
                            .with_level(Level::Critical)
                            .with_status(Status::Unknown, "Health check timed out")
                            .complete(),
                    );
                    break;
                }
            }
        }
        results.sort_by(|left, right| left.name.cmp(&right.name));
        let report = Report {
            service_name: "system".to_owned(),
            status: determine_status(&results),
            check_results: results,
            start_time: self.start_time,
            uptime: nanos(self.started_at.elapsed()),
            version: self.version.clone(),
            metadata: BTreeMap::new(),
            refreshed_at: Utc::now(),
            refresh_elapsed: nanos(started.elapsed()),
        };
        let mut cache = self.cache.write().await;
        if self.registration_generation.load(Ordering::SeqCst) == registration_generation {
            *cache = Some(CachedReport {
                generated: Instant::now(),
                report: report.clone(),
            });
        }
        report
    }

    pub async fn service(&self, name: &str) -> Option<Arc<dyn ServiceHealth>> {
        self.services.read().await.get(name).cloned()
    }

    async fn invalidate_registration_cache(&self) {
        let mut cache = self.cache.write().await;
        self.registration_generation.fetch_add(1, Ordering::SeqCst);
        *cache = None;
    }

    async fn cached_report(&self) -> Option<Report> {
        self.cache
            .read()
            .await
            .as_ref()
            .filter(|cached| cached.generated.elapsed() < self.refresh_interval)
            .map(|cached| cached.report.clone())
    }
}

fn reporter_results(index: usize, report: Report) -> Vec<CheckResult> {
    if !report.check_results.is_empty() {
        return report.check_results;
    }
    let name = if report.service_name.is_empty() {
        format!("reporter.{index}")
    } else {
        format!("reporter.{}", report.service_name)
    };
    vec![
        CheckResult::new(name, Category::Dependency)
            .with_level(Level::Critical)
            .with_status(
                report.status,
                format!("Subsystem health is {:?}", report.status),
            )
            .complete(),
    ]
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Debug, Deserialize)]
struct ServiceQuery {
    service: Option<String>,
}

/// Returns an Axum router exposing the platform health contract.
pub fn router(manager: Arc<HealthManager>) -> Router {
    Router::new()
        .route("/health", get(system_health))
        .route("/health/readiness", get(readiness))
        .route("/health/liveness", get(liveness))
        .route("/health/service", get(service_health))
        .with_state(manager)
}

async fn system_health(State(manager): State<Arc<HealthManager>>) -> Response {
    let report = manager.report().await;
    let status = if matches!(report.status, Status::Down | Status::Unknown) {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    (status, Json(report)).into_response()
}

async fn readiness(State(manager): State<Arc<HealthManager>>) -> Response {
    if manager.is_shutting_down() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "NOT_READY", "reason": "shutting_down"})),
        )
            .into_response();
    }
    let report = manager.report().await;
    if matches!(report.status, Status::Down | Status::Unknown) {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "NOT_READY"})),
        )
            .into_response()
    } else {
        (StatusCode::OK, Json(json!({"status": "READY"}))).into_response()
    }
}

async fn liveness() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "UP"})))
}

async fn service_health(
    State(manager): State<Arc<HealthManager>>,
    Query(query): Query<ServiceQuery>,
) -> Response {
    let Some(name) = query.service.filter(|name| !name.is_empty()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Missing required query parameter 'service'"})),
        )
            .into_response();
    };
    let Some(service) = manager.service(&name).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Service not found"})),
        )
            .into_response();
    };
    let inspected = timeout(
        DEFAULT_CHECK_TIMEOUT,
        AssertUnwindSafe(async {
            let running = service.is_running().await;
            let metrics = if running {
                service.metrics().await
            } else {
                BTreeMap::new()
            };
            (running, metrics)
        })
        .catch_unwind(),
    )
    .await;
    let (check, response_status) = match inspected {
        Ok(Ok((running, metrics))) => {
            let mut check = CheckResult::new(format!("service.{name}"), Category::Dependency)
                .with_level(Level::Critical)
                .with_status(
                    if running { Status::Up } else { Status::Down },
                    if running {
                        "Service is running"
                    } else {
                        "Service is not running"
                    },
                );
            check.details = metrics;
            (
                check.complete(),
                if running {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                },
            )
        }
        Ok(Err(_)) => (
            CheckResult::new(format!("service.{name}"), Category::Dependency)
                .with_level(Level::Critical)
                .with_status(Status::Unknown, "Service health task panicked")
                .complete(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        Err(_) => (
            CheckResult::new(format!("service.{name}"), Category::Dependency)
                .with_level(Level::Critical)
                .with_status(Status::Unknown, "Service health check timed out")
                .complete(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    };
    let report = Report {
        service_name: name,
        status: check.status,
        check_results: vec![check],
        start_time: manager.start_time,
        uptime: nanos(manager.started_at.elapsed()),
        version: manager.version.clone(),
        metadata: BTreeMap::new(),
        refreshed_at: Utc::now(),
        refresh_elapsed: 0,
    };
    (response_status, Json(report)).into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
        routing::get,
    };
    use tokio::{
        net::TcpListener,
        sync::{Notify, oneshot},
        time::sleep,
    };
    use tower::ServiceExt;

    use crate::errors::{AppError, ErrorType};

    use super::*;

    struct TestService {
        running: bool,
    }

    #[async_trait]
    impl ServiceHealth for TestService {
        fn name(&self) -> &'static str {
            "test"
        }

        async fn is_running(&self) -> bool {
            self.running
        }

        async fn metrics(&self) -> BTreeMap<String, Value> {
            BTreeMap::from([("requests".to_owned(), json!(3))])
        }
    }

    struct PanicHealthService;

    #[async_trait]
    impl ServiceHealth for PanicHealthService {
        fn name(&self) -> &'static str {
            "panic"
        }

        async fn is_running(&self) -> bool {
            panic!("health panic");
        }

        async fn metrics(&self) -> BTreeMap<String, Value> {
            BTreeMap::new()
        }
    }

    #[test]
    fn critical_down_wins_while_warning_down_degrades() {
        let warning = CheckResult::new("warning", Category::Dependency)
            .with_level(Level::Warning)
            .with_status(Status::Down, "optional dependency unavailable");
        assert_eq!(determine_status(&[warning]), Status::Degraded);
        let critical = CheckResult::new("critical", Category::Dependency)
            .with_level(Level::Critical)
            .with_status(Status::Down, "required dependency unavailable");
        assert_eq!(determine_status(&[critical]), Status::Down);
    }

    #[tokio::test]
    async fn readiness_fails_for_down_service_but_liveness_stays_up() {
        let manager = HealthManager::new(Duration::ZERO, "test");
        manager
            .register_service(Arc::new(TestService { running: false }))
            .await;
        let app = router(manager);
        let system = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(system.status(), StatusCode::SERVICE_UNAVAILABLE);
        let readiness = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health/readiness")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);
        let liveness = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health/liveness")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(liveness.status(), StatusCode::OK);

        let missing = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health/service")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
        let unknown = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health/service?service=missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
        let service = app
            .oneshot(
                Request::builder()
                    .uri("/health/service?service=test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(service.status(), StatusCode::SERVICE_UNAVAILABLE);

        let shutting_down = HealthManager::new(Duration::ZERO, "test");
        shutting_down.set_shutting_down();
        let response = router(shutting_down)
            .oneshot(
                Request::builder()
                    .uri("/health/readiness")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["reason"],
            "shutting_down"
        );

        let panic_manager = HealthManager::new(Duration::ZERO, "test");
        panic_manager
            .register_service(Arc::new(PanicHealthService))
            .await;
        let response = router(panic_manager)
            .oneshot(
                Request::builder()
                    .uri("/health/service?service=panic")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let report = serde_json::from_slice::<Report>(&body).unwrap();
        assert_eq!(report.status, Status::Unknown);
        assert_eq!(
            report.check_results[0].description,
            "Service health task panicked"
        );
    }

    #[tokio::test]
    async fn system_checks_keep_service_free_runtime_ready() {
        let manager = HealthManager::new(Duration::ZERO, "test");
        let report = manager.refresh().await;
        assert!(matches!(report.status, Status::Up | Status::Degraded));
        assert!(
            report
                .check_results
                .iter()
                .any(|result| result.name == "memory-usage")
        );
        assert!(
            report
                .check_results
                .iter()
                .any(|result| result.name == "runtime-worker-count")
        );

        let response = router(manager)
            .oneshot(
                Request::builder()
                    .uri("/health/readiness")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn connectivity_and_http_checkers_report_real_endpoints() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_port = tcp.local_addr().unwrap().port();
        let accept = tokio::spawn(async move {
            let _ = tcp.accept().await;
        });
        let connectivity = ConnectivityChecker::new("127.0.0.1", tcp_port)
            .check()
            .await;
        assert_eq!(connectivity.status, Status::Up);
        accept.await.unwrap();

        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = http.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(
                http,
                Router::new().route("/ready", get(|| async { StatusCode::CREATED })),
            )
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
        });
        let check = HttpEndpointChecker::new(format!("http://{address}/ready"))
            .with_expected_status(HttpStatusCode::CREATED)
            .check()
            .await;
        assert_eq!(check.status, Status::Up);
        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn data_access_checker_distinguishes_success_failure_and_timeout() {
        let calls = Arc::new(AtomicUsize::new(0));
        let success_calls = calls.clone();
        let success = DataAccessChecker::new("cache", "cache access", move || {
            let calls = success_calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .check()
        .await;
        assert_eq!(success.status, Status::Up);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let failure = DataAccessChecker::new("cache", "cache access", || async {
            Err(AppError::new(ErrorType::Internal, "read failed"))
        })
        .check()
        .await;
        assert_eq!(failure.status, Status::Down);

        let slow = DataAccessChecker::new("cache", "cache access", || async {
            sleep(Duration::from_millis(50)).await;
            Ok(())
        })
        .with_timeout(Duration::from_millis(1))
        .check()
        .await;
        assert_eq!(slow.status, Status::Degraded);
    }

    struct StaticReporter(Status);

    #[async_trait]
    impl Reporter for StaticReporter {
        async fn report_health(&self) -> Report {
            let mut report = Report::new("dependency", Utc::now(), "test");
            report.status = self.0;
            report
        }
    }

    struct ContributingReporter;

    #[async_trait]
    impl Reporter for ContributingReporter {
        async fn report_health(&self) -> Report {
            let mut report = Report::new("contributor", Utc::now(), "test");
            report.add_result(
                CheckResult::new("reported", Category::Dependency)
                    .with_status(Status::Up, "reported"),
            );
            report
        }
    }

    #[tokio::test]
    async fn dependency_checker_maps_report_status_and_manager_uses_reporters() {
        let checker = DependencyChecker::new(
            "search",
            Arc::new(StaticReporter(Status::Down)),
            Level::Warning,
        );
        let result = checker.check().await;
        assert_eq!(result.status, Status::Down);
        assert_eq!(result.level, Level::Warning);

        let manager = HealthManager::new(Duration::ZERO, "test");
        manager
            .register_reporter(Arc::new(ContributingReporter))
            .await;
        assert!(
            manager
                .refresh()
                .await
                .check_results
                .iter()
                .any(|result| result.name == "reported")
        );

        let manager = HealthManager::new(Duration::ZERO, "test");
        manager
            .register_reporter(Arc::new(StaticReporter(Status::Down)))
            .await;
        let report = manager.refresh().await;
        assert_eq!(report.status, Status::Down);
        assert!(
            report
                .check_results
                .iter()
                .any(|result| result.name == "reporter.dependency")
        );
    }

    struct PanicReporter;

    #[async_trait]
    impl Reporter for PanicReporter {
        async fn report_health(&self) -> Report {
            panic!("reporter panic");
        }
    }

    struct SlowHealthService;

    #[async_trait]
    impl ServiceHealth for SlowHealthService {
        fn name(&self) -> &'static str {
            "slow"
        }

        async fn is_running(&self) -> bool {
            sleep(Duration::from_secs(1)).await;
            true
        }

        async fn metrics(&self) -> BTreeMap<String, Value> {
            BTreeMap::new()
        }
    }

    #[tokio::test]
    async fn manager_contains_reporter_panics_and_bounds_service_health() {
        let panic_manager = HealthManager::new(Duration::ZERO, "test");
        panic_manager
            .register_reporter(Arc::new(PanicReporter))
            .await;
        let report = panic_manager
            .refresh_with_timeout(Duration::from_secs(1))
            .await;
        assert!(
            report
                .check_results
                .iter()
                .any(|result| result.name == "reporter.0.health-task")
        );

        let slow_manager = HealthManager::new(Duration::ZERO, "test");
        slow_manager
            .register_service(Arc::new(SlowHealthService))
            .await;
        let started = Instant::now();
        let report = slow_manager
            .refresh_with_timeout(Duration::from_millis(5))
            .await;
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(
            report
                .check_results
                .iter()
                .any(|result| result.name == "health-timeout")
        );
    }

    struct CountingChecker(Arc<AtomicUsize>);

    #[async_trait]
    impl Checker for CountingChecker {
        async fn check(&self) -> CheckResult {
            let call = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            CheckResult::new("counting", Category::Performance)
                .with_status(Status::Up, "counted")
                .with_detail("call", call)
                .complete()
        }
    }

    struct OnceBlockingChecker {
        entered: Arc<Notify>,
        release: Arc<Notify>,
        blocked: AtomicBool,
    }

    #[async_trait]
    impl Checker for OnceBlockingChecker {
        async fn check(&self) -> CheckResult {
            if !self.blocked.swap(true, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            CheckResult::new("blocking", Category::Performance)
                .with_status(Status::Up, "finished")
                .complete()
        }
    }

    #[tokio::test]
    async fn reports_are_cached_invalidated_and_cloned() {
        let calls = Arc::new(AtomicUsize::new(0));
        let manager = HealthManager::new(Duration::from_secs(60), "test");
        manager
            .register_checker(Arc::new(CountingChecker(calls.clone())))
            .await;

        let mut first = manager.report().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        first.service_name = "mutated".to_owned();
        first.check_results.clear();

        let second = manager.report().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(second.service_name, "system");
        assert!(!second.check_results.is_empty());

        manager
            .register_checker(Arc::new(CountingChecker(Arc::new(AtomicUsize::new(0)))))
            .await;
        let _ = manager.report().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn registration_invalidates_an_in_flight_refresh_snapshot() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let manager = HealthManager::new(Duration::from_secs(60), "test");
        manager
            .register_checker(Arc::new(OnceBlockingChecker {
                entered: entered.clone(),
                release: release.clone(),
                blocked: AtomicBool::new(false),
            }))
            .await;

        let refresh_manager = manager.clone();
        let refresh = tokio::spawn(async move { refresh_manager.report().await });
        entered.notified().await;

        let late_calls = Arc::new(AtomicUsize::new(0));
        let register_manager = manager.clone();
        let register_calls = late_calls.clone();
        let registration = tokio::spawn(async move {
            register_manager
                .register_checker(Arc::new(CountingChecker(register_calls)))
                .await;
        });
        timeout(Duration::from_secs(1), registration)
            .await
            .unwrap()
            .unwrap();

        release.notify_one();
        let first = refresh.await.unwrap();
        assert!(
            !first
                .check_results
                .iter()
                .any(|result| result.name == "counting")
        );

        let second = manager.report().await;
        assert_eq!(late_calls.load(Ordering::SeqCst), 1);
        assert!(
            second
                .check_results
                .iter()
                .any(|result| result.name == "counting")
        );
    }

    struct PanicChecker;

    #[async_trait]
    impl Checker for PanicChecker {
        async fn check(&self) -> CheckResult {
            panic!("checker panic");
        }
    }

    struct SlowChecker;

    #[async_trait]
    impl Checker for SlowChecker {
        async fn check(&self) -> CheckResult {
            sleep(Duration::from_secs(1)).await;
            CheckResult::new("slow", Category::Performance).with_status(Status::Up, "finished")
        }
    }

    struct DelayedChecker {
        name: &'static str,
        delay: Duration,
    }

    #[async_trait]
    impl Checker for DelayedChecker {
        async fn check(&self) -> CheckResult {
            sleep(self.delay).await;
            CheckResult::new(self.name, Category::Performance).with_status(Status::Up, "finished")
        }
    }

    #[tokio::test]
    async fn parallel_checks_contain_panics_and_enforce_timeout() {
        let panic_results =
            run_checks_parallel(vec![Arc::new(PanicChecker)], Duration::from_secs(1)).await;
        assert_eq!(panic_results[0].status, Status::Unknown);

        let timeout_results =
            run_checks_parallel(vec![Arc::new(SlowChecker)], Duration::from_millis(1)).await;
        assert_eq!(timeout_results[0].name, "health-timeout");
    }

    #[tokio::test]
    async fn parallel_checks_preserve_input_order() {
        let results = run_checks_parallel(
            vec![
                Arc::new(DelayedChecker {
                    name: "first",
                    delay: Duration::from_millis(10),
                }),
                Arc::new(DelayedChecker {
                    name: "second",
                    delay: Duration::ZERO,
                }),
            ],
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(
            results
                .iter()
                .map(|result| result.name.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );
    }
}
