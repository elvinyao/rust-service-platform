//! Service and workflow registration, dispatch, monitoring, and rollback.

use std::{
    collections::BTreeMap, fmt::Write as _, panic::AssertUnwindSafe, sync::Arc, time::Duration,
};

use async_trait::async_trait;
use futures_util::FutureExt as _;
use serde_json::Value;
use service_platform::{
    errors::{AppError, ErrorType, Result},
    health::{HealthManager, ServiceHealth},
    ruleengine::Message,
};
use tokio::{
    sync::RwLock,
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval_at, timeout},
};

use crate::service::Service;

pub struct ServiceManager {
    services: RwLock<BTreeMap<String, Arc<dyn Service>>>,
    health: Arc<HealthManager>,
    version: String,
}

impl ServiceManager {
    pub fn new(version: impl Into<String>, health: Arc<HealthManager>) -> Arc<Self> {
        Arc::new(Self {
            services: RwLock::new(BTreeMap::new()),
            health,
            version: version.into(),
        })
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub async fn register(&self, service: Arc<dyn Service>) -> Result<()> {
        let name = service.name();
        if name.is_empty() {
            return invalid("Cannot register service with empty name");
        }
        let mut services = self.services.write().await;
        if services.contains_key(name) {
            return invalid("Service is already registered");
        }
        services.insert(name.to_owned(), service.clone());
        drop(services);
        self.health
            .register_service(Arc::new(ManagedServiceHealth(service)))
            .await;
        Ok(())
    }

    pub async fn get(&self, name: &str) -> Option<Arc<dyn Service>> {
        self.services.read().await.get(name).cloned()
    }

    pub async fn list(&self) -> Vec<Arc<dyn Service>> {
        self.services.read().await.values().cloned().collect()
    }

    pub async fn by_workflow_and_type(
        &self,
        workflow: &str,
        service_type: &str,
    ) -> Vec<Arc<dyn Service>> {
        self.services
            .read()
            .await
            .values()
            .filter(|service| {
                service.workflow() == workflow && service.service_type() == service_type
            })
            .cloned()
            .collect()
    }

    /// Starts all services concurrently and rolls back services started by this call.
    pub async fn start_all(&self) -> Result<()> {
        let services = self.list().await;
        if services.is_empty() {
            return invalid("No services registered to start");
        }
        let mut tasks = JoinSet::new();
        for service in services {
            tasks.spawn(async move {
                let was_running = service.is_running().await;
                let result = AssertUnwindSafe(service.start()).catch_unwind().await;
                let started_by_call = !was_running && service.is_running().await;
                (service, result, started_by_call)
            });
        }
        let mut failed = Vec::new();
        let mut started = Vec::new();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((service, Ok(Ok(())), true)) => started.push(service),
                Ok((_service, Ok(Ok(())), false)) => {}
                Ok((service, Ok(Err(error)), started_by_call)) => {
                    failed.push(format!("{}: {error}", service.name()));
                    if started_by_call {
                        started.push(service);
                    }
                }
                Ok((service, Err(_), started_by_call)) => {
                    failed.push(format!("{}: service start panicked", service.name()));
                    if started_by_call {
                        started.push(service);
                    }
                }
                Err(error) => failed.push(format!("service start task: {error}")),
            }
        }
        if failed.is_empty() {
            return Ok(());
        }
        failed.sort();

        let mut rollback = JoinSet::new();
        for service in started {
            rollback.spawn(async move {
                let name = service.name().to_owned();
                let result = timeout(
                    Duration::from_secs(10),
                    AssertUnwindSafe(service.stop()).catch_unwind(),
                )
                .await;
                (name, result)
            });
        }
        let mut rollback_failures = Vec::new();
        while let Some(result) = rollback.join_next().await {
            match result {
                Ok((_, Ok(Ok(Ok(()))))) => {}
                Ok((name, Ok(Ok(Err(error))))) => {
                    rollback_failures.push(format!("{name}: {error}"));
                }
                Ok((name, Ok(Err(_)))) => {
                    rollback_failures.push(format!("{name}: rollback panicked"));
                }
                Ok((name, Err(_))) => {
                    rollback_failures.push(format!("{name}: rollback timed out"));
                }
                Err(error) => rollback_failures.push(format!("rollback task: {error}")),
            }
        }
        rollback_failures.sort();
        let mut message = format!("Failed to start some services: {}", failed.join(", "));
        if !rollback_failures.is_empty() {
            write!(
                &mut message,
                "; rollback failures: {}",
                rollback_failures.join(", ")
            )
            .expect("writing to String cannot fail");
        }
        Err(AppError::new(ErrorType::ServiceUnavailable, message))
    }

    pub async fn stop_all(&self) -> Result<()> {
        let mut tasks = JoinSet::new();
        for service in self.list().await {
            tasks.spawn(async move {
                let name = service.name().to_owned();
                let result = AssertUnwindSafe(service.stop()).catch_unwind().await;
                (name, result)
            });
        }
        let mut failures = Vec::new();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((_, Ok(Ok(())))) => {}
                Ok((name, Ok(Err(error)))) => failures.push(format!("{name}: {error}")),
                Ok((name, Err(_))) => failures.push(format!("{name}: service stop panicked")),
                Err(error) => failures.push(format!("service stop task: {error}")),
            }
        }
        failures.sort();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(AppError::new(
                ErrorType::ServiceUnavailable,
                format!("Failed to stop some services: {}", failures.join(", ")),
            ))
        }
    }

    /// Restarts services that are no longer running until the shutdown signal arrives.
    pub async fn monitor(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        self.monitor_with_interval(&mut shutdown, Duration::from_secs(30))
            .await;
    }

    async fn monitor_with_interval(
        self: &Arc<Self>,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
        monitor_interval: Duration,
    ) {
        let mut ticker = interval_at(Instant::now() + monitor_interval, monitor_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                _ = ticker.tick() => {
                    self.check_services_once().await;
                }
            }
        }
    }

    async fn check_services_once(&self) {
        for service in self.list().await {
            if service.is_running().await {
                continue;
            }
            if let Err(error) = service.restart().await {
                tracing::error!(
                    service = service.name(),
                    error = %error,
                    "Failed to restart service"
                );
            }
        }
    }
}

struct ManagedServiceHealth(Arc<dyn Service>);

#[async_trait]
impl ServiceHealth for ManagedServiceHealth {
    fn name(&self) -> &str {
        self.0.name()
    }

    async fn is_running(&self) -> bool {
        self.0.is_running().await
    }

    async fn metrics(&self) -> BTreeMap<String, Value> {
        self.0.metrics().await
    }

    async fn custom_checks(&self) -> Vec<service_platform::health::CheckResult> {
        self.0.custom_checks().await
    }
}

#[async_trait]
pub trait Workflow: Send + Sync {
    fn name(&self) -> &str;
    async fn process_message(&self, message: &Message) -> Result<()>;
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct WorkflowManager {
    workflows: RwLock<BTreeMap<String, Arc<dyn Workflow>>>,
}

impl WorkflowManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn register(&self, workflow: Arc<dyn Workflow>) -> Result<()> {
        let name = workflow.name();
        if name.is_empty() {
            return invalid("Cannot register workflow with empty name");
        }
        let mut workflows = self.workflows.write().await;
        if workflows.contains_key(name) {
            return invalid("Workflow is already registered");
        }
        workflows.insert(name.to_owned(), workflow);
        Ok(())
    }

    pub async fn get(&self, name: &str) -> Option<Arc<dyn Workflow>> {
        self.workflows.read().await.get(name).cloned()
    }

    pub async fn list(&self) -> Vec<String> {
        self.workflows.read().await.keys().cloned().collect()
    }

    /// Dispatches in stable name order and stops at the first successful workflow.
    pub async fn dispatch(&self, message: &Message) -> Result<()> {
        let workflows: Vec<_> = self.workflows.read().await.values().cloned().collect();
        if workflows.is_empty() {
            return Err(AppError::new(
                ErrorType::NotFound,
                "No workflows registered to process messages",
            ));
        }
        let mut failures = Vec::new();
        for workflow in workflows {
            match workflow.process_message(message).await {
                Ok(()) => return Ok(()),
                Err(error) => failures.push(format!("{}: {error}", workflow.name())),
            }
        }
        Err(AppError::new(
            ErrorType::ServiceUnavailable,
            format!(
                "All workflows failed to process message: {}",
                failures.join("; ")
            ),
        ))
    }

    pub async fn stop_all(&self) -> Result<()> {
        let workflows: Vec<_> = self
            .workflows
            .read()
            .await
            .iter()
            .rev()
            .map(|(_, workflow)| workflow.clone())
            .collect();
        let mut failures = Vec::new();
        for workflow in workflows {
            if let Err(error) = workflow.stop().await {
                failures.push(format!("stop workflow {}: {error}", workflow.name()));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(AppError::new(
                ErrorType::ServiceUnavailable,
                failures.join("; "),
            ))
        }
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(AppError::new(ErrorType::InvalidInput, message))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use service_platform::{health::CheckResult, ruleengine::DEFAULT_WORKFLOW_NAME};
    use tokio::{
        sync::{Mutex as TokioMutex, watch},
        time::sleep,
    };

    use super::*;

    struct TestService {
        name: String,
        running: AtomicBool,
        fail_start: bool,
        fail_stop: bool,
    }

    #[async_trait]
    impl ServiceHealth for TestService {
        fn name(&self) -> &str {
            &self.name
        }

        async fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }

        async fn metrics(&self) -> BTreeMap<String, Value> {
            BTreeMap::new()
        }

        async fn custom_checks(&self) -> Vec<CheckResult> {
            Vec::new()
        }
    }

    #[async_trait]
    impl Service for TestService {
        fn service_type(&self) -> &'static str {
            "test"
        }

        fn workflow(&self) -> &'static str {
            service_platform::ruleengine::DEFAULT_WORKFLOW_NAME
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        async fn start(&self) -> Result<()> {
            self.running.store(true, Ordering::SeqCst);
            if self.fail_start {
                return Err(AppError::new(ErrorType::Internal, "failed"));
            }
            Ok(())
        }

        async fn stop(&self) -> Result<()> {
            self.running.store(false, Ordering::SeqCst);
            if self.fail_stop {
                Err(AppError::new(ErrorType::Internal, "stop failed"))
            } else {
                Ok(())
            }
        }
    }

    struct PanickingStartService {
        running: AtomicBool,
    }

    #[async_trait]
    impl ServiceHealth for PanickingStartService {
        fn name(&self) -> &'static str {
            "panicking"
        }

        async fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }

        async fn metrics(&self) -> BTreeMap<String, Value> {
            BTreeMap::new()
        }
    }

    #[async_trait]
    impl Service for PanickingStartService {
        fn service_type(&self) -> &'static str {
            "test"
        }

        fn workflow(&self) -> &'static str {
            DEFAULT_WORKFLOW_NAME
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        async fn start(&self) -> Result<()> {
            self.running.store(true, Ordering::SeqCst);
            panic!("start panic");
        }

        async fn stop(&self) -> Result<()> {
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn service_start_failure_rolls_back_successes() {
        let health = HealthManager::new(Duration::ZERO, "test");
        let manager = ServiceManager::new("test", health);
        let first = Arc::new(TestService {
            name: "first".to_owned(),
            running: AtomicBool::new(false),
            fail_start: false,
            fail_stop: false,
        });
        let second = Arc::new(TestService {
            name: "second".to_owned(),
            running: AtomicBool::new(false),
            fail_start: true,
            fail_stop: false,
        });
        manager.register(first.clone()).await.unwrap();
        manager.register(second.clone()).await.unwrap();
        assert!(manager.start_all().await.is_err());
        assert!(!first.running.load(Ordering::SeqCst));
        assert!(!second.running.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn service_start_panic_is_named_and_rolled_back() {
        let manager = ServiceManager::new("test", HealthManager::new(Duration::ZERO, "test"));
        let service = Arc::new(PanickingStartService {
            running: AtomicBool::new(false),
        });
        manager.register(service.clone()).await.unwrap();

        let error = manager.start_all().await.unwrap_err().to_string();

        assert!(error.contains("panicking: service start panicked"));
        assert!(!service.running.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn service_manager_rejects_empty_and_duplicate_registration() {
        let manager =
            ServiceManager::new("test", HealthManager::new(Duration::from_secs(30), "test"));
        assert!(manager.start_all().await.is_err());

        let service = Arc::new(TestService {
            name: "service".to_owned(),
            running: AtomicBool::new(false),
            fail_start: false,
            fail_stop: false,
        });
        manager.register(service.clone()).await.unwrap();
        let duplicate: Arc<dyn Service> = service;
        assert!(manager.register(duplicate).await.is_err());
        assert_eq!(
            manager
                .by_workflow_and_type(DEFAULT_WORKFLOW_NAME, "test")
                .await
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn service_manager_covers_identity_success_stop_failure_and_rollback_failure() {
        let manager =
            ServiceManager::new("1.2.3", HealthManager::new(Duration::from_secs(30), "test"));
        assert_eq!(manager.version(), "1.2.3");
        assert!(manager.get("missing").await.is_none());
        assert!(manager.list().await.is_empty());

        let empty = Arc::new(TestService {
            name: String::new(),
            running: AtomicBool::new(false),
            fail_start: false,
            fail_stop: false,
        });
        assert!(manager.register(empty).await.is_err());

        let running = Arc::new(TestService {
            name: "running".to_owned(),
            running: AtomicBool::new(true),
            fail_start: false,
            fail_stop: false,
        });
        manager.register(running.clone()).await.unwrap();
        manager.start_all().await.unwrap();
        assert_eq!(manager.list().await.len(), 1);
        assert!(manager.get("running").await.is_some());
        assert!(
            manager
                .by_workflow_and_type("missing", "test")
                .await
                .is_empty()
        );
        manager.stop_all().await.unwrap();
        assert!(!running.running.load(Ordering::SeqCst));

        let manager =
            ServiceManager::new("test", HealthManager::new(Duration::from_secs(30), "test"));
        manager
            .register(Arc::new(TestService {
                name: "cannot-stop".to_owned(),
                running: AtomicBool::new(true),
                fail_start: false,
                fail_stop: true,
            }))
            .await
            .unwrap();
        let error = manager.stop_all().await.unwrap_err();
        assert!(error.to_string().contains("cannot-stop"));

        let manager =
            ServiceManager::new("test", HealthManager::new(Duration::from_secs(30), "test"));
        manager
            .register(Arc::new(TestService {
                name: "rollback-fails".to_owned(),
                running: AtomicBool::new(false),
                fail_start: true,
                fail_stop: true,
            }))
            .await
            .unwrap();
        let error = manager.start_all().await.unwrap_err();
        assert!(error.to_string().contains("rollback failures"));
    }

    struct RestartableService {
        running: AtomicBool,
        starts: AtomicUsize,
    }

    #[async_trait]
    impl ServiceHealth for RestartableService {
        fn name(&self) -> &'static str {
            "restartable"
        }

        async fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }

        async fn metrics(&self) -> BTreeMap<String, Value> {
            BTreeMap::new()
        }
    }

    #[async_trait]
    impl Service for RestartableService {
        fn service_type(&self) -> &'static str {
            "test"
        }

        fn workflow(&self) -> &'static str {
            DEFAULT_WORKFLOW_NAME
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        async fn start(&self) -> Result<()> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            self.running.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn stop(&self) -> Result<()> {
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn service_monitor_restarts_down_service_and_stops_on_signal() {
        let manager =
            ServiceManager::new("test", HealthManager::new(Duration::from_secs(30), "test"));
        let service = Arc::new(RestartableService {
            running: AtomicBool::new(false),
            starts: AtomicUsize::new(0),
        });
        manager.register(service.clone()).await.unwrap();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let monitored = manager.clone();
        let task = tokio::spawn(async move {
            monitored
                .monitor_with_interval(&mut shutdown_rx, Duration::from_millis(5))
                .await;
        });
        timeout(Duration::from_secs(1), async {
            while service.starts.load(Ordering::SeqCst) == 0 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
        assert!(service.running.load(Ordering::SeqCst));
        service.stop().await.unwrap();
    }

    struct RecordingWorkflow {
        name: String,
        succeed: bool,
        calls: AtomicUsize,
        stops: Arc<TokioMutex<Vec<String>>>,
        fail_stop: bool,
    }

    #[async_trait]
    impl Workflow for RecordingWorkflow {
        fn name(&self) -> &str {
            &self.name
        }

        async fn process_message(&self, _message: &Message) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.succeed {
                Ok(())
            } else {
                Err(AppError::new(ErrorType::Internal, "workflow failed"))
            }
        }

        async fn stop(&self) -> Result<()> {
            self.stops.lock().await.push(self.name.clone());
            if self.fail_stop {
                Err(AppError::new(ErrorType::Internal, "stop failed"))
            } else {
                Ok(())
            }
        }
    }

    fn message() -> Message {
        Message {
            id: "message-1".to_owned(),
            kind: "event".to_owned(),
            ..Message::default()
        }
    }

    #[tokio::test]
    async fn workflow_dispatch_handles_empty_failure_and_first_success() {
        let manager = WorkflowManager::new();
        assert_eq!(
            manager.dispatch(&message()).await.unwrap_err().error_type,
            ErrorType::NotFound
        );

        let stops = Arc::new(TokioMutex::new(Vec::new()));
        let first = Arc::new(RecordingWorkflow {
            name: "a-failure".to_owned(),
            succeed: false,
            calls: AtomicUsize::new(0),
            stops: stops.clone(),
            fail_stop: false,
        });
        let second = Arc::new(RecordingWorkflow {
            name: "b-success".to_owned(),
            succeed: true,
            calls: AtomicUsize::new(0),
            stops: stops.clone(),
            fail_stop: false,
        });
        let never = Arc::new(RecordingWorkflow {
            name: "c-never".to_owned(),
            succeed: true,
            calls: AtomicUsize::new(0),
            stops,
            fail_stop: false,
        });
        manager.register(second.clone()).await.unwrap();
        manager.register(never.clone()).await.unwrap();
        manager.register(first.clone()).await.unwrap();
        manager.dispatch(&message()).await.unwrap();
        assert_eq!(first.calls.load(Ordering::SeqCst), 1);
        assert_eq!(second.calls.load(Ordering::SeqCst), 1);
        assert_eq!(never.calls.load(Ordering::SeqCst), 0);
        assert!(manager.register(second).await.is_err());
    }

    #[tokio::test]
    async fn workflow_stop_uses_reverse_name_order_and_aggregates_failures() {
        let manager = WorkflowManager::new();
        let stops = Arc::new(TokioMutex::new(Vec::new()));
        for (name, fail_stop) in [("a", true), ("b", false), ("c", true)] {
            manager
                .register(Arc::new(RecordingWorkflow {
                    name: name.to_owned(),
                    succeed: true,
                    calls: AtomicUsize::new(0),
                    stops: stops.clone(),
                    fail_stop,
                }))
                .await
                .unwrap();
        }
        assert!(manager.stop_all().await.is_err());
        assert_eq!(*stops.lock().await, vec!["c", "b", "a"]);
    }

    #[tokio::test]
    async fn workflow_manager_covers_registration_lookup_all_failed_and_empty_stop() {
        let manager = WorkflowManager::new();
        assert!(manager.stop_all().await.is_ok());
        assert!(manager.get("missing").await.is_none());
        assert!(manager.list().await.is_empty());

        let stops = Arc::new(TokioMutex::new(Vec::new()));
        let empty = Arc::new(RecordingWorkflow {
            name: String::new(),
            succeed: false,
            calls: AtomicUsize::new(0),
            stops: stops.clone(),
            fail_stop: false,
        });
        assert!(manager.register(empty).await.is_err());

        for name in ["b", "a"] {
            manager
                .register(Arc::new(RecordingWorkflow {
                    name: name.to_owned(),
                    succeed: false,
                    calls: AtomicUsize::new(0),
                    stops: stops.clone(),
                    fail_stop: false,
                }))
                .await
                .unwrap();
        }
        assert_eq!(manager.list().await, vec!["a", "b"]);
        assert!(manager.get("a").await.is_some());
        let error = manager.dispatch(&message()).await.unwrap_err();
        assert_eq!(error.error_type, ErrorType::ServiceUnavailable);
        assert!(error.to_string().contains("a:"));
        assert!(error.to_string().contains("b:"));
        manager.stop_all().await.unwrap();
    }
}
