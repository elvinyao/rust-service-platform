//! Reference application wiring and administrative HTTP API.

use std::{path::PathBuf, sync::Arc, time::Duration};

use axum::{Json, Router, extract::State, routing::get};
use serde::Serialize;
use serde_json::Value;
use service_platform::{
    config::RuntimeConfig,
    errors::{AppError, ErrorType, Result},
    health::{self, HealthManager},
    ruleengine::Message,
    runtime::{AdminServer, AdminServerOptions},
};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time::timeout,
};

use crate::{
    container::Container,
    manager::{ServiceManager, Workflow, WorkflowManager},
    service::{MessageHandler, WEBSOCKET_INPUT_SERVICE_NAME, WebSocketService},
    workflow::{EngineAdminSnapshot, WorkflowEngine},
};

const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const APP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct AppState {
    started: bool,
    container: Option<Arc<Container>>,
    health: Option<Arc<HealthManager>>,
    services: Option<Arc<ServiceManager>>,
    workflows: Option<Arc<WorkflowManager>>,
    workflow_engine: Option<Arc<WorkflowEngine>>,
    admin: Option<Arc<AdminServer>>,
    monitor_shutdown: Option<watch::Sender<bool>>,
    monitor_task: Option<JoinHandle<()>>,
}

impl AppState {
    fn has_owned_wiring(&self) -> bool {
        self.container.is_some()
            || self.health.is_some()
            || self.services.is_some()
            || self.workflows.is_some()
            || self.workflow_engine.is_some()
            || self.admin.is_some()
            || self.monitor_shutdown.is_some()
            || self.monitor_task.is_some()
    }
}

pub struct App {
    name: String,
    config: RuntimeConfig,
    engine_config_path: PathBuf,
    rules_path: PathBuf,
    state: Mutex<AppState>,
}

impl App {
    pub fn new(
        name: impl Into<String>,
        config: RuntimeConfig,
        engine_config_path: impl Into<PathBuf>,
        rules_path: impl Into<PathBuf>,
    ) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            config,
            engine_config_path: engine_config_path.into(),
            rules_path: rules_path.into(),
            state: Mutex::new(AppState::default()),
        })
    }

    pub fn configured_admin_address(&self) -> &str {
        &self.config.admin.address
    }

    pub async fn admin_address(&self) -> Option<std::net::SocketAddr> {
        let admin = self.state.lock().await.admin.clone();
        match admin {
            Some(admin) => admin.address().await,
            None => None,
        }
    }

    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.started {
            return Err(AppError::new(
                ErrorType::Conflict,
                "application is already started",
            ));
        }
        if state.has_owned_wiring() {
            return Err(AppError::new(
                ErrorType::Conflict,
                "application has components pending cleanup",
            ));
        }
        self.config.validate()?;

        let container = Container::new(APP_VERSION, self.config.clone());
        let health = container.health_manager();
        let services = container.service_manager();
        state.container = Some(container.clone());
        state.health = Some(health.clone());
        state.services = Some(services.clone());

        if let Err(error) = container.register_services().await {
            let cleanup = stop_state(&mut state).await.err();
            return Err(with_cleanup(error, cleanup));
        }
        let service_start = if services.list().await.is_empty() {
            Ok(())
        } else {
            services.start_all().await
        };
        if let Err(error) = service_start {
            let cleanup = stop_state(&mut state).await.err();
            return Err(with_cleanup(error, cleanup));
        }

        let (monitor_tx, monitor_rx) = watch::channel(false);
        state.monitor_shutdown = Some(monitor_tx);
        state.monitor_task = Some(tokio::spawn(services.clone().monitor(monitor_rx)));

        let workflow_engine = match WorkflowEngine::new(
            services.clone(),
            &self.engine_config_path,
            &self.rules_path,
            &self.config,
        )
        .await
        {
            Ok(engine) => engine,
            Err(error) => {
                let cleanup = stop_state(&mut state).await.err();
                return Err(with_cleanup(error, cleanup));
            }
        };
        let workflows = WorkflowManager::new();
        let workflow: Arc<dyn Workflow> = workflow_engine.clone();
        if let Err(error) = workflows.register(workflow).await {
            let _ = workflow_engine.stop().await;
            let cleanup = stop_state(&mut state).await.err();
            return Err(with_cleanup(error, cleanup));
        }
        state.workflows = Some(workflows.clone());
        state.workflow_engine = Some(workflow_engine.clone());

        let listener_setup = if self.config.inputs.websocket.enabled {
            setup_message_listener(&services, &workflows).await
        } else {
            Ok(())
        };
        if let Err(error) = listener_setup {
            let cleanup = stop_state(&mut state).await.err();
            return Err(with_cleanup(error, cleanup));
        }

        let admin_state = AdminApiState {
            services,
            workflows,
            workflow_engine,
        };
        let router = health::router(health).merge(admin_router(admin_state));
        let admin = Arc::new(AdminServer::with_options(
            &self.config.admin.address,
            router,
            AdminServerOptions::from(&self.config.admin),
        ));
        if let Err(error) = admin.start().await {
            let cleanup = stop_state(&mut state).await.err();
            return Err(with_cleanup(error, cleanup));
        }
        tracing::info!(
            application = %self.name,
            address = ?admin.address().await,
            "Admin server started"
        );
        state.admin = Some(admin);
        state.started = true;
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        let result = stop_state(&mut state).await;
        if result.is_ok() {
            state.started = false;
        }
        result
    }
}

async fn setup_message_listener(
    services: &Arc<ServiceManager>,
    workflows: &Arc<WorkflowManager>,
) -> Result<()> {
    let service = services
        .get(WEBSOCKET_INPUT_SERVICE_NAME)
        .await
        .ok_or_else(|| {
            AppError::new(
                ErrorType::NotFound,
                format!("Service not found: {WEBSOCKET_INPUT_SERVICE_NAME}"),
            )
        })?;
    let websocket = service
        .as_any()
        .downcast_ref::<WebSocketService>()
        .ok_or_else(|| AppError::new(ErrorType::Internal, "Service is not WebSocketService"))?;
    let workflows = workflows.clone();
    let handler: MessageHandler = Arc::new(move |message: Message| {
        let workflows = workflows.clone();
        Box::pin(async move {
            if let Err(error) = workflows.dispatch(&message).await {
                tracing::error!(
                    message_id = %message.id,
                    message_type = %message.kind,
                    error = %error,
                    "Error dispatching message"
                );
            }
        })
    });
    websocket.on_message(handler).await;
    Ok(())
}

async fn stop_state(state: &mut AppState) -> Result<()> {
    stop_state_with_timeout(state, APP_SHUTDOWN_TIMEOUT).await
}

async fn stop_state_with_timeout(state: &mut AppState, shutdown_timeout: Duration) -> Result<()> {
    let mut failures = Vec::new();
    if let Some(health) = &state.health {
        health.set_shutting_down();
    }
    if let Some(shutdown) = state.monitor_shutdown.take() {
        let _ = shutdown.send(true);
    }
    if let Some(mut task) = state.monitor_task.take() {
        match timeout(shutdown_timeout, &mut task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(format!("stop service monitor: {error}")),
            Err(_) => {
                task.abort();
                let _ = task.await;
                failures.push("stop service monitor: shutdown timed out".to_owned());
            }
        }
    }
    if let Some(admin) = state.admin.take() {
        if let Err(error) = admin.stop().await {
            failures.push(format!("stop admin server: {error}"));
        }
    }
    if let Some(workflows) = state.workflows.clone() {
        match timeout(shutdown_timeout, workflows.stop_all()).await {
            Ok(Ok(())) => {
                state.workflows = None;
                state.workflow_engine = None;
            }
            Ok(Err(error)) => failures.push(format!("stop workflows: {error}")),
            Err(_) => failures.push("stop workflows: shutdown timed out".to_owned()),
        }
    } else if let Some(engine) = state.workflow_engine.clone() {
        match timeout(shutdown_timeout, engine.stop()).await {
            Ok(Ok(())) => state.workflow_engine = None,
            Ok(Err(error)) => failures.push(format!("stop workflow: {error}")),
            Err(_) => failures.push("stop workflow: shutdown timed out".to_owned()),
        }
    }
    if let Some(services) = state.services.clone() {
        match timeout(shutdown_timeout, services.stop_all()).await {
            Ok(Ok(())) => state.services = None,
            Ok(Err(error)) => failures.push(format!("stop services: {error}")),
            Err(_) => failures.push("stop services: shutdown timed out".to_owned()),
        }
    }
    if failures.is_empty() {
        state.health = None;
        state.container = None;
        Ok(())
    } else {
        Err(AppError::new(
            ErrorType::ServiceUnavailable,
            failures.join("; "),
        ))
    }
}

fn with_cleanup(error: AppError, cleanup: Option<AppError>) -> AppError {
    match cleanup {
        None => error,
        Some(cleanup) => AppError::new(
            error.error_type,
            format!("{error}; clean up failed startup: {cleanup}"),
        ),
    }
}

#[derive(Clone)]
struct AdminApiState {
    services: Arc<ServiceManager>,
    workflows: Arc<WorkflowManager>,
    workflow_engine: Arc<WorkflowEngine>,
}

fn admin_router(state: AdminApiState) -> Router {
    Router::new()
        .route("/services", get(services_list))
        .route("/workflows", get(workflows_list))
        .route("/rule-engine", get(rule_engine_info))
        .with_state(state)
}

#[derive(Serialize)]
struct ServiceInfo {
    name: String,
    running: bool,
    workflow: String,
    #[serde(rename = "type")]
    service_type: String,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    metrics: std::collections::BTreeMap<String, Value>,
}

async fn services_list(State(state): State<AdminApiState>) -> Json<Vec<ServiceInfo>> {
    let mut response = Vec::new();
    for service in state.services.list().await {
        response.push(ServiceInfo {
            name: service.name().to_owned(),
            running: service.is_running().await,
            workflow: service.workflow().to_owned(),
            service_type: service.service_type().to_owned(),
            metrics: service.metrics().await,
        });
    }
    Json(response)
}

#[derive(Serialize)]
struct WorkflowInfo {
    name: String,
}

async fn workflows_list(State(state): State<AdminApiState>) -> Json<Vec<WorkflowInfo>> {
    Json(
        state
            .workflows
            .list()
            .await
            .into_iter()
            .map(|name| WorkflowInfo { name })
            .collect(),
    )
}

async fn rule_engine_info(State(state): State<AdminApiState>) -> Json<EngineAdminSnapshot> {
    Json(state.workflow_engine.admin_snapshot().await)
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        path::Path,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use reqwest::StatusCode;

    use super::*;

    struct RetryStopWorkflow {
        stops: AtomicUsize,
    }

    #[async_trait]
    impl Workflow for RetryStopWorkflow {
        fn name(&self) -> &'static str {
            "retry"
        }

        async fn process_message(&self, _message: &Message) -> Result<()> {
            Ok(())
        }

        async fn stop(&self) -> Result<()> {
            if self.stops.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(AppError::new(ErrorType::ServiceUnavailable, "retry stop"))
            } else {
                Ok(())
            }
        }
    }

    struct NeverStopWorkflow;

    #[async_trait]
    impl Workflow for NeverStopWorkflow {
        fn name(&self) -> &'static str {
            "never-stop"
        }

        async fn process_message(&self, _message: &Message) -> Result<()> {
            Ok(())
        }

        async fn stop(&self) -> Result<()> {
            pending().await
        }
    }

    #[tokio::test]
    async fn failed_cleanup_remains_owned_until_retry_succeeds() {
        let workflow = Arc::new(RetryStopWorkflow {
            stops: AtomicUsize::new(0),
        });
        let workflows = WorkflowManager::new();
        workflows.register(workflow.clone()).await.unwrap();
        let mut state = AppState {
            started: true,
            health: Some(HealthManager::new(Duration::ZERO, "test")),
            workflows: Some(workflows),
            ..AppState::default()
        };

        assert!(stop_state(&mut state).await.is_err());
        assert!(state.has_owned_wiring());
        assert!(state.workflows.is_some());
        assert!(state.health.is_some());

        stop_state(&mut state).await.unwrap();
        assert!(state.workflows.is_none());
        assert!(state.health.is_none());
        assert!(!state.has_owned_wiring());
        assert_eq!(workflow.stops.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn shutdown_bounds_monitor_and_retains_timed_out_workflow() {
        let workflows = WorkflowManager::new();
        workflows
            .register(Arc::new(NeverStopWorkflow))
            .await
            .unwrap();
        let mut state = AppState {
            started: true,
            health: Some(HealthManager::new(Duration::ZERO, "test")),
            workflows: Some(workflows),
            monitor_task: Some(tokio::spawn(pending())),
            ..AppState::default()
        };

        let started = tokio::time::Instant::now();
        let error = stop_state_with_timeout(&mut state, Duration::from_millis(1))
            .await
            .unwrap_err()
            .to_string();

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(error.contains("stop service monitor: shutdown timed out"));
        assert!(error.contains("stop workflows: shutdown timed out"));
        assert!(state.monitor_task.is_none());
        assert!(state.workflows.is_some());
        assert!(state.health.is_some());
        assert!(state.has_owned_wiring());
    }

    #[tokio::test]
    async fn app_exposes_admin_contract_and_can_restart() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config = RuntimeConfig::default();
        config.admin.address = "127.0.0.1:0".to_owned();
        config.inputs.websocket.enabled = false;
        let app = App::new(
            "test",
            config,
            root.join("config/rule-engine.yaml"),
            root.join("config/workflow-rules.yaml"),
        );
        for _ in 0..2 {
            app.start().await.unwrap();
            let address = app.admin_address().await.unwrap();
            for (path, expected) in [
                ("/health", StatusCode::OK),
                ("/health/readiness", StatusCode::OK),
                ("/health/liveness", StatusCode::OK),
                ("/health/service?service=missing", StatusCode::NOT_FOUND),
                ("/services", StatusCode::OK),
                ("/workflows", StatusCode::OK),
                ("/rule-engine", StatusCode::OK),
            ] {
                let response = reqwest::get(format!("http://{address}{path}"))
                    .await
                    .unwrap();
                assert_eq!(response.status(), expected, "{path}");
            }
            app.stop().await.unwrap();
        }
    }

    #[tokio::test]
    async fn duplicate_start_is_rejected_without_replacing_wiring() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut config = RuntimeConfig::default();
        config.admin.address = "127.0.0.1:0".to_owned();
        config.inputs.websocket.enabled = false;
        let app = App::new(
            "test",
            config,
            root.join("config/rule-engine.yaml"),
            root.join("config/workflow-rules.yaml"),
        );
        app.start().await.unwrap();
        assert!(app.start().await.is_err());
        assert!(app.admin_address().await.is_some());
        app.stop().await.unwrap();
    }
}
