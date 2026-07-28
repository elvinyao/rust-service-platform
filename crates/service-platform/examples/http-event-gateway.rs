use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use service_platform::{
    AppError,
    executor::{Executor, HttpExecutor, LogExecutor},
    pipeline::{Engine, ExecutionError},
    ruleengine::{
        DEFAULT_WORKFLOW_NAME, EngineConfig, ExecutionPlan, Message, RuleProvider, YamlProvider,
    },
};
use tokio::net::TcpListener;

const MAX_EVENT_BODY_BYTES: usize = 1 << 20;

#[derive(Serialize)]
struct EventResponse {
    message_id: String,
    workflow: String,
    matched_rules: Vec<String>,
    planned_actions: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    service_platform::logger::init(
        &service_platform::logger::LoggerConfig::from_env().map_err(anyhow::Error::msg)?,
    )
    .map_err(anyhow::Error::msg)?;

    let rules_path = PathBuf::from(env_or("WORKFLOW_RULES", "config/workflow-rules.yaml"));
    let provider: Arc<dyn RuleProvider> =
        Arc::new(YamlProvider::new("yaml", rules_path, DEFAULT_WORKFLOW_NAME));
    let executors: Vec<Arc<dyn Executor>> =
        vec![Arc::new(LogExecutor), Arc::new(HttpExecutor::new())];
    let engine = Arc::new(
        Engine::new(EngineConfig::default(), vec![provider], executors)
            .await
            .context("create pipeline")?,
    );
    engine.start().await.context("start pipeline")?;

    let address = env_or("EVENT_GATEWAY_ADDR", "127.0.0.1:18081")
        .parse::<SocketAddr>()
        .context("parse EVENT_GATEWAY_ADDR")?;
    let router = Router::new()
        .route("/health", get(health))
        .route("/pipeline", get(pipeline_snapshot))
        .route("/events", post(handle_event))
        .layer(DefaultBodyLimit::max(MAX_EVENT_BODY_BYTES))
        .with_state(engine.clone());
    let listener = TcpListener::bind(address)
        .await
        .context("bind event gateway")?;
    tracing::info!(address = %listener.local_addr()?, "Event gateway listening");
    let serving = axum::serve(listener, router).with_graceful_shutdown(shutdown_signal());
    let serve_result = serving.await.context("serve event gateway");
    let stop_result = engine.stop().await.context("stop pipeline");
    serve_result.and(stop_result)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

async fn pipeline_snapshot(State(engine): State<Arc<Engine>>) -> Json<serde_json::Value> {
    Json(serde_json::to_value(engine.snapshot().await).expect("serialize engine snapshot"))
}

async fn handle_event(State(engine): State<Arc<Engine>>, body: Bytes) -> Response {
    let mut message = match serde_json::from_slice::<Message>(&body) {
        Ok(message) => message,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("decode event: {error}")})),
            )
                .into_response();
        }
    };
    if message.id.is_empty() || message.kind.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "id and type are required"})),
        )
            .into_response();
    }
    if message.timestamp.timestamp() == 0 {
        message.timestamp = chrono::Utc::now();
    }

    match engine.process(DEFAULT_WORKFLOW_NAME, &message).await {
        Ok(plan) => event_response(StatusCode::OK, &message, &plan, None),
        Err(error) => {
            if let Some(details) = execution_error(&error) {
                event_response(
                    StatusCode::BAD_GATEWAY,
                    &message,
                    &details.plan,
                    Some(error.to_string()),
                )
            } else {
                event_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &message,
                    &ExecutionPlan {
                        workflow: DEFAULT_WORKFLOW_NAME.to_owned(),
                        rules: Vec::new(),
                        actions: Vec::new(),
                    },
                    Some(error.to_string()),
                )
            }
        }
    }
}

fn event_response(
    status: StatusCode,
    message: &Message,
    plan: &ExecutionPlan,
    error: Option<String>,
) -> Response {
    (
        status,
        Json(EventResponse {
            message_id: message.id.clone(),
            workflow: plan.workflow.clone(),
            matched_rules: plan.rules.iter().map(|rule| rule.id.clone()).collect(),
            planned_actions: plan
                .actions
                .iter()
                .map(|action| action.id.clone())
                .collect(),
            error,
        }),
    )
        .into_response()
}

fn execution_error(error: &AppError) -> Option<&ExecutionError> {
    error
        .source
        .as_deref()
        .and_then(|source| source.downcast_ref::<ExecutionError>())
}

fn env_or(key: &str, fallback: &str) -> String {
    env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_owned())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                let _ = result;
            }
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
