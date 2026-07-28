use std::{env, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use service_platform::{
    executor::{Executor, HttpExecutor, LogExecutor},
    pipeline::Engine,
    ruleengine::{DEFAULT_WORKFLOW_NAME, EngineConfig, Message, RuleProvider, YamlProvider},
};
use tokio_tungstenite::connect_async;

#[tokio::main]
async fn main() -> Result<()> {
    service_platform::logger::init(
        &service_platform::logger::LoggerConfig::from_env().map_err(anyhow::Error::msg)?,
    )
    .map_err(anyhow::Error::msg)?;
    let provider: Arc<dyn RuleProvider> = Arc::new(YamlProvider::new(
        "yaml",
        PathBuf::from(env_or("WORKFLOW_RULES", "config/workflow-rules.yaml")),
        DEFAULT_WORKFLOW_NAME,
    ));
    let executors: Vec<Arc<dyn Executor>> =
        vec![Arc::new(LogExecutor), Arc::new(HttpExecutor::new())];
    let engine = Engine::new(EngineConfig::default(), vec![provider], executors)
        .await
        .context("create pipeline")?;
    engine.start().await.context("start pipeline")?;

    let websocket_url = env_or("WEBSOCKET_URL", "ws://localhost:8093/ws");
    let (mut connection, _) = connect_async(&websocket_url)
        .await
        .with_context(|| format!("connect to {websocket_url}"))?;
    tracing::info!(url = %websocket_url, "WebSocket connected");
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        let incoming = tokio::select! {
            () = &mut shutdown => break,
            incoming = connection.next() => incoming,
        };
        let Some(incoming) = incoming else {
            break;
        };
        let frame = incoming.context("read WebSocket frame")?;
        if !frame.is_text() && !frame.is_binary() {
            continue;
        }
        let message =
            serde_json::from_slice::<Message>(&frame.into_data()).context("decode event")?;
        if message.kind == "system" {
            continue;
        }
        match engine.process(DEFAULT_WORKFLOW_NAME, &message).await {
            Ok(plan) => tracing::info!(
                message = %message.id,
                matched_rules = plan.rules.len(),
                actions = plan.actions.len(),
                "Event processed"
            ),
            Err(error) => tracing::error!(
                message = %message.id,
                error = %error,
                "Event processing failed"
            ),
        }
    }

    engine.stop().await.context("stop pipeline")
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
