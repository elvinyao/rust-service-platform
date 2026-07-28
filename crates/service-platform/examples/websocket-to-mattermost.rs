use std::{collections::BTreeMap, env, sync::Arc};

use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::StreamExt;
use serde_json::json;
use service_platform::{
    executor::{Executor, HttpExecutor},
    pipeline::Engine,
    ruleengine::{
        Action, Condition, ConditionOperator, DEFAULT_WORKFLOW_NAME, EngineConfig, Message, Rule,
        RuleProvider, RuleSet, StaticProvider,
    },
};
use tokio_tungstenite::connect_async;

#[tokio::main]
async fn main() -> Result<()> {
    service_platform::logger::init(
        &service_platform::logger::LoggerConfig::from_env().map_err(anyhow::Error::msg)?,
    )
    .map_err(anyhow::Error::msg)?;
    let post_url = env_or("MATTERMOST_POST_URL", "http://localhost:8091/api/v4/posts");
    let api_token = env_or("MATTERMOST_API_TOKEN", "test-token-123");
    let channel = env_or("MATTERMOST_CHANNEL", "test-channel-1");
    let body_template = format!(
        r#"{{"channel_id":"{channel}","message":"Received {{{{.Type}}}}: {{{{.Content}}}}"}}"#
    );
    let provider: Arc<dyn RuleProvider> = Arc::new(StaticProvider::new(
        "notifications",
        RuleSet {
            source: "notifications".to_owned(),
            version: "1".to_owned(),
            loaded_at: Utc::now(),
            rules: vec![Rule {
                id: "notify-aaa".to_owned(),
                workflow: DEFAULT_WORKFLOW_NAME.to_owned(),
                source: "notifications".to_owned(),
                enabled: true,
                priority: 0,
                conditions: vec![Condition {
                    field: "type".to_owned(),
                    op: ConditionOperator::Eq,
                    value: json!("AAA"),
                }],
                actions: vec![Action {
                    id: "post-notification".to_owned(),
                    executor: "http".to_owned(),
                    priority: 0,
                    params: BTreeMap::from([
                        ("method".to_owned(), json!("POST")),
                        ("url".to_owned(), json!(post_url)),
                        (
                            "headers".to_owned(),
                            json!({"Authorization": format!("Bearer {api_token}")}),
                        ),
                        ("body_template".to_owned(), json!(body_template)),
                    ]),
                }],
            }],
            last_error: String::new(),
        },
    ));
    let mut config = EngineConfig::default();
    config.workflows[0].providers = vec!["notifications".to_owned()];
    config.workflows[0].pipeline_order = vec!["notifications".to_owned()];
    let executors: Vec<Arc<dyn Executor>> = vec![Arc::new(HttpExecutor::new())];
    let engine = Engine::new(config, vec![provider], executors)
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
                "Notification processed"
            ),
            Err(error) => tracing::error!(
                message = %message.id,
                error = %error,
                "Notification failed"
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
