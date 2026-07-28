use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Value, json};
use service_platform::{
    errors::{AppError, ErrorType},
    executor::{Executor, render_template},
    pipeline::Engine,
    ruleengine::{
        Action, Condition, ConditionOperator, DEFAULT_WORKFLOW_NAME, EngineConfig, Message, Rule,
        RuleProvider, RuleSet, StaticProvider,
    },
};

#[derive(Default)]
struct CollectExecutor {
    entries: Mutex<Vec<String>>,
}

impl CollectExecutor {
    fn entries(&self) -> service_platform::Result<Vec<String>> {
        self.entries
            .lock()
            .map(|entries| entries.clone())
            .map_err(|_| AppError::new(ErrorType::Internal, "collector lock is poisoned"))
    }
}

#[async_trait]
impl Executor for CollectExecutor {
    fn executor_type(&self) -> &'static str {
        "collect"
    }

    async fn execute(&self, message: &Message, action: &Action) -> service_platform::Result<()> {
        let template = action
            .params
            .get("template")
            .and_then(Value::as_str)
            .filter(|template| !template.is_empty())
            .ok_or_else(|| {
                AppError::new(
                    ErrorType::InvalidInput,
                    "collect action requires params.template",
                )
            })?;
        let entry = render_template(template, message).map_err(|error| {
            AppError::wrap(ErrorType::InvalidInput, "render collect template", error)
        })?;
        self.entries
            .lock()
            .map_err(|_| AppError::new(ErrorType::Internal, "collector lock is poisoned"))?
            .push(entry);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let collector = Arc::new(CollectExecutor::default());
    let provider: Arc<dyn RuleProvider> = Arc::new(StaticProvider::new(
        "local",
        RuleSet {
            source: "local".to_owned(),
            version: "1".to_owned(),
            loaded_at: Utc::now(),
            rules: vec![Rule {
                id: "collect-audit-entry".to_owned(),
                workflow: DEFAULT_WORKFLOW_NAME.to_owned(),
                source: "local".to_owned(),
                enabled: true,
                priority: 0,
                conditions: vec![Condition {
                    field: "type".to_owned(),
                    op: ConditionOperator::Eq,
                    value: json!("AUDIT"),
                }],
                actions: vec![Action {
                    id: "store-audit-entry".to_owned(),
                    executor: collector.executor_type().to_owned(),
                    priority: 0,
                    params: BTreeMap::from([(
                        "template".to_owned(),
                        json!("event={{.ID}} user={{.UserID}} content={{.Content}}"),
                    )]),
                }],
            }],
            last_error: String::new(),
        },
    ));
    let mut config = EngineConfig::default();
    config.workflows[0].providers = vec!["local".to_owned()];
    config.workflows[0].pipeline_order = vec!["local".to_owned()];
    let executors: Vec<Arc<dyn Executor>> = vec![collector.clone()];
    let engine = Engine::new(config, vec![provider], executors).await?;
    engine.start().await?;

    engine
        .process(
            DEFAULT_WORKFLOW_NAME,
            &Message {
                id: "audit-1".to_owned(),
                kind: "AUDIT".to_owned(),
                content: "configuration changed".to_owned(),
                user_id: "local-user".to_owned(),
                ..Message::default()
            },
        )
        .await?;
    for entry in collector.entries()? {
        println!("{entry}");
    }

    engine.stop().await?;
    Ok(())
}
