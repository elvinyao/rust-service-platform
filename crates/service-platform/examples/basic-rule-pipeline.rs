use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use service_platform::{
    executor::{Executor, LogExecutor},
    pipeline::Engine,
    ruleengine::{
        Action, Condition, ConditionOperator, DEFAULT_WORKFLOW_NAME, EngineConfig, Message, Rule,
        RuleProvider, RuleSet, StaticProvider,
    },
};

#[tokio::main]
async fn main() -> Result<()> {
    service_platform::logger::init(&service_platform::logger::LoggerConfig::default())
        .map_err(anyhow::Error::msg)?;

    let provider: Arc<dyn RuleProvider> = Arc::new(StaticProvider::new(
        "local",
        RuleSet {
            source: "local".to_owned(),
            version: "1".to_owned(),
            loaded_at: Utc::now(),
            rules: vec![Rule {
                id: "greet-event".to_owned(),
                workflow: DEFAULT_WORKFLOW_NAME.to_owned(),
                source: "local".to_owned(),
                enabled: true,
                priority: 0,
                conditions: vec![Condition {
                    field: "type".to_owned(),
                    op: ConditionOperator::Eq,
                    value: json!("GREETING"),
                }],
                actions: vec![Action {
                    id: "log-greeting".to_owned(),
                    executor: "log".to_owned(),
                    priority: 0,
                    params: BTreeMap::from([
                        ("level".to_owned(), json!("info")),
                        (
                            "template".to_owned(),
                            json!("Greeting from {{.UserID}}: {{.Content}}"),
                        ),
                    ]),
                }],
            }],
            last_error: String::new(),
        },
    ));
    let mut config = EngineConfig::default();
    config.workflows[0].providers = vec!["local".to_owned()];
    config.workflows[0].pipeline_order = vec!["local".to_owned()];
    let executors: Vec<Arc<dyn Executor>> = vec![Arc::new(LogExecutor)];
    let engine = Engine::new(config, vec![provider], executors).await?;
    engine.start().await?;

    let plan = engine
        .process(
            DEFAULT_WORKFLOW_NAME,
            &Message {
                id: "example-1".to_owned(),
                kind: "GREETING".to_owned(),
                content: "hello from the framework".to_owned(),
                user_id: "local-user".to_owned(),
                ..Message::default()
            },
        )
        .await?;
    println!(
        "matched_rules={} executed_actions={}",
        plan.rules.len(),
        plan.actions.len()
    );

    engine.stop().await?;
    Ok(())
}
