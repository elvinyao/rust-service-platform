//! Optional Confluence provider and Mattermost/BadgeDB executors.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Value, json};
use service_platform::{
    errors::{AppError, ErrorType, Result},
    executor::{Executor, render_template},
    health::ServiceHealth,
    ruleengine::{
        Action, Condition, ConditionOperator, Message, Rule, RuleProvider, RuleSet,
        validate_rule_set,
    },
};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::service::{Badge, BadgeDbService, ConfluenceSettingsService, MattermostService};

pub struct ConfluenceProvider {
    name: String,
    workflow: String,
    settings: Arc<ConfluenceSettingsService>,
    last_good: RwLock<RuleSet>,
}

impl ConfluenceProvider {
    pub fn new(
        name: impl Into<String>,
        workflow: impl Into<String>,
        settings: Arc<ConfluenceSettingsService>,
    ) -> Self {
        let name = name.into();
        Self {
            last_good: RwLock::new(RuleSet::empty(name.clone())),
            name,
            workflow: workflow.into(),
            settings,
        }
    }

    async fn current_snapshot(&self) -> Result<RuleSet> {
        if !self.settings.is_running().await {
            return Err(AppError::new(
                ErrorType::ServiceUnavailable,
                "service ConfluenceSettingsService is not running",
            ));
        }

        let mut rules = Vec::new();
        for (index, setting) in self.settings.settings().await.into_iter().enumerate() {
            let mut conditions = vec![Condition {
                field: "type".to_owned(),
                op: ConditionOperator::Eq,
                value: Value::String(setting.event_type.clone()),
            }];
            if !setting.pattern.is_empty() {
                conditions.push(Condition {
                    field: "content".to_owned(),
                    op: ConditionOperator::Regex,
                    value: Value::String(setting.pattern),
                });
            }
            rules.push(Rule {
                id: format!("cf_rule_{index}_{}", setting.event_type),
                workflow: self.workflow.clone(),
                source: self.name.clone(),
                enabled: setting.enabled,
                priority: i32::try_from(index).unwrap_or(i32::MAX) + 1000,
                conditions,
                actions: vec![Action {
                    id: format!("cf_action_{index}"),
                    executor: "mattermost".to_owned(),
                    priority: 100,
                    params: BTreeMap::from([
                        ("channel_id".to_owned(), json!(setting.channel_id)),
                        ("template".to_owned(), json!(setting.message_template)),
                    ]),
                }],
            });
        }
        let snapshot = RuleSet {
            source: self.name.clone(),
            version: "1".to_owned(),
            loaded_at: Utc::now(),
            rules,
            last_error: String::new(),
        };
        validate_rule_set(&snapshot)?;
        Ok(snapshot)
    }
}

#[async_trait]
impl RuleProvider for ConfluenceProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<()> {
        let snapshot = self.current_snapshot().await?;
        *self.last_good.write().await = snapshot;
        Ok(())
    }

    async fn snapshot(&self) -> RuleSet {
        match self.current_snapshot().await {
            Ok(snapshot) => {
                *self.last_good.write().await = snapshot.clone();
                snapshot
            }
            Err(error) => {
                let mut fallback = self.last_good.read().await.clone();
                fallback.last_error = error.to_string();
                fallback
            }
        }
    }
}

pub struct MattermostExecutor {
    mattermost: Arc<MattermostService>,
}

impl MattermostExecutor {
    pub fn new(mattermost: Arc<MattermostService>) -> Self {
        Self { mattermost }
    }
}

#[async_trait]
impl Executor for MattermostExecutor {
    fn executor_type(&self) -> &'static str {
        "mattermost"
    }

    async fn execute(&self, message: &Message, action: &Action) -> Result<()> {
        let template = action
            .params
            .get("template")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mut output = render_template(template, message).map_err(|error| {
            AppError::wrap(ErrorType::InvalidInput, "render mattermost template", error)
        })?;
        if output.is_empty() {
            output = format!(
                "Event received: type={} id={} content={}",
                message.kind, message.id, message.content
            );
        }
        match action.params.get("channel_id").and_then(Value::as_str) {
            Some(channel) if !channel.is_empty() => {
                self.mattermost
                    .send_message_to_channel(channel, &output)
                    .await
            }
            _ => self.mattermost.send_message(&output).await,
        }
    }
}

pub struct BadgeDbExecutor {
    database: Arc<BadgeDbService>,
}

impl BadgeDbExecutor {
    pub fn new(database: Arc<BadgeDbService>) -> Self {
        Self { database }
    }
}

#[async_trait]
impl Executor for BadgeDbExecutor {
    fn executor_type(&self) -> &'static str {
        "db"
    }

    async fn execute(&self, message: &Message, action: &Action) -> Result<()> {
        let string_param = |name: &str, fallback: &str| {
            action
                .params
                .get(name)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or(fallback)
                .to_owned()
        };
        let badge = Badge {
            id: format!("wf_{}_{}", message.id, Uuid::new_v4()),
            name: string_param("name", "workflow-badge"),
            description: string_param("description", "created by workflow executor"),
            user_id: message.user_id.clone(),
            awarded_at: Utc::now(),
            kind: string_param("badge_type", &message.kind),
            attributes: BTreeMap::from([
                ("source_message_id".to_owned(), json!(message.id)),
                ("action_id".to_owned(), json!(action.id)),
            ]),
            revoked: false,
            revoked_at: None,
        };
        self.database.save_badge(badge).await
    }
}

#[cfg(test)]
mod tests {
    use service_platform::ruleengine::DEFAULT_WORKFLOW_NAME;

    use crate::{config::RuntimeConfig, service::Service};

    use super::*;

    fn message() -> Message {
        Message {
            id: "message-1".to_owned(),
            kind: "DEPLOY".to_owned(),
            content: "production".to_owned(),
            user_id: "user-1".to_owned(),
            ..Message::default()
        }
    }

    fn action(executor: &str) -> Action {
        Action {
            id: "action-1".to_owned(),
            executor: executor.to_owned(),
            priority: 100,
            params: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn confluence_provider_rejects_stopped_service_and_keeps_last_snapshot() {
        let settings = Arc::new(
            ConfluenceSettingsService::new(RuntimeConfig::default().adapters.confluence, "test")
                .unwrap(),
        );
        let provider = ConfluenceProvider::new("confluence", DEFAULT_WORKFLOW_NAME, settings);
        assert_eq!(provider.name(), "confluence");
        assert_eq!(
            provider.start().await.unwrap_err().error_type,
            ErrorType::ServiceUnavailable
        );
        let snapshot = provider.snapshot().await;
        assert_eq!(snapshot.source, "confluence");
        assert!(snapshot.rules.is_empty());
        assert!(snapshot.last_error.contains("not running"));
    }

    #[tokio::test]
    async fn mattermost_executor_validates_templates_before_sending() {
        let mattermost = Arc::new(
            MattermostService::new(RuntimeConfig::default().adapters.mattermost, "test").unwrap(),
        );
        let executor = MattermostExecutor::new(mattermost);
        assert_eq!(executor.executor_type(), "mattermost");

        let mut invalid = action("mattermost");
        invalid
            .params
            .insert("template".to_owned(), json!("{{.Unsupported}}"));
        assert_eq!(
            executor
                .execute(&message(), &invalid)
                .await
                .unwrap_err()
                .error_type,
            ErrorType::InvalidInput
        );

        let mut explicit_channel = action("mattermost");
        explicit_channel
            .params
            .insert("channel_id".to_owned(), json!("alerts"));
        assert_eq!(
            executor
                .execute(&message(), &explicit_channel)
                .await
                .unwrap_err()
                .error_type,
            ErrorType::InvalidInput
        );
        assert_eq!(
            executor
                .execute(&message(), &action("mattermost"))
                .await
                .unwrap_err()
                .error_type,
            ErrorType::InvalidInput
        );
    }

    #[tokio::test]
    async fn badgedb_executor_applies_custom_and_default_badge_fields() {
        let database = Arc::new(BadgeDbService::new(
            RuntimeConfig::default().adapters.badgedb,
            "test",
        ));
        database.start().await.unwrap();
        let executor = BadgeDbExecutor::new(database.clone());
        assert_eq!(executor.executor_type(), "db");

        let mut custom = action("db");
        custom.params.insert("name".to_owned(), json!("release"));
        custom
            .params
            .insert("description".to_owned(), json!("deployed"));
        custom
            .params
            .insert("badge_type".to_owned(), json!("production"));
        executor.execute(&message(), &custom).await.unwrap();

        let mut defaults = action("db");
        defaults.params.insert("name".to_owned(), json!(""));
        executor.execute(&message(), &defaults).await.unwrap();
        assert_eq!(database.metrics().await["db_entries"], 2);
        database.stop().await.unwrap();
    }
}
