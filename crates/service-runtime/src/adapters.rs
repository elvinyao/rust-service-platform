//! Optional Confluence provider and Mattermost/BadgeDB executors.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Value, json};
use service_platform::{
    errors::{AppError, ErrorType, Result},
    executor::{Executor, render_template},
    ruleengine::{
        Action, Condition, ConditionOperator, Message, Rule, RuleProvider, RuleSet,
        validate_rule_set,
    },
};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    manager::ServiceManager,
    service::{
        BADGEDB_SERVICE_NAME, Badge, BadgeDbService, CONFLUENCE_SETTINGS_SERVICE_NAME,
        ConfluenceSettingsService, MATTERMOST_SERVICE_NAME, MattermostService,
    },
};

pub struct ConfluenceProvider {
    name: String,
    workflow: String,
    services: Arc<ServiceManager>,
    last_good: RwLock<RuleSet>,
}

impl ConfluenceProvider {
    pub fn new(
        name: impl Into<String>,
        workflow: impl Into<String>,
        services: Arc<ServiceManager>,
    ) -> Self {
        let name = name.into();
        Self {
            last_good: RwLock::new(RuleSet::empty(name.clone())),
            name,
            workflow: workflow.into(),
            services,
        }
    }

    async fn current_snapshot(&self) -> Result<RuleSet> {
        let service = self
            .services
            .get(CONFLUENCE_SETTINGS_SERVICE_NAME)
            .await
            .ok_or_else(|| {
                AppError::new(
                    ErrorType::NotFound,
                    "service ConfluenceSettingsService not found",
                )
            })?;
        let settings_service = service
            .as_any()
            .downcast_ref::<ConfluenceSettingsService>()
            .ok_or_else(|| {
                AppError::new(
                    ErrorType::Internal,
                    "service ConfluenceSettingsService type mismatch",
                )
            })?;
        if !service.is_running().await {
            return Err(AppError::new(
                ErrorType::ServiceUnavailable,
                "service ConfluenceSettingsService is not running",
            ));
        }

        let mut rules = Vec::new();
        for (index, setting) in settings_service.settings().await.into_iter().enumerate() {
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
    services: Arc<ServiceManager>,
}

impl MattermostExecutor {
    pub fn new(services: Arc<ServiceManager>) -> Self {
        Self { services }
    }
}

#[async_trait]
impl Executor for MattermostExecutor {
    fn executor_type(&self) -> &'static str {
        "mattermost"
    }

    async fn execute(&self, message: &Message, action: &Action) -> Result<()> {
        let service = self
            .services
            .get(MATTERMOST_SERVICE_NAME)
            .await
            .ok_or_else(|| AppError::new(ErrorType::NotFound, "MattermostService not found"))?;
        let mattermost = service
            .as_any()
            .downcast_ref::<MattermostService>()
            .ok_or_else(|| AppError::new(ErrorType::Internal, "MattermostService type mismatch"))?;
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
                mattermost.send_message_to_channel(channel, &output).await
            }
            _ => mattermost.send_message(&output).await,
        }
    }
}

pub struct BadgeDbExecutor {
    services: Arc<ServiceManager>,
}

impl BadgeDbExecutor {
    pub fn new(services: Arc<ServiceManager>) -> Self {
        Self { services }
    }
}

#[async_trait]
impl Executor for BadgeDbExecutor {
    fn executor_type(&self) -> &'static str {
        "db"
    }

    async fn execute(&self, message: &Message, action: &Action) -> Result<()> {
        let service = self
            .services
            .get(BADGEDB_SERVICE_NAME)
            .await
            .ok_or_else(|| AppError::new(ErrorType::NotFound, "BadgeDBService not found"))?;
        let database = service
            .as_any()
            .downcast_ref::<BadgeDbService>()
            .ok_or_else(|| AppError::new(ErrorType::Internal, "BadgeDBService type mismatch"))?;
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
        database.save_badge(badge).await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };

    use service_platform::{
        config::RuntimeConfig,
        health::{CheckResult, HealthManager, ServiceHealth},
        ruleengine::DEFAULT_WORKFLOW_NAME,
    };

    use crate::service::{Service, WebSocketService};

    use super::*;

    struct NamedService {
        name: &'static str,
        running: AtomicBool,
    }

    #[async_trait]
    impl ServiceHealth for NamedService {
        fn name(&self) -> &str {
            self.name
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
    impl Service for NamedService {
        fn service_type(&self) -> &'static str {
            "wrong"
        }

        fn workflow(&self) -> &str {
            DEFAULT_WORKFLOW_NAME
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn start(&self) -> Result<()> {
            self.running.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn stop(&self) -> Result<()> {
            self.running.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    fn manager() -> Arc<ServiceManager> {
        ServiceManager::new("test", HealthManager::new(Duration::from_secs(30), "test"))
    }

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
    async fn adapters_report_missing_services_and_provider_keeps_last_snapshot() {
        let services = manager();
        let provider =
            ConfluenceProvider::new("confluence", DEFAULT_WORKFLOW_NAME, services.clone());
        assert_eq!(provider.name(), "confluence");
        assert_eq!(
            provider.start().await.unwrap_err().error_type,
            ErrorType::NotFound
        );
        let snapshot = provider.snapshot().await;
        assert_eq!(snapshot.source, "confluence");
        assert!(snapshot.rules.is_empty());
        assert!(snapshot.last_error.contains("not found"));

        let mattermost = MattermostExecutor::new(services.clone());
        assert_eq!(mattermost.executor_type(), "mattermost");
        assert_eq!(
            mattermost
                .execute(&message(), &action("mattermost"))
                .await
                .unwrap_err()
                .error_type,
            ErrorType::NotFound
        );

        let database = BadgeDbExecutor::new(services);
        assert_eq!(database.executor_type(), "db");
        assert_eq!(
            database
                .execute(&message(), &action("db"))
                .await
                .unwrap_err()
                .error_type,
            ErrorType::NotFound
        );
    }

    #[tokio::test]
    async fn adapters_reject_services_registered_under_the_wrong_concrete_type() {
        let services = manager();
        for name in [
            CONFLUENCE_SETTINGS_SERVICE_NAME,
            MATTERMOST_SERVICE_NAME,
            BADGEDB_SERVICE_NAME,
        ] {
            services
                .register(Arc::new(NamedService {
                    name,
                    running: AtomicBool::new(true),
                }))
                .await
                .unwrap();
        }

        let provider =
            ConfluenceProvider::new("confluence", DEFAULT_WORKFLOW_NAME, services.clone());
        assert_eq!(
            provider.start().await.unwrap_err().error_type,
            ErrorType::Internal
        );
        assert_eq!(
            MattermostExecutor::new(services.clone())
                .execute(&message(), &action("mattermost"))
                .await
                .unwrap_err()
                .error_type,
            ErrorType::Internal
        );
        assert_eq!(
            BadgeDbExecutor::new(services)
                .execute(&message(), &action("db"))
                .await
                .unwrap_err()
                .error_type,
            ErrorType::Internal
        );
    }

    #[tokio::test]
    async fn confluence_provider_rejects_a_stopped_settings_service() {
        let services = manager();
        let settings = Arc::new(
            ConfluenceSettingsService::new(RuntimeConfig::default().adapters.confluence, "test")
                .unwrap(),
        );
        services.register(settings).await.unwrap();
        let provider = ConfluenceProvider::new("confluence", DEFAULT_WORKFLOW_NAME, services);
        assert_eq!(
            provider.start().await.unwrap_err().error_type,
            ErrorType::ServiceUnavailable
        );
    }

    #[tokio::test]
    async fn mattermost_executor_validates_templates_before_sending() {
        let services = manager();
        let mattermost = Arc::new(
            MattermostService::new(RuntimeConfig::default().adapters.mattermost, "test").unwrap(),
        );
        services.register(mattermost).await.unwrap();
        let executor = MattermostExecutor::new(services);

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
        let services = manager();
        let database = Arc::new(BadgeDbService::new(
            RuntimeConfig::default().adapters.badgedb,
            "test",
        ));
        database.start().await.unwrap();
        services.register(database.clone()).await.unwrap();
        let executor = BadgeDbExecutor::new(services);

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

    #[tokio::test]
    async fn wrong_service_fixture_implements_full_service_contract() {
        let service = NamedService {
            name: "fixture",
            running: AtomicBool::new(false),
        };
        assert_eq!(service.service_type(), "wrong");
        assert_eq!(service.workflow(), DEFAULT_WORKFLOW_NAME);
        assert!(service.as_any().is::<NamedService>());
        service.start().await.unwrap();
        assert!(service.is_running().await);
        service.stop().await.unwrap();
        assert!(!service.is_running().await);

        let websocket =
            WebSocketService::new(RuntimeConfig::default().inputs.websocket, "test-version");
        assert!(websocket.as_any().is::<WebSocketService>());
    }
}
