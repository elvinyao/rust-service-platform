//! Rule-driven workflow wiring for the reference runtime.

use std::{collections::BTreeMap, path::Path, sync::Arc};

use async_trait::async_trait;
use serde::Serialize;
use service_platform::{
    config::RuntimeConfig,
    errors::{AppError, ErrorType, Result},
    executor::{Executor, HttpExecutor, LogExecutor},
    pipeline::{Engine, EngineSnapshot},
    ruleengine::{DEFAULT_WORKFLOW_NAME, EngineConfig, Message, RuleProvider, YamlProvider},
};

use crate::{
    adapters::{BadgeDbExecutor, ConfluenceProvider, MattermostExecutor},
    manager::{ServiceManager, Workflow},
};

#[derive(Debug, Clone, Serialize)]
pub struct EngineAdminSnapshot {
    pub name: String,
    #[serde(flatten)]
    pub engine: EngineSnapshot,
}

pub struct WorkflowEngine {
    name: String,
    engine: Arc<Engine>,
}

impl WorkflowEngine {
    pub async fn new(
        services: Arc<ServiceManager>,
        config_path: impl AsRef<Path>,
        rules_path: impl AsRef<Path>,
        runtime: &RuntimeConfig,
    ) -> Result<Arc<Self>> {
        let config = EngineConfig::load(config_path)?;
        let config = filter_engine_config(config, runtime)?;
        validate_adapter_dependencies(&config, runtime)?;

        let mut providers: Vec<Arc<dyn RuleProvider>> = vec![Arc::new(YamlProvider::new(
            "yaml",
            rules_path.as_ref(),
            DEFAULT_WORKFLOW_NAME,
        ))];
        if runtime.adapters.confluence.enabled && engine_uses_provider(&config, "confluence") {
            providers.push(Arc::new(ConfluenceProvider::new(
                "confluence",
                DEFAULT_WORKFLOW_NAME,
                services.clone(),
            )));
        }

        let mut executors: Vec<Arc<dyn Executor>> =
            vec![Arc::new(LogExecutor), Arc::new(HttpExecutor::new())];
        if runtime.adapters.badgedb.enabled {
            executors.push(Arc::new(BadgeDbExecutor::new(services.clone())));
        }
        if runtime.adapters.mattermost.enabled {
            executors.push(Arc::new(MattermostExecutor::new(services)));
        }

        let engine = Arc::new(Engine::new(config, providers, executors).await?);
        engine.start().await?;
        Ok(Arc::new(Self {
            name: DEFAULT_WORKFLOW_NAME.to_owned(),
            engine,
        }))
    }

    pub async fn admin_snapshot(&self) -> EngineAdminSnapshot {
        EngineAdminSnapshot {
            name: self.name.clone(),
            engine: self.engine.snapshot().await,
        }
    }
}

fn filter_engine_config(mut config: EngineConfig, runtime: &RuntimeConfig) -> Result<EngineConfig> {
    let enabled = BTreeMap::from([
        ("yaml", true),
        ("confluence", runtime.adapters.confluence.enabled),
    ]);
    for workflow in &mut config.workflows {
        workflow
            .providers
            .retain(|provider| enabled.get(provider.as_str()).copied().unwrap_or(true));
        workflow
            .pipeline_order
            .retain(|provider| enabled.get(provider.as_str()).copied().unwrap_or(true));
        if workflow.providers.is_empty() {
            return invalid(format!(
                "workflow {:?} has no enabled providers after applying runtime config",
                workflow.name
            ));
        }
        if workflow.pipeline_order.is_empty() {
            workflow.pipeline_order.clone_from(&workflow.providers);
        }
    }
    config.validate()?;
    Ok(config)
}

fn validate_adapter_dependencies(config: &EngineConfig, runtime: &RuntimeConfig) -> Result<()> {
    if engine_uses_provider(config, "confluence")
        && runtime.adapters.confluence.enabled
        && !runtime.adapters.mattermost.enabled
    {
        return invalid("confluence provider requires the mattermost executor to be enabled");
    }
    Ok(())
}

fn engine_uses_provider(config: &EngineConfig, provider: &str) -> bool {
    config
        .workflows
        .iter()
        .any(|workflow| workflow.providers.iter().any(|name| name == provider))
}

#[async_trait]
impl Workflow for WorkflowEngine {
    fn name(&self) -> &str {
        &self.name
    }

    async fn process_message(&self, message: &Message) -> Result<()> {
        let outcome = self.engine.process_outcome(&self.name, message).await?;
        if outcome.plan.actions.is_empty() {
            tracing::debug!(
                message_type = %message.kind,
                message_id = %message.id,
                "No matching rules"
            );
            return Ok(());
        }
        tracing::info!(
            workflow = %self.name,
            rules = outcome.plan.rules.len(),
            actions = outcome.plan.actions.len(),
            "Workflow matched rules and actions"
        );
        if outcome.failures.is_empty() {
            Ok(())
        } else {
            Err(AppError::new(
                ErrorType::PartialFailure,
                format!(
                    "workflow execution had partial failures: {}",
                    outcome
                        .failures
                        .iter()
                        .map(|failure| format!(
                            "action={} executor={}: {}",
                            failure.action_id, failure.executor, failure.error
                        ))
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
            ))
        }
    }

    async fn stop(&self) -> Result<()> {
        self.engine.stop().await
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(AppError::new(ErrorType::InvalidInput, message))
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use service_platform::{
        config::RuntimeConfig,
        health::HealthManager,
        ruleengine::{CompositionMode, DEFAULT_WORKFLOW_NAME},
    };

    use super::*;

    #[tokio::test]
    async fn default_workflow_uses_yaml_and_core_executors() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let runtime = RuntimeConfig::default();
        let services = ServiceManager::new("test", HealthManager::new(Duration::default(), "test"));
        let workflow = WorkflowEngine::new(
            services,
            root.join("config/rule-engine.yaml"),
            root.join("config/workflow-rules.yaml"),
            &runtime,
        )
        .await
        .unwrap();
        let snapshot = workflow.admin_snapshot().await;
        assert_eq!(snapshot.name, DEFAULT_WORKFLOW_NAME);
        assert_eq!(snapshot.engine.executors, vec!["http", "log"]);
        assert_eq!(
            snapshot.engine.composer.config.workflows[0].mode,
            CompositionMode::Single
        );
        workflow.stop().await.unwrap();
    }

    #[tokio::test]
    async fn disabled_provider_is_removed_from_or_policy() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("engine.yaml");
        fs::write(
            &config_path,
            "workflows:\n  - name: WorkflowEngine\n    providers: [yaml, confluence]\n    mode: or\n",
        )
        .unwrap();
        let config = EngineConfig::load(config_path).unwrap();
        let filtered = filter_engine_config(config, &RuntimeConfig::default()).unwrap();
        assert_eq!(filtered.workflows[0].providers, vec!["yaml"]);
    }

    #[test]
    fn validate_adapter_dependencies_checks_confluence_and_mattermost_coupling() {
        let mut config = EngineConfig::default();
        config.workflows[0].providers = vec!["confluence".to_owned()];

        let mut runtime = RuntimeConfig::default();
        runtime.adapters.confluence.enabled = true;
        runtime.adapters.mattermost.enabled = false;

        assert!(validate_adapter_dependencies(&config, &runtime).is_err());

        runtime.adapters.mattermost.enabled = true;
        assert!(validate_adapter_dependencies(&config, &runtime).is_ok());
    }

    #[test]
    fn filter_engine_config_rejects_empty_providers() {
        let mut config = EngineConfig::default();
        config.workflows[0].providers = vec!["confluence".to_owned()];

        let mut runtime = RuntimeConfig::default();
        runtime.adapters.confluence.enabled = false;

        assert!(filter_engine_config(config, &runtime).is_err());
    }
}
