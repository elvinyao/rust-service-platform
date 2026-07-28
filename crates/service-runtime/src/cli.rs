//! Command-line startup and shutdown orchestration for the reference runtime.

use std::{env, future::Future, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use service_platform::{
    config::RuntimeConfig,
    logger::{self, LoggerConfig},
};
use tokio::time::timeout;

use crate::app::App;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimePaths {
    runtime: PathBuf,
    engine: PathBuf,
    rules: PathBuf,
}

impl RuntimePaths {
    fn from_env() -> Self {
        Self::from_lookup(|key| env::var(key).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        Self {
            runtime: path_or(
                lookup("RUNTIME_CONFIG"),
                PathBuf::from("config/runtime.yaml"),
            ),
            engine: path_or(
                lookup("RULE_ENGINE_CONFIG"),
                PathBuf::from("config/rule-engine.yaml"),
            ),
            rules: path_or(
                lookup("WORKFLOW_RULES"),
                PathBuf::from("config/workflow-rules.yaml"),
            ),
        }
    }
}

fn path_or(value: Option<String>, fallback: PathBuf) -> PathBuf {
    value
        .filter(|value| !value.is_empty())
        .map_or(fallback, PathBuf::from)
}

/// Runs the reference service until an operating-system shutdown signal arrives.
pub async fn run() -> Result<()> {
    let logger_config = LoggerConfig::from_env().map_err(anyhow::Error::msg)?;
    logger::init(&logger_config).map_err(anyhow::Error::msg)?;
    run_with_shutdown(RuntimePaths::from_env(), shutdown_signal()).await
}

async fn run_with_shutdown(paths: RuntimePaths, shutdown: impl Future<Output = ()>) -> Result<()> {
    let config = RuntimeConfig::load(&paths.runtime)
        .with_context(|| format!("load runtime config {}", paths.runtime.display()))?;
    let app = App::new("service-workflow", config, paths.engine, paths.rules);
    app.start().await.context("start application")?;

    shutdown.await;
    timeout(Duration::from_secs(30), app.stop())
        .await
        .context("application shutdown timed out")?
        .context("stop application")
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut quit = signal(SignalKind::quit()).expect("install SIGQUIT handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                let _ = result;
            }
            _ = terminate.recv() => {}
            _ = quit.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_paths_use_defaults_overrides_and_ignore_blanks() {
        assert_eq!(
            RuntimePaths::from_lookup(|_| None),
            RuntimePaths {
                runtime: PathBuf::from("config/runtime.yaml"),
                engine: PathBuf::from("config/rule-engine.yaml"),
                rules: PathBuf::from("config/workflow-rules.yaml"),
            }
        );
        let paths = RuntimePaths::from_lookup(|key| {
            Some(
                match key {
                    "RUNTIME_CONFIG" => "runtime.yml",
                    "RULE_ENGINE_CONFIG" => "engine.yml",
                    "WORKFLOW_RULES" => "rules.yml",
                    _ => "",
                }
                .to_owned(),
            )
        });
        assert_eq!(paths.runtime, PathBuf::from("runtime.yml"));
        assert_eq!(paths.engine, PathBuf::from("engine.yml"));
        assert_eq!(paths.rules, PathBuf::from("rules.yml"));
        assert_eq!(
            path_or(Some(String::new()), PathBuf::from("fallback")),
            PathBuf::from("fallback")
        );
    }

    #[tokio::test]
    async fn runtime_runner_starts_and_stops_with_repository_rules() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let directory = tempfile::tempdir().unwrap();
        let runtime_path = directory.path().join("runtime.yaml");
        let mut config = RuntimeConfig::default();
        config.admin.address = "127.0.0.1:0".to_owned();
        config.inputs.websocket.enabled = false;
        std::fs::write(&runtime_path, serde_yaml::to_string(&config).unwrap()).unwrap();

        run_with_shutdown(
            RuntimePaths {
                runtime: runtime_path,
                engine: root.join("config/rule-engine.yaml"),
                rules: root.join("config/workflow-rules.yaml"),
            },
            async {},
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn runtime_runner_adds_config_path_context() {
        let missing = PathBuf::from("/definitely/missing/runtime.yaml");
        let error = run_with_shutdown(
            RuntimePaths {
                runtime: missing.clone(),
                engine: PathBuf::new(),
                rules: PathBuf::new(),
            },
            async {},
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("load runtime config {}", missing.display()))
        );
    }

    #[test]
    fn runtime_paths_from_env_returns_paths() {
        let paths = RuntimePaths::from_env();
        assert_eq!(paths.runtime, PathBuf::from("config/runtime.yaml"));
    }
}
