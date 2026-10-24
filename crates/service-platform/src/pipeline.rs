//! Provider-to-action orchestration with validation and lifecycle rollback.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::Duration,
};

use serde::Serialize;
use tokio::{
    sync::{Mutex, RwLock},
    time::{Instant, timeout_at},
};

use crate::{
    errors::{AppError, ErrorType, Result},
    executor::Executor,
    ruleengine::{
        Composer, ComposerSnapshot, EngineConfig, ExecutionPlan, Message, RuleProvider,
        validate_provider_rule_set,
    },
};

const LIFECYCLE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ActionFailure {
    pub action_id: String,
    pub executor: String,
    pub error: String,
}

/// Typed details for an execution plan whose independent actions partially failed.
#[derive(Debug, Clone)]
pub struct ExecutionError {
    pub plan: ExecutionPlan,
    pub failures: Vec<ActionFailure>,
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("pipeline execution failed: ")?;
        for (index, failure) in self.failures.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(
                formatter,
                "action={} executor={}: {}",
                failure.action_id, failure.executor, failure.error
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for ExecutionError {}

#[derive(Debug)]
pub struct ProcessOutcome {
    pub plan: ExecutionPlan,
    pub failures: Vec<ActionFailure>,
}

impl ProcessOutcome {
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EngineSnapshot {
    pub composer: ComposerSnapshot,
    pub executors: Vec<String>,
}

#[derive(Default)]
struct EngineState {
    started: bool,
    active_providers: Vec<String>,
    active_executors: Vec<String>,
}

/// Lifecycle-managed rule pipeline.
pub struct Engine {
    config: EngineConfig,
    composer: Composer,
    providers: BTreeMap<String, Arc<dyn RuleProvider>>,
    executors: BTreeMap<String, Arc<dyn Executor>>,
    state: Mutex<EngineState>,
    process_gate: RwLock<()>,
}

impl Engine {
    #[allow(clippy::unused_async)]
    pub async fn new(
        config: EngineConfig,
        providers: Vec<Arc<dyn RuleProvider>>,
        executors: Vec<Arc<dyn Executor>>,
    ) -> Result<Self> {
        config.validate().map_err(|error| {
            AppError::wrap(ErrorType::InvalidInput, "validate engine config", error)
        })?;

        let mut provider_map = BTreeMap::new();
        for provider in providers {
            let name = provider.name().to_owned();
            if name.is_empty() {
                return invalid("rule provider name is empty");
            }
            if provider_map.insert(name.clone(), provider).is_some() {
                return invalid(format!("rule provider {name} is registered more than once"));
            }
        }
        validate_configured_providers(&config, &provider_map)?;

        let mut executor_map = BTreeMap::new();
        for executor in executors {
            let name = executor.executor_type().to_owned();
            if name.is_empty() {
                return invalid("executor type is empty");
            }
            if executor_map.insert(name.clone(), executor).is_some() {
                return invalid(format!("executor {name} is registered more than once"));
            }
        }
        let composer = Composer::new(config.clone(), provider_map.clone());
        Ok(Self {
            config,
            composer,
            providers: provider_map,
            executors: executor_map,
            state: Mutex::new(EngineState::default()),
            process_gate: RwLock::new(()),
        })
    }

    /// Starts providers, validates all snapshots, then starts executors.
    pub async fn start(&self) -> Result<()> {
        let _gate = self.process_gate.write().await;
        let mut state = self.state.lock().await;
        if state.started {
            return Ok(());
        }
        if !state.active_providers.is_empty() || !state.active_executors.is_empty() {
            return invalid("pipeline has components pending cleanup");
        }

        for (name, provider) in &self.providers {
            state.active_providers.push(name.clone());
            if let Err(error) = provider.start().await {
                let start_error = AppError::wrap(
                    ErrorType::ServiceUnavailable,
                    format!("start rule provider {name}"),
                    error,
                );
                return Err(self.rollback_locked(&mut state, start_error).await);
            }
        }
        if let Err(error) = self.validate_provider_executors().await {
            return Err(self.rollback_locked(&mut state, error).await);
        }

        for (name, executor) in &self.executors {
            state.active_executors.push(name.clone());
            if let Err(error) = executor.start().await {
                let start_error = AppError::wrap(
                    ErrorType::ServiceUnavailable,
                    format!("start executor {name}"),
                    error,
                );
                return Err(self.rollback_locked(&mut state, start_error).await);
            }
        }
        state.started = true;
        Ok(())
    }

    /// Prevents new processing, waits for active processing, and stops in reverse order.
    pub async fn stop(&self) -> Result<()> {
        let _gate = self.process_gate.write().await;
        let mut state = self.state.lock().await;
        if !state.started && state.active_providers.is_empty() && state.active_executors.is_empty()
        {
            return Ok(());
        }
        state.started = false;
        self.stop_components_locked(&mut state).await
    }

    async fn rollback_locked(&self, state: &mut EngineState, start_error: AppError) -> AppError {
        match self.stop_components_locked(state).await {
            Ok(()) => start_error,
            Err(cleanup_error) => AppError::new(
                start_error.error_type,
                format!("{start_error}; roll back pipeline startup: {cleanup_error}"),
            ),
        }
    }

    async fn stop_components_locked(&self, state: &mut EngineState) -> Result<()> {
        let mut failures = Vec::new();
        let deadline = Instant::now() + LIFECYCLE_CLEANUP_TIMEOUT;
        let mut retained_executors = BTreeSet::new();
        for name in state.active_executors.iter().rev() {
            if let Some(executor) = self.executors.get(name) {
                match timeout_at(deadline, executor.stop()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        retained_executors.insert(name.clone());
                        failures.push(format!("stop executor {name}: {error}"));
                    }
                    Err(_) => {
                        retained_executors.insert(name.clone());
                        failures.push(format!("stop executor {name}: cleanup timed out"));
                    }
                }
            }
        }
        state
            .active_executors
            .retain(|name| retained_executors.contains(name));

        let mut retained_providers = BTreeSet::new();
        for name in state.active_providers.iter().rev() {
            if let Some(provider) = self.providers.get(name) {
                match timeout_at(deadline, provider.stop()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        retained_providers.insert(name.clone());
                        failures.push(format!("stop rule provider {name}: {error}"));
                    }
                    Err(_) => {
                        retained_providers.insert(name.clone());
                        failures.push(format!("stop rule provider {name}: cleanup timed out"));
                    }
                }
            }
        }
        state
            .active_providers
            .retain(|name| retained_providers.contains(name));

        if failures.is_empty() {
            Ok(())
        } else {
            Err(AppError::new(
                ErrorType::ServiceUnavailable,
                failures.join("; "),
            ))
        }
    }

    /// Builds a plan, attempts every selected action, and returns typed partial failures.
    pub async fn process(&self, workflow: &str, message: &Message) -> Result<ExecutionPlan> {
        let outcome = self.process_outcome(workflow, message).await?;
        if outcome.is_success() {
            return Ok(outcome.plan);
        }
        let execution_error = ExecutionError {
            plan: outcome.plan,
            failures: outcome.failures,
        };
        Err(AppError::wrap(
            ErrorType::PartialFailure,
            "one or more pipeline actions failed",
            execution_error,
        ))
    }

    /// Returns both the execution plan and all action failures without collapsing them.
    pub async fn process_outcome(
        &self,
        workflow: &str,
        message: &Message,
    ) -> Result<ProcessOutcome> {
        let _gate = self.process_gate.read().await;
        if !self.state.lock().await.started {
            return Err(AppError::new(
                ErrorType::ServiceUnavailable,
                "pipeline is not started",
            ));
        }
        let plan = self
            .composer
            .build_execution_plan(workflow, message)
            .await?;
        let mut failures = Vec::new();
        for action in &plan.actions {
            let Some(executor) = self.executors.get(&action.executor) else {
                failures.push(ActionFailure {
                    action_id: action.id.clone(),
                    executor: action.executor.clone(),
                    error: "executor is not registered".to_owned(),
                });
                continue;
            };
            if let Err(error) = executor.execute(message, action).await {
                failures.push(ActionFailure {
                    action_id: action.id.clone(),
                    executor: action.executor.clone(),
                    error: error.to_string(),
                });
            }
        }
        Ok(ProcessOutcome { plan, failures })
    }

    pub async fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            composer: self.composer.snapshot().await,
            executors: self.executors.keys().cloned().collect(),
        }
    }

    pub async fn is_started(&self) -> bool {
        self.state.lock().await.started
    }

    async fn validate_provider_executors(&self) -> Result<()> {
        for (provider_name, provider) in &self.providers {
            let snapshot = provider.snapshot().await;
            validate_provider_rule_set(&self.config, provider_name, &snapshot).map_err(
                |error| {
                    AppError::wrap(
                        ErrorType::InvalidInput,
                        format!("provider {provider_name} published invalid rules"),
                        error,
                    )
                },
            )?;
            for rule in snapshot.rules.iter().filter(|rule| rule.enabled) {
                for action in &rule.actions {
                    if !self.executors.contains_key(&action.executor) {
                        return invalid(format!(
                            "provider {provider_name} rule {} references unregistered executor {}",
                            rule.id, action.executor
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_configured_providers(
    config: &EngineConfig,
    providers: &BTreeMap<String, Arc<dyn RuleProvider>>,
) -> Result<()> {
    for workflow in &config.workflows {
        for provider in &workflow.providers {
            if !providers.contains_key(provider) {
                return invalid(format!(
                    "workflow {} provider {provider} is not registered",
                    workflow.name
                ));
            }
        }
    }
    Ok(())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(AppError::new(ErrorType::InvalidInput, message))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use chrono::Utc;
    use tokio::{
        sync::{Mutex as TokioMutex, Notify},
        time::{Duration, timeout},
    };

    use crate::{
        executor::LogExecutor,
        ruleengine::{Action, DEFAULT_WORKFLOW_NAME, Rule, RuleSet, StaticProvider},
    };

    use super::*;

    struct BlockingExecutor {
        entered: Notify,
        release: Notify,
        stopped: AtomicBool,
    }

    #[async_trait]
    impl Executor for BlockingExecutor {
        fn executor_type(&self) -> &'static str {
            "blocking"
        }

        async fn stop(&self) -> Result<()> {
            self.stopped.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn execute(&self, _message: &Message, _action: &Action) -> Result<()> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    struct FailingExecutor;

    #[async_trait]
    impl Executor for FailingExecutor {
        fn executor_type(&self) -> &'static str {
            "failing"
        }

        async fn execute(&self, _message: &Message, _action: &Action) -> Result<()> {
            Err(AppError::new(ErrorType::Internal, "action failed"))
        }
    }

    struct RecordingExecutor {
        name: String,
        events: Arc<TokioMutex<Vec<String>>>,
        fail_start: bool,
        stop_failures: AtomicUsize,
    }

    #[async_trait]
    impl Executor for RecordingExecutor {
        fn executor_type(&self) -> &str {
            &self.name
        }

        async fn start(&self) -> Result<()> {
            self.events
                .lock()
                .await
                .push(format!("start executor {}", self.name));
            if self.fail_start {
                return Err(AppError::new(ErrorType::Internal, "executor start failed"));
            }
            Ok(())
        }

        async fn stop(&self) -> Result<()> {
            self.events
                .lock()
                .await
                .push(format!("stop executor {}", self.name));
            if self
                .stop_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(AppError::new(ErrorType::Internal, "executor stop failed"));
            }
            Ok(())
        }

        async fn execute(&self, _message: &Message, _action: &Action) -> Result<()> {
            Ok(())
        }
    }

    struct RecordingProvider {
        name: String,
        events: Arc<TokioMutex<Vec<String>>>,
        fail_start: bool,
    }

    #[async_trait]
    impl RuleProvider for RecordingProvider {
        fn name(&self) -> &str {
            &self.name
        }

        async fn start(&self) -> Result<()> {
            self.events
                .lock()
                .await
                .push(format!("start {}", self.name));
            if self.fail_start {
                return Err(AppError::new(ErrorType::Internal, "load failed"));
            }
            Ok(())
        }

        async fn stop(&self) -> Result<()> {
            self.events.lock().await.push(format!("stop {}", self.name));
            Ok(())
        }

        async fn snapshot(&self) -> RuleSet {
            RuleSet::empty(&self.name)
        }
    }

    fn provider_with_action(executor: &str) -> Arc<dyn RuleProvider> {
        Arc::new(StaticProvider::new(
            "yaml",
            RuleSet {
                source: "yaml".to_owned(),
                version: "1".to_owned(),
                loaded_at: Utc::now(),
                rules: vec![Rule {
                    id: "rule".to_owned(),
                    workflow: DEFAULT_WORKFLOW_NAME.to_owned(),
                    source: "yaml".to_owned(),
                    enabled: true,
                    priority: 1,
                    conditions: Vec::new(),
                    actions: vec![Action {
                        executor: executor.to_owned(),
                        ..Action::default()
                    }],
                }],
                last_error: String::new(),
            },
        ))
    }

    #[tokio::test]
    async fn startup_rolls_back_providers_in_reverse_order() {
        let events = Arc::new(TokioMutex::new(Vec::new()));
        let providers: Vec<Arc<dyn RuleProvider>> = vec![
            Arc::new(RecordingProvider {
                name: "a".to_owned(),
                events: events.clone(),
                fail_start: false,
            }),
            Arc::new(RecordingProvider {
                name: "b".to_owned(),
                events: events.clone(),
                fail_start: true,
            }),
        ];
        let mut config = EngineConfig::default();
        config.workflows[0].mode = crate::ruleengine::CompositionMode::Or;
        config.workflows[0].providers = vec!["a".to_owned(), "b".to_owned()];
        let engine = Engine::new(config, providers, Vec::new()).await.unwrap();
        assert!(engine.start().await.is_err());
        assert_eq!(
            *events.lock().await,
            vec!["start a", "start b", "stop b", "stop a"]
        );
    }

    #[tokio::test]
    async fn construction_rejects_invalid_provider_and_executor_registration() {
        let missing = Engine::new(EngineConfig::default(), Vec::new(), Vec::new())
            .await
            .err()
            .unwrap();
        assert!(missing.to_string().contains("not registered"));

        let events = Arc::new(TokioMutex::new(Vec::new()));
        let empty_provider: Arc<dyn RuleProvider> = Arc::new(RecordingProvider {
            name: String::new(),
            events: events.clone(),
            fail_start: false,
        });
        assert!(
            Engine::new(EngineConfig::default(), vec![empty_provider], Vec::new())
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("name is empty")
        );

        let providers: Vec<Arc<dyn RuleProvider>> = vec![
            Arc::new(RecordingProvider {
                name: "yaml".to_owned(),
                events: events.clone(),
                fail_start: false,
            }),
            Arc::new(RecordingProvider {
                name: "yaml".to_owned(),
                events,
                fail_start: false,
            }),
        ];
        assert!(
            Engine::new(EngineConfig::default(), providers, Vec::new())
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("more than once")
        );

        assert!(
            Engine::new(
                EngineConfig::default(),
                vec![Arc::new(StaticProvider::new(
                    "yaml",
                    RuleSet::empty("yaml")
                ))],
                vec![Arc::new(LogExecutor), Arc::new(LogExecutor)],
            )
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("more than once")
        );
    }

    #[tokio::test]
    async fn executor_start_failure_rolls_back_every_component_in_reverse_order() {
        let events = Arc::new(TokioMutex::new(Vec::new()));
        let provider: Arc<dyn RuleProvider> = Arc::new(RecordingProvider {
            name: "yaml".to_owned(),
            events: events.clone(),
            fail_start: false,
        });
        let executors: Vec<Arc<dyn Executor>> = vec![
            Arc::new(RecordingExecutor {
                name: "a".to_owned(),
                events: events.clone(),
                fail_start: false,
                stop_failures: AtomicUsize::new(0),
            }),
            Arc::new(RecordingExecutor {
                name: "b".to_owned(),
                events: events.clone(),
                fail_start: true,
                stop_failures: AtomicUsize::new(0),
            }),
        ];
        let engine = Engine::new(EngineConfig::default(), vec![provider], executors)
            .await
            .unwrap();

        assert!(engine.start().await.is_err());
        assert_eq!(
            *events.lock().await,
            vec![
                "start yaml",
                "start executor a",
                "start executor b",
                "stop executor b",
                "stop executor a",
                "stop yaml",
            ]
        );
        assert!(!engine.is_started().await);
    }

    #[tokio::test]
    async fn cleanup_failure_is_retained_for_retry_before_restart() {
        let events = Arc::new(TokioMutex::new(Vec::new()));
        let executor = Arc::new(RecordingExecutor {
            name: "cleanup".to_owned(),
            events,
            fail_start: false,
            stop_failures: AtomicUsize::new(1),
        });
        let engine = Engine::new(
            EngineConfig::default(),
            vec![Arc::new(StaticProvider::new(
                "yaml",
                RuleSet::empty("yaml"),
            ))],
            vec![executor],
        )
        .await
        .unwrap();

        engine.start().await.unwrap();
        assert!(engine.stop().await.is_err());
        assert!(!engine.is_started().await);
        assert!(
            engine
                .start()
                .await
                .unwrap_err()
                .to_string()
                .contains("pending cleanup")
        );
        engine.stop().await.unwrap();
        engine.start().await.unwrap();
        engine.start().await.unwrap();
        engine.stop().await.unwrap();
        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn empty_plan_snapshot_and_restart_cycle_are_stable() {
        let engine = Engine::new(
            EngineConfig::default(),
            vec![Arc::new(StaticProvider::new(
                "yaml",
                RuleSet::empty("yaml"),
            ))],
            vec![Arc::new(LogExecutor)],
        )
        .await
        .unwrap();
        assert!(
            engine
                .process(DEFAULT_WORKFLOW_NAME, &Message::default())
                .await
                .is_err()
        );

        for _ in 0..2 {
            engine.start().await.unwrap();
            let outcome = engine
                .process_outcome(DEFAULT_WORKFLOW_NAME, &Message::default())
                .await
                .unwrap();
            assert!(outcome.is_success());
            assert!(outcome.plan.rules.is_empty());
            assert!(outcome.plan.actions.is_empty());
            let snapshot = engine.snapshot().await;
            assert_eq!(snapshot.executors, vec!["log"]);
            assert!(snapshot.composer.providers.contains_key("yaml"));
            engine.stop().await.unwrap();
        }
    }

    #[tokio::test]
    async fn stop_waits_for_in_flight_processing() {
        let executor = Arc::new(BlockingExecutor {
            entered: Notify::new(),
            release: Notify::new(),
            stopped: AtomicBool::new(false),
        });
        let engine = Arc::new(
            Engine::new(
                EngineConfig::default(),
                vec![provider_with_action("blocking")],
                vec![executor.clone()],
            )
            .await
            .unwrap(),
        );
        engine.start().await.unwrap();

        let process_engine = engine.clone();
        let process = tokio::spawn(async move {
            process_engine
                .process(DEFAULT_WORKFLOW_NAME, &Message::default())
                .await
        });
        executor.entered.notified().await;
        let stop_engine = engine.clone();
        let mut stop = tokio::spawn(async move { stop_engine.stop().await });
        assert!(timeout(Duration::from_millis(30), &mut stop).await.is_err());
        executor.release.notify_one();
        process.await.unwrap().unwrap();
        stop.await.unwrap().unwrap();
        assert!(executor.stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn startup_rejects_rules_with_unknown_executors() {
        let engine = Engine::new(
            EngineConfig::default(),
            vec![provider_with_action("missing")],
            vec![Arc::new(LogExecutor)],
        )
        .await
        .unwrap();
        assert!(
            engine
                .start()
                .await
                .unwrap_err()
                .to_string()
                .contains("unregistered executor")
        );
    }

    #[tokio::test]
    async fn process_returns_typed_partial_failure_with_execution_plan() {
        let engine = Engine::new(
            EngineConfig::default(),
            vec![provider_with_action("failing")],
            vec![Arc::new(FailingExecutor)],
        )
        .await
        .unwrap();
        engine.start().await.unwrap();

        let error = engine
            .process(DEFAULT_WORKFLOW_NAME, &Message::default())
            .await
            .unwrap_err();

        assert_eq!(error.error_type, ErrorType::PartialFailure);
        let details = error
            .source
            .as_deref()
            .and_then(|source| source.downcast_ref::<ExecutionError>())
            .unwrap();
        assert_eq!(details.plan.actions.len(), 1);
        assert_eq!(details.failures.len(), 1);
        assert_eq!(details.failures[0].executor, "failing");
        engine.stop().await.unwrap();
    }
}
