//! Rule model, strict YAML providers, matching, and provider composition.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;

use crate::errors::{AppError, ErrorType, Result};

pub const DEFAULT_WORKFLOW_NAME: &str = "WorkflowEngine";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Condition {
    pub field: String,
    pub op: ConditionOperator,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConditionOperator {
    Eq,
    Contains,
    Regex,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Action {
    pub id: String,
    pub executor: String,
    pub priority: i32,
    pub params: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Rule {
    pub id: String,
    pub workflow: String,
    pub source: String,
    pub enabled: bool,
    pub priority: i32,
    pub conditions: Vec<Condition>,
    pub actions: Vec<Action>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuleSet {
    pub source: String,
    pub version: String,
    pub loaded_at: DateTime<Utc>,
    pub rules: Vec<Rule>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_error: String,
}

impl RuleSet {
    pub fn empty(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            version: "0".to_owned(),
            loaded_at: Utc::now(),
            rules: Vec::new(),
            last_error: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Message {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub content: String,
    pub user_id: String,
    pub timestamp: DateTime<Utc>,
    pub metadata: BTreeMap<String, Value>,
}

impl Default for Message {
    fn default() -> Self {
        Self {
            id: String::new(),
            kind: String::new(),
            content: String::new(),
            user_id: String::new(),
            timestamp: Utc::now(),
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompositionMode {
    Single,
    Or,
    And,
    Pipeline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActionOrder {
    Priority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionMergeConfig {
    pub dedup: bool,
    pub order: ActionOrder,
}

impl Default for ActionMergeConfig {
    fn default() -> Self {
        Self {
            dedup: true,
            order: ActionOrder::Priority,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowPolicy {
    pub name: String,
    pub providers: Vec<String>,
    pub mode: CompositionMode,
    pub pipeline_order: Vec<String>,
    pub action_merge: ActionMergeConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineConfig {
    pub workflows: Vec<WorkflowPolicy>,
    pub action_merge: ActionMergeConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            workflows: vec![WorkflowPolicy {
                name: DEFAULT_WORKFLOW_NAME.to_owned(),
                providers: vec!["yaml".to_owned()],
                mode: CompositionMode::Single,
                pipeline_order: vec!["yaml".to_owned()],
                action_merge: ActionMergeConfig::default(),
            }],
            action_merge: ActionMergeConfig::default(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawEngineConfig {
    workflows: Option<Vec<RawWorkflowPolicy>>,
    action_merge: Option<RawActionMergeConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawWorkflowPolicy {
    name: Option<String>,
    providers: Option<Vec<String>>,
    mode: Option<CompositionMode>,
    pipeline_order: Option<Vec<String>>,
    action_merge: Option<RawActionMergeConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawActionMergeConfig {
    dedup: Option<bool>,
    order: Option<ActionOrder>,
}

impl EngineConfig {
    /// Loads and normalizes one strict YAML configuration document.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() {
            return Ok(Self::default());
        }
        let data = fs::read_to_string(path).map_err(|error| {
            AppError::wrap(
                ErrorType::InvalidInput,
                format!("read rule engine config {}", path.display()),
                error,
            )
        })?;
        if data.trim().is_empty() {
            return invalid("rule engine config is empty");
        }
        let mut documents = serde_yaml::Deserializer::from_str(&data);
        let first = documents
            .next()
            .ok_or_else(|| AppError::new(ErrorType::InvalidInput, "rule engine config is empty"))?;
        let raw = RawEngineConfig::deserialize(first).map_err(|error| {
            AppError::wrap(ErrorType::InvalidInput, "parse rule engine config", error)
        })?;
        if documents.next().is_some() {
            return invalid("parse rule engine config: multiple YAML documents are not supported");
        }
        let config = Self::from_raw(raw);
        config.validate()?;
        Ok(config)
    }

    fn from_raw(raw: RawEngineConfig) -> Self {
        let global = merge_action_config(ActionMergeConfig::default(), raw.action_merge.as_ref());
        let defaults = Self::default();
        let workflows = match raw.workflows {
            None => defaults.workflows,
            Some(workflows) => workflows
                .into_iter()
                .map(|workflow| {
                    let providers = workflow
                        .providers
                        .unwrap_or_else(|| vec!["yaml".to_owned()]);
                    WorkflowPolicy {
                        name: workflow
                            .name
                            .unwrap_or_else(|| DEFAULT_WORKFLOW_NAME.to_owned()),
                        mode: workflow.mode.unwrap_or(CompositionMode::Single),
                        pipeline_order: workflow
                            .pipeline_order
                            .unwrap_or_else(|| providers.clone()),
                        providers,
                        action_merge: merge_action_config(
                            global.clone(),
                            workflow.action_merge.as_ref(),
                        ),
                    }
                })
                .collect(),
        };
        Self {
            workflows,
            action_merge: global,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.workflows.is_empty() {
            return invalid("workflows must contain at least one policy");
        }
        let mut workflow_names = HashSet::new();
        for (index, workflow) in self.workflows.iter().enumerate() {
            let prefix = format!("workflows[{index}]");
            if workflow.name.is_empty() {
                return invalid(format!("{prefix}.name is required"));
            }
            if !workflow_names.insert(&workflow.name) {
                return invalid(format!("{prefix}.name {:?} is duplicated", workflow.name));
            }
            validate_unique_names(&format!("{prefix}.providers"), &workflow.providers)?;
            if workflow.mode == CompositionMode::Single && workflow.providers.len() != 1 {
                return invalid(format!(
                    "{prefix}.providers must contain exactly one value for mode \"single\""
                ));
            }
            if workflow.mode == CompositionMode::Pipeline {
                validate_unique_names(
                    &format!("{prefix}.pipeline_order"),
                    &workflow.pipeline_order,
                )?;
                let providers: BTreeSet<_> = workflow.providers.iter().collect();
                let order: BTreeSet<_> = workflow.pipeline_order.iter().collect();
                if providers != order || workflow.providers.len() != workflow.pipeline_order.len() {
                    return invalid(format!(
                        "{prefix}.pipeline_order must contain every configured provider"
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn policy(&self, workflow: &str) -> Option<&WorkflowPolicy> {
        self.workflows.iter().find(|policy| policy.name == workflow)
    }
}

fn merge_action_config(
    base: ActionMergeConfig,
    raw: Option<&RawActionMergeConfig>,
) -> ActionMergeConfig {
    let Some(raw) = raw else {
        return base;
    };
    ActionMergeConfig {
        dedup: raw.dedup.unwrap_or(base.dedup),
        order: raw.order.unwrap_or(base.order),
    }
}

fn validate_unique_names(name: &str, values: &[String]) -> Result<()> {
    if values.is_empty() {
        return invalid(format!("{name} must contain at least one value"));
    }
    let mut seen = HashSet::new();
    for value in values {
        if value.is_empty() {
            return invalid(format!("{name} contains an empty value"));
        }
        if !seen.insert(value) {
            return invalid(format!("{name} contains duplicate value {value:?}"));
        }
    }
    Ok(())
}

/// Extension point for static or dynamic rule sources.
#[async_trait]
pub trait RuleProvider: Send + Sync {
    fn name(&self) -> &str;
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
    async fn snapshot(&self) -> RuleSet;
}

/// In-memory provider useful for embedding and tests.
pub struct StaticProvider {
    name: String,
    snapshot: RuleSet,
}

impl StaticProvider {
    pub fn new(name: impl Into<String>, mut snapshot: RuleSet) -> Self {
        let name = name.into();
        snapshot.source.clone_from(&name);
        for rule in &mut snapshot.rules {
            rule.source.clone_from(&name);
        }
        Self { name, snapshot }
    }
}

#[async_trait]
impl RuleProvider for StaticProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<()> {
        validate_rule_set(&self.snapshot)
    }

    async fn snapshot(&self) -> RuleSet {
        self.snapshot.clone()
    }
}

/// Strict YAML-backed rule provider.
pub struct YamlProvider {
    name: String,
    path: PathBuf,
    default_workflow: String,
    snapshot: RwLock<RuleSet>,
}

impl YamlProvider {
    pub fn new(
        name: impl Into<String>,
        path: impl Into<PathBuf>,
        default_workflow: impl Into<String>,
    ) -> Self {
        let name = name.into();
        Self {
            snapshot: RwLock::new(RuleSet::empty(name.clone())),
            name,
            path: path.into(),
            default_workflow: default_workflow.into(),
        }
    }

    fn load(&self) -> Result<RuleSet> {
        if self.path.as_os_str().is_empty() {
            return Ok(RuleSet {
                source: self.name.clone(),
                version: "1".to_owned(),
                loaded_at: Utc::now(),
                rules: Vec::new(),
                last_error: String::new(),
            });
        }
        let data = fs::read_to_string(&self.path).map_err(|error| {
            AppError::wrap(
                ErrorType::InvalidInput,
                format!("read rules file {}", self.path.display()),
                error,
            )
        })?;
        if data.trim().is_empty() {
            return invalid("rules file is empty");
        }
        let mut documents = serde_yaml::Deserializer::from_str(&data);
        let first = documents
            .next()
            .ok_or_else(|| AppError::new(ErrorType::InvalidInput, "rules file is empty"))?;
        let file = RawRulesFile::deserialize(first)
            .map_err(|error| AppError::wrap(ErrorType::InvalidInput, "parse rules file", error))?;
        if documents.next().is_some() {
            return invalid("parse rules file: multiple YAML documents are not supported");
        }

        let mut rules = Vec::with_capacity(file.rules.len());
        for raw in file.rules {
            rules.push(Rule {
                id: raw.id,
                workflow: raw
                    .workflow
                    .unwrap_or_else(|| self.default_workflow.clone()),
                source: self.name.clone(),
                enabled: raw.enabled.unwrap_or(true),
                priority: raw.priority,
                conditions: raw.conditions,
                actions: raw.actions,
            });
        }
        rules.sort_by_key(|rule| rule.priority);
        let snapshot = RuleSet {
            source: self.name.clone(),
            version: file.version.unwrap_or_else(|| "1".to_owned()),
            loaded_at: Utc::now(),
            rules,
            last_error: String::new(),
        };
        validate_rule_set(&snapshot)?;
        Ok(snapshot)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRulesFile {
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    rules: Vec<RawRule>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRule {
    id: String,
    #[serde(default)]
    workflow: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    priority: i32,
    #[serde(default)]
    conditions: Vec<Condition>,
    actions: Vec<Action>,
}

#[async_trait]
impl RuleProvider for YamlProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<()> {
        *self.snapshot.write().await = self.load()?;
        Ok(())
    }

    async fn snapshot(&self) -> RuleSet {
        self.snapshot.read().await.clone()
    }
}

pub fn validate_rule_set(rule_set: &RuleSet) -> Result<()> {
    let mut ids = HashSet::new();
    for (rule_index, rule) in rule_set.rules.iter().enumerate() {
        let prefix = format!("rules[{rule_index}]");
        if rule.id.is_empty() {
            return invalid(format!("{prefix}.id is required"));
        }
        if !ids.insert(&rule.id) {
            return invalid(format!("{prefix}.id {:?} is duplicated", rule.id));
        }
        if rule.actions.is_empty() {
            return invalid(format!("{prefix}.actions is required"));
        }
        for (condition_index, condition) in rule.conditions.iter().enumerate() {
            let condition_prefix = format!("{prefix}.conditions[{condition_index}]");
            if !valid_condition_field(&condition.field) {
                return invalid(format!(
                    "{condition_prefix}.field {:?} is invalid",
                    condition.field
                ));
            }
            if condition.op == ConditionOperator::Regex {
                Regex::new(&normalize_value(&condition.value)).map_err(|error| {
                    AppError::wrap(
                        ErrorType::InvalidInput,
                        format!("{condition_prefix}.value regex is invalid"),
                        error,
                    )
                })?;
            }
        }
        for (action_index, action) in rule.actions.iter().enumerate() {
            if action.executor.is_empty() {
                return invalid(format!(
                    "{prefix}.actions[{action_index}].executor is required"
                ));
            }
        }
    }
    Ok(())
}

pub fn validate_provider_rule_set(
    config: &EngineConfig,
    provider_name: &str,
    rule_set: &RuleSet,
) -> Result<()> {
    validate_rule_set(rule_set)?;
    for (index, rule) in rule_set.rules.iter().enumerate() {
        if rule.workflow.is_empty() {
            continue;
        }
        let policy = config.policy(&rule.workflow).ok_or_else(|| {
            AppError::new(
                ErrorType::InvalidInput,
                format!(
                    "rules[{index}].workflow {:?} has no configured policy",
                    rule.workflow
                ),
            )
        })?;
        if !workflow_uses_provider(policy, provider_name) {
            return invalid(format!(
                "rules[{index}].workflow {:?} cannot use provider {provider_name:?}",
                rule.workflow
            ));
        }
    }
    Ok(())
}

fn workflow_uses_provider(policy: &WorkflowPolicy, provider_name: &str) -> bool {
    if policy.mode == CompositionMode::Single {
        return policy
            .providers
            .first()
            .is_some_and(|name| name == provider_name);
    }
    policy.providers.iter().any(|name| name == provider_name)
}

fn valid_condition_field(field: &str) -> bool {
    if matches!(field, "id" | "type" | "content" | "user_id" | "timestamp") {
        return true;
    }
    field
        .strip_prefix("metadata.")
        .is_some_and(|path| !path.is_empty() && path.split('.').all(|part| !part.is_empty()))
}

pub fn match_rules(rules: &[Rule], message: &Message) -> Vec<Rule> {
    let mut matched: Vec<_> = rules
        .iter()
        .filter(|rule| rule.enabled && match_rule(rule, message))
        .cloned()
        .collect();
    matched.sort_by_key(|rule| rule.priority);
    matched
}

pub fn match_rule(rule: &Rule, message: &Message) -> bool {
    rule.conditions
        .iter()
        .all(|condition| match_condition(condition, message))
}

pub fn match_condition(condition: &Condition, message: &Message) -> bool {
    let Some(actual) = message_field(message, &condition.field) else {
        return false;
    };
    let actual = normalize_value(&actual);
    let expected = normalize_value(&condition.value);
    match condition.op {
        ConditionOperator::Eq => actual == expected,
        ConditionOperator::Contains => actual.contains(&expected),
        ConditionOperator::Regex => {
            Regex::new(&expected).is_ok_and(|expression| expression.is_match(&actual))
        }
    }
}

fn message_field(message: &Message, field: &str) -> Option<Value> {
    match field {
        "id" => Some(Value::String(message.id.clone())),
        "type" => Some(Value::String(message.kind.clone())),
        "content" => Some(Value::String(message.content.clone())),
        "user_id" => Some(Value::String(message.user_id.clone())),
        "timestamp" => Some(Value::String(message.timestamp.to_rfc3339())),
        _ => {
            let path = field.strip_prefix("metadata.")?;
            let first = path.split('.').next()?;
            let mut current = message.metadata.get(first)?;
            for part in path.split('.').skip(1) {
                current = current.as_object()?.get(part)?;
            }
            Some(current.clone())
        }
    }
}

fn normalize_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => "<nil>".to_owned(),
        _ => value.to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionPlan {
    pub workflow: String,
    pub rules: Vec<Rule>,
    pub actions: Vec<Action>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ComposerSnapshot {
    pub config: EngineConfig,
    pub providers: BTreeMap<String, RuleSet>,
}

/// Deterministically composes provider matches into an execution plan.
pub struct Composer {
    config: EngineConfig,
    providers: BTreeMap<String, Arc<dyn RuleProvider>>,
}

impl Composer {
    pub fn new(config: EngineConfig, providers: BTreeMap<String, Arc<dyn RuleProvider>>) -> Self {
        Self { config, providers }
    }

    pub async fn snapshot(&self) -> ComposerSnapshot {
        let mut providers = BTreeMap::new();
        for (name, provider) in &self.providers {
            providers.insert(name.clone(), provider.snapshot().await);
        }
        ComposerSnapshot {
            config: self.config.clone(),
            providers,
        }
    }

    pub async fn build_execution_plan(
        &self,
        workflow: &str,
        message: &Message,
    ) -> Result<ExecutionPlan> {
        let policy = self.config.policy(workflow).ok_or_else(|| {
            AppError::new(
                ErrorType::InvalidInput,
                format!("workflow {workflow} has no configured policy"),
            )
        })?;
        let order = if policy.mode == CompositionMode::Pipeline && !policy.pipeline_order.is_empty()
        {
            &policy.pipeline_order
        } else {
            &policy.providers
        };
        if order.is_empty() {
            return invalid(format!(
                "workflow {workflow} has no rule providers configured"
            ));
        }

        let mut matched = BTreeMap::new();
        for name in order {
            let provider = self.providers.get(name).ok_or_else(|| {
                AppError::new(
                    ErrorType::InvalidInput,
                    format!("rule provider {name} not found"),
                )
            })?;
            let snapshot = provider.snapshot().await;
            validate_provider_rule_set(&self.config, name, &snapshot).map_err(|error| {
                AppError::wrap(
                    ErrorType::InvalidInput,
                    format!("rule provider {name} published invalid rules"),
                    error,
                )
            })?;
            let rules: Vec<_> = snapshot
                .rules
                .iter()
                .filter(|rule| rule.workflow.is_empty() || rule.workflow == workflow)
                .cloned()
                .collect();
            matched.insert(name.clone(), match_rules(&rules, message));
        }

        let mut selected = select_rules(policy, order, &matched).unwrap_or_default();
        selected.sort_by_key(|rule| rule.priority);
        let mut actions: Vec<_> = selected
            .iter()
            .flat_map(|rule| rule.actions.iter().cloned())
            .collect();
        actions = merge_actions(actions, &policy.action_merge);
        Ok(ExecutionPlan {
            workflow: workflow.to_owned(),
            rules: selected,
            actions,
        })
    }
}

fn select_rules(
    policy: &WorkflowPolicy,
    order: &[String],
    matched: &BTreeMap<String, Vec<Rule>>,
) -> Option<Vec<Rule>> {
    match policy.mode {
        CompositionMode::Single => {
            let rules = matched.get(order.first()?)?.clone();
            (!rules.is_empty()).then_some(rules)
        }
        CompositionMode::Or => {
            let all: Vec<_> = order
                .iter()
                .flat_map(|name| matched.get(name).into_iter().flatten().cloned())
                .collect();
            (!all.is_empty()).then_some(all)
        }
        CompositionMode::And | CompositionMode::Pipeline => {
            if order
                .iter()
                .any(|name| matched.get(name).is_none_or(Vec::is_empty))
            {
                return None;
            }
            Some(
                order
                    .iter()
                    .flat_map(|name| matched.get(name).into_iter().flatten().cloned())
                    .collect(),
            )
        }
    }
}

fn merge_actions(actions: Vec<Action>, config: &ActionMergeConfig) -> Vec<Action> {
    let mut merged = if config.dedup {
        let mut seen = HashSet::new();
        actions
            .into_iter()
            .filter(|action| seen.insert(action_dedup_key(action)))
            .collect()
    } else {
        actions
    };
    if config.order == ActionOrder::Priority {
        merged.sort_by_key(|action| action.priority);
    }
    merged
}

fn action_dedup_key(action: &Action) -> String {
    if !action.id.is_empty() {
        return action.id.clone();
    }
    let params = serde_json::to_string(&action.params).unwrap_or_else(|_| "{}".to_owned());
    format!("{}|{params}", action.executor)
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(AppError::new(ErrorType::InvalidInput, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn action(id: &str, priority: i32) -> Action {
        Action {
            id: id.to_owned(),
            executor: "log".to_owned(),
            priority,
            params: BTreeMap::new(),
        }
    }

    fn rule(id: &str, priority: i32, action: Action) -> Rule {
        Rule {
            id: id.to_owned(),
            workflow: DEFAULT_WORKFLOW_NAME.to_owned(),
            source: "yaml".to_owned(),
            enabled: true,
            priority,
            conditions: vec![Condition {
                field: "type".to_owned(),
                op: ConditionOperator::Eq,
                value: Value::String("AAA".to_owned()),
            }],
            actions: vec![action],
        }
    }

    #[test]
    fn repository_engine_profiles_are_strict_and_valid() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for relative in [
            "config/rule-engine.yaml",
            "config/profiles/demo/rule-engine.yaml",
        ] {
            EngineConfig::load(root.join(relative)).unwrap();
        }
    }

    #[tokio::test]
    async fn repository_rule_profiles_are_strict_and_valid() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for relative in [
            "config/workflow-rules.yaml",
            "examples/rules/incident-routing.yaml",
            "examples/rules/release-gate.yaml",
            "examples/rules/support-triage.yaml",
            "examples/rules/webhook-chain.yaml",
        ] {
            let provider = YamlProvider::new("yaml", root.join(relative), DEFAULT_WORKFLOW_NAME);
            provider.start().await.unwrap();
            let snapshot = provider.snapshot().await;
            assert!(!snapshot.rules.is_empty(), "{relative}");
            validate_rule_set(&snapshot).unwrap();
        }
    }

    #[tokio::test]
    async fn yaml_provider_loads_defaults_and_rejects_unknown_fields() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let provider = YamlProvider::new(
            "yaml",
            root.join("config/workflow-rules.yaml"),
            DEFAULT_WORKFLOW_NAME,
        );
        provider.start().await.unwrap();
        let snapshot = provider.snapshot().await;
        assert_eq!(snapshot.rules.len(), 1);
        assert!(snapshot.rules[0].enabled);
        assert_eq!(snapshot.rules[0].workflow, DEFAULT_WORKFLOW_NAME);

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rules.yaml");
        fs::write(
            &path,
            "rules:\n  - id: bad\n    typo: true\n    actions: []\n",
        )
        .unwrap();
        let provider = YamlProvider::new("yaml", path, DEFAULT_WORKFLOW_NAME);
        assert!(provider.start().await.is_err());

        let empty = directory.path().join("empty.yaml");
        fs::write(&empty, "").unwrap();
        let provider = YamlProvider::new("yaml", empty, DEFAULT_WORKFLOW_NAME);
        assert!(
            provider
                .start()
                .await
                .unwrap_err()
                .to_string()
                .contains("empty")
        );

        let empty_engine = directory.path().join("empty-engine.yaml");
        fs::write(&empty_engine, "").unwrap();
        assert!(
            EngineConfig::load(empty_engine)
                .unwrap_err()
                .to_string()
                .contains("empty")
        );
    }

    #[test]
    fn matcher_supports_scalar_and_nested_metadata_fields() {
        let message = Message {
            kind: "AAA".to_owned(),
            metadata: BTreeMap::from([(
                "environment".to_owned(),
                serde_json::json!({"name": "production"}),
            )]),
            ..Message::default()
        };
        let conditions = [
            Condition {
                field: "type".to_owned(),
                op: ConditionOperator::Eq,
                value: Value::String("AAA".to_owned()),
            },
            Condition {
                field: "metadata.environment.name".to_owned(),
                op: ConditionOperator::Regex,
                value: Value::String("^prod".to_owned()),
            },
        ];
        assert!(
            conditions
                .iter()
                .all(|item| match_condition(item, &message))
        );
    }

    #[tokio::test]
    async fn composer_orders_and_deduplicates_actions() {
        let snapshot = RuleSet {
            source: "yaml".to_owned(),
            version: "1".to_owned(),
            loaded_at: Utc::now(),
            rules: vec![
                rule("later", 20, action("same", 20)),
                rule("earlier", 10, action("same", 10)),
            ],
            last_error: String::new(),
        };
        let provider: Arc<dyn RuleProvider> = Arc::new(StaticProvider::new("yaml", snapshot));
        let composer = Composer::new(
            EngineConfig::default(),
            BTreeMap::from([("yaml".to_owned(), provider)]),
        );
        let plan = composer
            .build_execution_plan(
                DEFAULT_WORKFLOW_NAME,
                &Message {
                    kind: "AAA".to_owned(),
                    ..Message::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(plan.rules.len(), 2);
        assert_eq!(plan.actions, vec![action("same", 10)]);
    }

    #[test]
    fn single_mode_requires_exactly_one_provider() {
        let mut config = EngineConfig::default();
        config.workflows[0].providers.push("remote".to_owned());
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("exactly one")
        );
    }

    fn provider(name: &str, message_type: &str, action_id: &str) -> Arc<dyn RuleProvider> {
        Arc::new(StaticProvider::new(
            name,
            RuleSet {
                source: name.to_owned(),
                version: "1".to_owned(),
                loaded_at: Utc::now(),
                rules: vec![Rule {
                    id: format!("{name}-rule"),
                    workflow: DEFAULT_WORKFLOW_NAME.to_owned(),
                    source: name.to_owned(),
                    enabled: true,
                    priority: 100,
                    conditions: vec![Condition {
                        field: "type".to_owned(),
                        op: ConditionOperator::Eq,
                        value: Value::String(message_type.to_owned()),
                    }],
                    actions: vec![action(action_id, 100)],
                }],
                last_error: String::new(),
            },
        ))
    }

    fn composer_for(
        mode: CompositionMode,
        pipeline_order: Vec<String>,
        first_type: &str,
        second_type: &str,
    ) -> Composer {
        let providers = vec!["first".to_owned(), "second".to_owned()];
        Composer::new(
            EngineConfig {
                workflows: vec![WorkflowPolicy {
                    name: DEFAULT_WORKFLOW_NAME.to_owned(),
                    providers,
                    mode,
                    pipeline_order,
                    action_merge: ActionMergeConfig::default(),
                }],
                action_merge: ActionMergeConfig::default(),
            },
            BTreeMap::from([
                (
                    "first".to_owned(),
                    provider("first", first_type, "first-action"),
                ),
                (
                    "second".to_owned(),
                    provider("second", second_type, "second-action"),
                ),
            ]),
        )
    }

    #[tokio::test]
    async fn composition_modes_apply_their_exact_provider_semantics() {
        let message = Message {
            kind: "AAA".to_owned(),
            ..Message::default()
        };

        let or = composer_for(CompositionMode::Or, Vec::new(), "AAA", "BBB");
        let plan = or
            .build_execution_plan(DEFAULT_WORKFLOW_NAME, &message)
            .await
            .unwrap();
        assert_eq!(plan.rules.len(), 1);
        assert_eq!(plan.actions[0].id, "first-action");

        let and = composer_for(CompositionMode::And, Vec::new(), "AAA", "BBB");
        let plan = and
            .build_execution_plan(DEFAULT_WORKFLOW_NAME, &message)
            .await
            .unwrap();
        assert!(plan.rules.is_empty());
        assert!(plan.actions.is_empty());

        let and = composer_for(CompositionMode::And, Vec::new(), "AAA", "AAA");
        assert_eq!(
            and.build_execution_plan(DEFAULT_WORKFLOW_NAME, &message)
                .await
                .unwrap()
                .rules
                .len(),
            2
        );

        let pipeline = composer_for(
            CompositionMode::Pipeline,
            vec!["second".to_owned(), "first".to_owned()],
            "AAA",
            "AAA",
        );
        let plan = pipeline
            .build_execution_plan(DEFAULT_WORKFLOW_NAME, &message)
            .await
            .unwrap();
        assert_eq!(
            plan.actions
                .iter()
                .map(|action| action.id.as_str())
                .collect::<Vec<_>>(),
            vec!["second-action", "first-action"]
        );

        let single = Composer::new(
            EngineConfig::default(),
            BTreeMap::from([("yaml".to_owned(), provider("yaml", "BBB", "single"))]),
        );
        let plan = single
            .build_execution_plan(DEFAULT_WORKFLOW_NAME, &message)
            .await
            .unwrap();
        assert!(plan.rules.is_empty());
    }

    #[tokio::test]
    async fn composer_reports_unknown_workflows_and_providers_and_snapshots_state() {
        let composer = Composer::new(
            EngineConfig::default(),
            BTreeMap::from([("yaml".to_owned(), provider("yaml", "AAA", "action"))]),
        );
        assert!(
            composer
                .build_execution_plan("missing", &Message::default())
                .await
                .unwrap_err()
                .to_string()
                .contains("no configured policy")
        );
        let snapshot = composer.snapshot().await;
        assert_eq!(snapshot.providers.len(), 1);
        assert!(snapshot.providers.contains_key("yaml"));

        let composer = Composer::new(EngineConfig::default(), BTreeMap::new());
        assert!(
            composer
                .build_execution_plan(DEFAULT_WORKFLOW_NAME, &Message::default())
                .await
                .unwrap_err()
                .to_string()
                .contains("not found")
        );
    }

    #[test]
    fn action_merge_supports_id_param_dedup_and_explicit_duplicates() {
        let mut first = action("", 20);
        first.executor = "http".to_owned();
        first
            .params
            .insert("url".to_owned(), Value::String("one".to_owned()));
        let duplicate = first.clone();
        let mut distinct = first.clone();
        distinct
            .params
            .insert("url".to_owned(), Value::String("two".to_owned()));
        distinct.priority = 10;

        let merged = merge_actions(
            vec![first.clone(), duplicate.clone(), distinct.clone()],
            &ActionMergeConfig::default(),
        );
        assert_eq!(merged, vec![distinct.clone(), first.clone()]);

        let merged = merge_actions(
            vec![first, duplicate, distinct],
            &ActionMergeConfig {
                dedup: false,
                order: ActionOrder::Priority,
            },
        );
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].priority, 10);
    }

    #[test]
    fn engine_validation_rejects_empty_duplicate_and_invalid_pipeline_policies() {
        let mut config = EngineConfig::default();
        config.workflows.clear();
        assert!(config.validate().is_err());

        let cases = [
            WorkflowPolicy {
                name: String::new(),
                providers: vec!["yaml".to_owned()],
                mode: CompositionMode::Single,
                pipeline_order: vec!["yaml".to_owned()],
                action_merge: ActionMergeConfig::default(),
            },
            WorkflowPolicy {
                name: DEFAULT_WORKFLOW_NAME.to_owned(),
                providers: Vec::new(),
                mode: CompositionMode::Or,
                pipeline_order: Vec::new(),
                action_merge: ActionMergeConfig::default(),
            },
            WorkflowPolicy {
                name: DEFAULT_WORKFLOW_NAME.to_owned(),
                providers: vec!["yaml".to_owned(), "yaml".to_owned()],
                mode: CompositionMode::Or,
                pipeline_order: Vec::new(),
                action_merge: ActionMergeConfig::default(),
            },
            WorkflowPolicy {
                name: DEFAULT_WORKFLOW_NAME.to_owned(),
                providers: vec!["yaml".to_owned(), "remote".to_owned()],
                mode: CompositionMode::Pipeline,
                pipeline_order: vec!["yaml".to_owned()],
                action_merge: ActionMergeConfig::default(),
            },
        ];
        for workflow in cases {
            assert!(
                EngineConfig {
                    workflows: vec![workflow],
                    action_merge: ActionMergeConfig::default(),
                }
                .validate()
                .is_err()
            );
        }

        let duplicated_workflow = EngineConfig {
            workflows: vec![
                EngineConfig::default().workflows[0].clone(),
                EngineConfig::default().workflows[0].clone(),
            ],
            action_merge: ActionMergeConfig::default(),
        };
        assert!(duplicated_workflow.validate().is_err());
    }
}
