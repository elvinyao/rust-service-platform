# Rule Engine

The rule engine turns provider snapshots and one message into a deterministic execution plan. `service_platform::ruleengine` owns models, validation, matching, and composition; `service_platform::pipeline` owns lifecycle, executor preflight, and action execution.

## Message Model

```rust
pub struct Message {
    pub id: String,
    pub kind: String, // Serialized as "type".
    pub content: String,
    pub user_id: String,
    pub timestamp: DateTime<Utc>,
    pub metadata: BTreeMap<String, serde_json::Value>,
}
```

JSON shape:

```json
{
  "id": "inc-42",
  "type": "INCIDENT",
  "content": "checkout latency",
  "user_id": "monitor",
  "metadata": {
    "severity": "critical",
    "service": "checkout",
    "deployment": {
      "region": "ap-northeast-1"
    }
  }
}
```

`metadata` supports nested JSON objects. Conditions can address nested values with dot paths such as `metadata.deployment.region`.

## Rule Model

A rule contains:

- unique ID
- workflow name
- source provider
- enabled flag
- priority
- zero or more conditions
- one or more actions

All conditions in one rule must match. A rule with no conditions matches every message.

```yaml
- id: route_checkout_incident
  workflow: WorkflowEngine
  enabled: true
  priority: 20
  conditions:
    - field: type
      op: eq
      value: INCIDENT
    - field: metadata.service
      op: eq
      value: checkout
  actions:
    - id: log_checkout_incident
      executor: log
      priority: 10
      params:
        level: warn
        template: "incident={{.ID}} service=checkout"
```

## Conditions

Supported fields:

- `id`
- `type`
- `content`
- `user_id`
- `timestamp`
- `metadata.<path>`

Supported operators:

| Operator | Behavior |
| --- | --- |
| `eq` | Normalized scalar equality |
| `contains` | Substring containment |
| `regex` | Rust regular expression match |

Values normalize to strings. JSON strings use their contents; other JSON values use their JSON representation.

Regular expressions compile during rule validation. Invalid expressions fail provider startup instead of silently never matching.

## Providers

```rust
#[async_trait::async_trait]
pub trait RuleProvider: Send + Sync {
    fn name(&self) -> &str;
    async fn start(&self) -> service_platform::Result<()>;
    async fn stop(&self) -> service_platform::Result<()> {
        Ok(())
    }
    async fn snapshot(&self) -> RuleSet;
}
```

### Static Provider

`StaticProvider` owns an in-memory snapshot. It is useful for embedding, tests, and rules built from application configuration.

```rust
let provider: Arc<dyn RuleProvider> = Arc::new(StaticProvider::new(
    "local",
    rule_set,
));
```

The constructor stamps the provider name into the rule set and every rule.

### YAML Provider

`YamlProvider` loads one strict YAML document during `start`.

```rust
let provider: Arc<dyn RuleProvider> = Arc::new(YamlProvider::new(
    "yaml",
    "config/workflow-rules.yaml",
    DEFAULT_WORKFLOW_NAME,
));
```

It:

- rejects unknown fields
- rejects additional YAML documents
- applies defaults for workflow, enabled state, version, and collections
- validates IDs, actions, fields, operators, and regex values
- sorts rules stably by priority
- publishes a cloned immutable snapshot

### Dynamic Providers

A dynamic provider should publish a complete snapshot, not mutate a snapshot while a composer reads it. The demo Confluence provider follows a last-good-snapshot model:

1. build a candidate from the current service data
2. validate the complete candidate
3. publish it atomically
4. retain the previous valid snapshot when refresh fails
5. expose the refresh error through `last_error`

Dynamic snapshots are validated again every time a plan is built.

## Engine Configuration

```rust
pub struct EngineConfig {
    pub workflows: Vec<WorkflowPolicy>,
    pub action_merge: ActionMergeConfig,
}
```

Each exact workflow name maps to one `WorkflowPolicy`. There is no implicit fallback to the first policy.

## Composition Modes

Assume providers `a` and `b`.

### `single`

Exactly one provider must be configured. Its matching rules are selected.

### `or`

Matching rules from every provider are selected. It succeeds when at least one provider contributes a match.

### `and`

Every configured provider must contribute at least one matching rule. If any provider has no match, the plan is empty.

### `pipeline`

Every provider in `pipeline_order` must contribute at least one match, and selected rules are concatenated in that explicit provider order. The current pipeline mode composes stages; it does not transform the message between providers.

Provider order is deterministic. Selected rules are then stably sorted by rule priority.

## Action Merge

Actions from selected rules are collected and then merged.

When deduplication is enabled:

1. a non-empty action ID is the deduplication key
2. otherwise, the key is executor name plus canonical JSON parameters
3. the first action with a key is retained

The supported order is `priority`, implemented as a stable ascending sort.

## Execution Plans

```rust
pub struct ExecutionPlan {
    pub workflow: String,
    pub rules: Vec<Rule>,
    pub actions: Vec<Action>,
}
```

An empty plan is a successful result: the message was valid, but composition selected no rules.

Plan construction can fail when:

- the workflow has no exact policy
- a configured provider is missing
- a provider publishes an invalid snapshot
- a rule names a workflow that cannot use its provider

## Running a Pipeline

```rust
let engine = Engine::new(config, providers, executors).await?;
engine.start().await?;

let plan = engine
    .process(DEFAULT_WORKFLOW_NAME, &message)
    .await?;

engine.stop().await?;
```

`Engine::start`:

1. starts providers in sorted name order
2. validates every provider snapshot and enabled executor reference
3. starts executors in sorted type order
4. rolls back in reverse order on failure

`Engine::stop` blocks new processing, waits for in-flight processing, and stops executors then providers in reverse order. Cleanup has a global ten-second deadline; failed components remain tracked so a later stop can retry.

## Partial Failures

All actions in a selected plan are attempted even if one fails.

`Engine::process` returns an `AppError` with `ErrorType::PartialFailure`. Its source is `pipeline::ExecutionError`, which contains:

- the original execution plan
- one `ActionFailure` per failed action

Use `Engine::process_outcome` when an application explicitly wants plan and failure data without converting partial failures to an error:

```rust
let outcome = engine
    .process_outcome(DEFAULT_WORKFLOW_NAME, &message)
    .await?;

for failure in &outcome.failures {
    tracing::error!(
        action = %failure.action_id,
        executor = %failure.executor,
        error = %failure.error,
    );
}
```

## Snapshot API

`Engine::snapshot` returns:

- normalized engine configuration
- cloned snapshots from every provider
- sorted executor type names

The reference runtime exposes it at `GET /rule-engine`.

## Validation Boundaries

Validation occurs at several layers on purpose:

| Boundary | Catches |
| --- | --- |
| YAML decode | Unknown fields, invalid enum values, extra documents |
| `EngineConfig::validate` | Policy names, provider lists, mode invariants |
| provider startup | Rule IDs, actions, fields, regex values |
| pipeline preflight | Provider/workflow wiring and executor availability |
| plan construction | Invalid dynamic snapshots after startup |

This makes invalid configuration fail close to its source while still protecting against dynamic providers that become invalid later.
