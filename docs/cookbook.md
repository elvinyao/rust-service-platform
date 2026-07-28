# Framework Cookbook

These recipes use the public `service-platform` crate unless a section explicitly discusses the reference runtime.

## Build a Pipeline From YAML

```rust
use std::sync::Arc;

use service_platform::{
    executor::{Executor, HttpExecutor, LogExecutor},
    pipeline::Engine,
    ruleengine::{
        EngineConfig, RuleProvider, YamlProvider, DEFAULT_WORKFLOW_NAME,
    },
};

let config = EngineConfig::load("config/rule-engine.yaml")?;
let providers: Vec<Arc<dyn RuleProvider>> = vec![Arc::new(
    YamlProvider::new(
        "yaml",
        "config/workflow-rules.yaml",
        DEFAULT_WORKFLOW_NAME,
    ),
)];
let executors: Vec<Arc<dyn Executor>> = vec![
    Arc::new(LogExecutor),
    Arc::new(HttpExecutor::new()),
];

let engine = Engine::new(config, providers, executors).await?;
engine.start().await?;
```

Always call `stop` during application shutdown.

## Process an Event and Inspect Partial Failures

The strict API returns a plan or error:

```rust
match engine.process(DEFAULT_WORKFLOW_NAME, &message).await {
    Ok(plan) => {
        tracing::info!(
            rules = plan.rules.len(),
            actions = plan.actions.len(),
            "message processed"
        );
    }
    Err(error) => {
        if let Some(details) = error
            .source
            .as_deref()
            .and_then(|source| source.downcast_ref::<pipeline::ExecutionError>())
        {
            for failure in &details.failures {
                tracing::error!(
                    action = %failure.action_id,
                    executor = %failure.executor,
                    error = %failure.error,
                );
            }
        }
        return Err(error);
    }
}
```

When partial failures are expected data:

```rust
let outcome = engine
    .process_outcome(DEFAULT_WORKFLOW_NAME, &message)
    .await?;
```

Both APIs attempt every action.

## Match Structured Metadata

Input:

```json
{
  "id": "deploy-42",
  "type": "DEPLOYMENT",
  "content": "release complete",
  "user_id": "ci",
  "metadata": {
    "environment": {
      "name": "production",
      "region": "ap-northeast-1"
    }
  }
}
```

Rule:

```yaml
conditions:
  - field: metadata.environment.name
    op: eq
    value: production
  - field: metadata.environment.region
    op: regex
    value: "^ap-"
```

Nested metadata works for conditions. The built-in template `index` expression intentionally supports only top-level metadata.

## Let Multiple Rules Contribute Actions

Use `or`:

```yaml
workflows:
  - name: WorkflowEngine
    providers: [yaml, policy_service]
    mode: or
    pipeline_order: [yaml, policy_service]
    action_merge:
      dedup: true
      order: priority
```

Within a provider, every matching rule contributes actions. Across providers, `or` also collects all matches.

Make action IDs globally meaningful when deduplication is enabled:

```yaml
actions:
  - id: notify_primary_on_call
    executor: http
```

The first action with the ID wins.

## Send a Webhook

```yaml
- id: send_incident_webhook
  executor: http
  priority: 100
  params:
    method: POST
    url: "https://hooks.example.test/incidents"
    timeout_ms: 2000
    headers:
      Authorization: "Bearer example-token"
      X-Event-Source: rust-service-platform
    body_template: '{"id":"{{.ID}}","type":"{{.Type}}","content":"{{.Content}}"}'
```

For untrusted text, prefer a custom typed executor so JSON escaping is guaranteed.

## Register a Custom Executor

```rust
struct AuditExecutor {
    entries: tokio::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl Executor for AuditExecutor {
    fn executor_type(&self) -> &'static str {
        "audit"
    }

    async fn execute(
        &self,
        message: &Message,
        action: &Action,
    ) -> service_platform::Result<()> {
        let category = action
            .params
            .get("category")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| AppError::new(
                ErrorType::InvalidInput,
                "audit action requires params.category",
            ))?;
        self.entries
            .lock()
            .await
            .push(format!("{category}:{}", message.id));
        Ok(())
    }
}
```

Register the same `Arc` instance that the application uses for inspection:

```rust
let audit = Arc::new(AuditExecutor {
    entries: tokio::sync::Mutex::new(Vec::new()),
});
let executors: Vec<Arc<dyn Executor>> = vec![audit.clone()];
```

Run the complete example:

```bash
cargo run -p service-platform --example custom-executor
```

## Implement a Dynamic Rule Provider

```rust
struct RemoteProvider {
    name: String,
    snapshot: tokio::sync::RwLock<RuleSet>,
}

#[async_trait::async_trait]
impl RuleProvider for RemoteProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> service_platform::Result<()> {
        let candidate = self.fetch().await?;
        validate_rule_set(&candidate)?;
        *self.snapshot.write().await = candidate;
        Ok(())
    }

    async fn snapshot(&self) -> RuleSet {
        self.snapshot.read().await.clone()
    }
}
```

For periodic refresh:

- build a candidate away from the write lock
- validate before publication
- swap the complete snapshot atomically
- retain the last valid snapshot on failure
- expose refresh state through health and metrics
- keep a cancellation handle and join the task during `stop`

## Choose a Composition Mode

| Requirement | Mode |
| --- | --- |
| Exactly one source owns decisions | `single` |
| Any source may contribute | `or` |
| Every source must approve/contribute | `and` |
| Every source contributes in explicit source order | `pipeline` |

Do not use `and` or `pipeline` merely to order optional sources: one source with no match empties the plan.

## Add an HTTP Input

Keep transport outside the rule engine:

```rust
async fn ingest(
    State(engine): State<Arc<Engine>>,
    Json(message): Json<Message>,
) -> Result<Json<ExecutionPlan>, ApiError> {
    validate_message(&message)?;
    let plan = engine
        .process(DEFAULT_WORKFLOW_NAME, &message)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(plan))
}
```

Production input checklist:

- request body limit
- strict JSON decoding
- authentication and authorization
- request/trace IDs
- timeout and graceful shutdown
- status mapping for invalid input, dependency failure, and partial action failure
- idempotency strategy
- rate limiting

`http-event-gateway` is a runnable focused example.

## Test a YAML Rule File

```rust
#[tokio::test]
async fn production_rules_load_and_match() {
    let provider = YamlProvider::new(
        "yaml",
        "config/workflow-rules.yaml",
        DEFAULT_WORKFLOW_NAME,
    );
    provider.start().await.unwrap();
    let snapshot = provider.snapshot().await;

    let message = Message {
        kind: "AAA".to_owned(),
        ..Message::default()
    };
    let matched = match_rules(&snapshot.rules, &message);
    assert_eq!(matched[0].id, "yaml_log_aaa");
}
```

Also test:

- a non-matching message
- every regex branch
- nested metadata paths
- action ordering
- duplicate action IDs
- invalid unknown fields

## Test an HTTP Executor

Bind an Axum server to an ephemeral loopback port:

```rust
let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
let address = listener.local_addr()?;
```

Assert:

- default `POST`
- configured method
- rendered body
- custom headers
- default content type
- every 2xx status
- non-2xx status and body capture
- timeout
- transport error
- invalid body template

Never call an external network service from a unit test.

## Run Independent Work With a Limit

```rust
let pool = WorkerPool::new(8)?;
let token = CancellationToken::new();
let results = pool.run_cancellable(&token, tasks).await?;
```

Cancellation prevents pending tasks from starting but does not forcibly abort active work.

## Separate Local and Deployment Configuration

Keep complete explicit profiles:

```text
config/runtime.yaml
config/profiles/demo/runtime.yaml
config/profiles/staging/runtime.yaml
config/profiles/production/runtime.yaml
```

Use environment variables for addresses and secrets that differ at deployment time. Do not create an undocumented implicit merge order.

Validate in a test:

```rust
#[test]
fn repository_profiles_are_valid() {
    RuntimeConfig::load("config/profiles/production/runtime.yaml").unwrap();
    EngineConfig::load("config/profiles/production/rule-engine.yaml").unwrap();
}
```

## Diagnose an Event That Did Not Act

1. Verify the input accepted/broadcast the message.
2. Inspect `GET /services` for input connection state.
3. Inspect `GET /rule-engine` for the expected provider snapshot.
4. Confirm the exact workflow name.
5. Compare condition field paths and normalized values.
6. Check that the rule is enabled.
7. Check rule and action IDs after deduplication.
8. Look for executor errors and dependency health.
9. Reproduce with `http-event-gateway` and the same rule file.

An empty plan is not a pipeline error.

## Prepare an Executor for Production

- reuse connection pools
- bound every external call
- redact credentials and sensitive payloads
- classify retryable and permanent failures
- define idempotency behavior
- expose latency/error metrics through the owning application
- make shutdown bounded and idempotent
- test dependency non-2xx, malformed responses, timeouts, and cancellation

## Prepare the Runtime for Production

The reference runtime is an architecture example, not a complete production edge:

- put authentication in front of the admin API
- bind admin to a private interface
- add a stable metrics exporter before advertising `/metrics`
- use a real secret manager
- replace demo persistence
- define retry/dead-letter policy
- define request and event size limits at every input
- review SSRF risk before allowing rule authors to choose arbitrary HTTP URLs
- run `make verify` and `make coverage`
- build and scan the container image
- exercise graceful shutdown under load

## Runnable Scenario Index

| Scenario | Rule file | Primary concepts |
| --- | --- | --- |
| Incident routing | `examples/rules/incident-routing.yaml` | Multiple matching rules, regex metadata |
| Release gate | `examples/rules/release-gate.yaml` | Mutually exclusive status paths |
| Support triage | `examples/rules/support-triage.yaml` | Priority and content matching |
| Webhook chain | `examples/rules/webhook-chain.yaml` | HTTP executor and derived events |
| Compliance/audit | `custom-executor` | Custom stateful executor |
| Learning sandbox | base YAML + fake server | Full message path and debugging |

The [Development Guidebook](development.md) expands these into complete exercises with breakpoints, expected observations, failure modes, and extension ideas.
