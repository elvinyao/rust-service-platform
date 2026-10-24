# Executors

Executors perform actions selected by an execution plan. They are small, application-owned capabilities registered with `pipeline::Engine`.

## Trait

```rust
#[async_trait::async_trait]
pub trait Executor: Send + Sync {
    fn executor_type(&self) -> &str;

    async fn start(&self) -> service_platform::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> service_platform::Result<()> {
        Ok(())
    }

    async fn execute(
        &self,
        message: &Message,
        action: &Action,
    ) -> service_platform::Result<()>;
}
```

Use a stable, non-empty executor type. `pipeline::Engine::new` rejects empty and duplicate types and owns the sorted executor map directly.

Lifecycle hooks are optional through their default implementations. Providers start first; executors start only after provider snapshots and executor references pass preflight. Shutdown is reverse order.

## Templates

Built-in and demo executors share `render_template`.

Supported fields:

```text
{{.ID}}
{{.Type}}
{{.Content}}
{{.UserID}}
{{.Timestamp}}
```

Top-level metadata:

```text
{{index .Metadata "severity"}}
```

Missing or null metadata renders as `<no value>`.

The template language intentionally implements only the configuration subset used by the platform. Functions, loops, conditionals, arbitrary field traversal, and nested metadata indexing are unsupported. Unmatched delimiters and unsupported expressions return `InvalidInput`.

## Log Executor

Type: `log`

Parameters:

| Parameter | Type | Default |
| --- | --- | --- |
| `level` | string | `info` |
| `template` | string | structured fallback message |

Levels:

- `debug`
- `info`
- `warn`
- `error`

Example:

```yaml
- id: log_incident
  executor: log
  priority: 10
  params:
    level: warn
    template: 'incident={{.ID}} severity={{index .Metadata "severity"}}'
```

Unknown levels use `info`.

## HTTP Executor

Type: `http`

Parameters:

| Parameter | Type | Default |
| --- | --- | --- |
| `url` | non-empty string | required |
| `method` | HTTP method string | `POST` |
| `body_template` | string | empty body |
| `headers` | JSON object | empty |
| `timeout_ms` | positive number | `3000` |

Example:

```yaml
- id: notify_webhook
  executor: http
  priority: 20
  params:
    method: POST
    url: "https://events.example.test/hooks/incidents"
    timeout_ms: 2500
    headers:
      Authorization: "Bearer example-token"
      X-Event-Source: rust-service-platform
    body_template: '{"id":"{{.ID}}","type":"{{.Type}}","content":"{{.Content}}"}'
```

Behavior:

- request method and URL are validated while constructing the request
- body templates render before network I/O
- header values accept JSON scalar values and are converted to strings
- `Content-Type: application/json` is added unless supplied
- request timeout is applied by Reqwest
- every 2xx status succeeds
- non-2xx errors include status and at most 1024 response bytes
- transport and timeout failures return `ServiceUnavailable`

Use `HttpExecutor::with_client` when the application owns a configured Reqwest client.

Template interpolation does not JSON-escape arbitrary message text. For untrusted content, implement an executor that builds a typed value and uses `.json(...)`, or ensure the event contract provides already-safe values.

## Custom Executor

Minimal shape:

```rust
struct PublishExecutor {
    client: reqwest::Client,
    endpoint: String,
}

#[async_trait::async_trait]
impl Executor for PublishExecutor {
    fn executor_type(&self) -> &'static str {
        "publish"
    }

    async fn execute(
        &self,
        message: &Message,
        action: &Action,
    ) -> service_platform::Result<()> {
        let topic = action
            .params
            .get("topic")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| AppError::new(
                ErrorType::InvalidInput,
                "publish action requires params.topic",
            ))?;

        self.client
            .post(&self.endpoint)
            .json(&serde_json::json!({
                "topic": topic,
                "message": message,
            }))
            .send()
            .await
            .map_err(|error| AppError::wrap(
                ErrorType::ServiceUnavailable,
                "publish event",
                error,
            ))?
            .error_for_status()
            .map_err(|error| AppError::wrap(
                ErrorType::ServiceUnavailable,
                "publish event",
                error,
            ))?;
        Ok(())
    }
}
```

Register it:

```rust
let executors: Vec<Arc<dyn Executor>> = vec![
    Arc::new(LogExecutor),
    Arc::new(PublishExecutor { client, endpoint }),
];
let engine = Engine::new(config, providers, executors).await?;
```

The complete in-memory example is `crates/service-platform/examples/custom-executor.rs`:

```bash
cargo run -p service-platform --example custom-executor
```

## Design Checklist

A production executor should:

- validate required action parameters
- bound HTTP, database, or queue operations with timeouts
- return typed errors with useful context
- avoid holding synchronous locks across `.await`
- be safe for concurrent calls
- keep `start` and `stop` idempotent
- own or receive reusable clients instead of creating one per action
- avoid logging credentials or complete sensitive payloads
- make retry and idempotency behavior explicit

The pipeline does not retry actions because retry policy is domain-specific. Put retries in the executor or behind the dependency client, and use an idempotency key when duplicate side effects are unsafe.

## Failure Semantics

The engine attempts every independent action in plan order.

For each failed action it records:

- action ID
- executor type
- error text

`Engine::process` returns `ErrorType::PartialFailure` with a typed `ExecutionError` source. `Engine::process_outcome` returns the same plan and failures as data.

Do not treat an empty plan as an error. It means no rules matched.

## Demo Executors

`service-runtime` adds:

- `mattermost`: renders a message and posts through `MattermostService`
- `db`: creates a demonstration badge record through `BadgeDbService`

They demonstrate constructor-injected service-backed executors. Keep application-specific executors outside `service-platform`.
