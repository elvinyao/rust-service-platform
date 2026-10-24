# Reference Adapters

The reusable SDK does not depend on Mattermost, Confluence, BadgeDB, or the fake server. `service-runtime` contains these optional adapters to demonstrate inputs, dynamic providers, service-backed executors, health, and lifecycle wiring.

## Runtime Wiring

`app::App` constructs `HealthManager` and `ServiceManager`, registers only enabled services, and passes `AdapterServices` directly into `workflow::WorkflowEngine`.

`workflow::WorkflowEngine` always registers:

- YAML provider
- `log` executor
- `http` executor

It conditionally registers:

- Confluence provider
- Mattermost executor
- BadgeDB executor

Disabled providers are removed from workflow policies before validation.

## WebSocket Input

Service: `WebSocketInputService`

Source: `crates/service-runtime/src/service.rs`

Behavior:

1. combine `server_url` and `path`
2. connect asynchronously
3. mark connection metrics
4. decode text or binary JSON as `ruleengine::Message`
5. ignore `type: system` frames
6. invoke the registered async message handler
7. reconnect after disconnect or connect failure
8. stop promptly through a one-shot shutdown signal

Health:

- service running state is critical
- zero active WebSocket connections is critical/down
- no messages for 30 minutes is warning/degraded

Metrics include connection count, configured endpoint, reconnect interval, and last-message age.

The service validates configuration both during runtime profile loading and before start/reconfigure.

## Confluence-Like Rule Provider

Services:

- `ConfluenceSettingsService`
- `ConfluenceProvider`

The service fetches:

```text
GET {api_endpoint}/rest/api/content/{settings_page_id}
```

It expects:

```json
{
  "body": {
    "storage": {
      "value": "{\"rules\":[...]}"
    }
  }
}
```

Settings rule:

```json
{
  "event_type": "AAA",
  "pattern": ".*",
  "channel_id": "test-channel-1",
  "message_template": "Event AAA received: {{.Content}}",
  "enabled": true
}
```

The provider converts each setting to:

- `type eq <event_type>`
- optional `content regex <pattern>`
- one `mattermost` action

Operational behavior:

- a bounded shared Reqwest client is created once
- non-2xx and malformed responses return typed errors
- initial refresh failure is logged without killing the settings service
- periodic refresh is cancellable during both sleep and HTTP I/O
- the shared cache receives the latest settings
- the provider retains the last valid rule set when a later refresh fails

The workflow engine requires Mattermost when the active policy uses Confluence because generated actions target the `mattermost` executor.

## Mattermost-Like Executor

Services:

- `MattermostService`
- `MattermostExecutor`

Authentication:

1. use `api_token` when non-empty
2. otherwise post username/password to `/api/v4/users/login`
3. require a successful response and non-empty `Token` header

Posting:

```text
POST {server_url}/api/v4/posts
Authorization: Bearer <token>
Content-Type: application/json
```

Body:

```json
{
  "channel_id": "test-channel-1",
  "message": "rendered text"
}
```

The executor accepts:

| Parameter | Meaning |
| --- | --- |
| `template` | Shared executor template |
| `channel_id` | Per-action override of the configured channel |

The service also maintains an optional WebSocket connection for connectivity health. It accepts either a base WebSocket URL or one already ending in `/api/v4/websocket`, adds bearer authentication, sends the Mattermost authentication challenge, and uses a bounded handshake.

## BadgeDB Demo Executor

Services:

- `BadgeDBService`
- `BadgeDbExecutor`

Type: `db`

Action parameters:

| Parameter | Default |
| --- | --- |
| `name` | `workflow-badge` |
| `description` | `created by workflow executor` |
| `badge_type` | input message type |

The executor creates a badge with source message and action metadata, then saves it through the service.

The store is an in-memory `BTreeMap` protected by an asynchronous `RwLock`. The configured path is exposed in metrics and represents the demonstration backup target; it is not a durable database implementation.

## Fake Server

Run:

```bash
cargo run -p fake-server --bin fake-server
```

Default ports:

| API | Port |
| --- | --- |
| Confluence-like HTTP | `8090` |
| Mattermost-like HTTP | `8091` |
| Mattermost-like WebSocket | `8092` |
| Generic WebSocket/event API | `8093` |

Useful endpoints:

| Method and path | Purpose |
| --- | --- |
| `GET :8090/health` | Confluence-like health |
| `GET :8090/rest/api/content/settings-page-1` | Settings page |
| `POST :8091/api/v4/users/login` | Fake login |
| `POST :8091/api/v4/posts` | Create an authenticated post |
| `GET :8091/api/v4/posts` | List authenticated posts |
| `GET :8091/api/v4/system/ping` | Mattermost-like health |
| `GET ws://:8092/api/v4/websocket` | Mattermost-like WebSocket |
| `GET ws://:8093/ws` | Generic event WebSocket |
| `POST :8093/api/send` | Store and broadcast an event |
| `GET :8093/api/messages` | List stored events |
| `GET :8093/health` | Generic WebSocket health |

Fake Mattermost token:

```text
test-token-123
```

Custom ports:

```bash
cargo run -p fake-server --bin fake-server -- \
  --confluence-port 19090 \
  --mm-http-port 19091 \
  --mm-ws-port 19092 \
  --ws-port 19093
```

Healthcheck mode:

```bash
cargo run -p fake-server --bin fake-server -- \
  --healthcheck --ws-port 8093
```

## Adding an Input Adapter

1. Define a lifecycle-managed service implementing `Service` and `ServiceHealth`.
2. Validate all endpoint and interval configuration before spawning.
3. Keep cancellation handles and task handles in one protected runtime state.
4. Make `start` and `stop` idempotent.
5. Decode transport payloads into `ruleengine::Message`.
6. expose an async handler registration API.
7. Register the service conditionally in `App::start`.
8. Connect its handler to `WorkflowEngine::process_message`.
9. Add disconnect, reconnect, malformed-payload, shutdown, and health tests.

Transport concerns belong in the adapter. Rules should not know whether a message came from HTTP, WebSocket, a queue, or a file.

## Adding a Provider

1. Implement `RuleProvider`.
2. Publish complete, cloned snapshots.
3. validate candidates before replacing the last good snapshot.
4. use a stable provider name.
5. add the provider to workflow policies explicitly.
6. register it only when its runtime dependency is enabled.
7. test invalid dynamic snapshots after startup.

## Adding a Service-Backed Executor

1. Implement `Executor`.
2. Inject `Arc<MyService>` directly into the executor constructor.
3. Verify the service is running before executing actions.
4. Validate action parameters.
5. Call a bounded service method.
6. Return dependency errors to the pipeline.
7. Register the executor only when its service is enabled.
