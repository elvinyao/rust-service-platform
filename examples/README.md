# Runnable Examples

Rust examples live under `crates/service-platform/examples` and are compiled by `cargo build --workspace --all-targets`. Run every command from the repository root.

## Basic Rule Pipeline

File:

```text
crates/service-platform/examples/basic-rule-pipeline.rs
```

Run:

```bash
cargo run -p service-platform --example basic-rule-pipeline
```

Demonstrates:

- static provider
- exact workflow policy
- one condition
- built-in log executor
- engine start, process, plan inspection, and stop

## Custom Executor

```bash
cargo run -p service-platform --example custom-executor
```

Demonstrates:

- implementing `Executor`
- validating action parameters
- using the shared template renderer
- thread-safe executor state
- registering one concrete executor as `Arc<dyn Executor>`

Expected output:

```text
event=audit-1 user=local-user content=configuration changed
```

## HTTP Event Gateway

Start:

```bash
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

Endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Gateway liveness |
| `GET /pipeline` | Engine/provider/executor snapshot |
| `POST /events` | Strict event ingestion |

Send an event:

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"gateway-1","type":"AAA","content":"hello","user_id":"local-user"}'
```

Environment:

| Variable | Default |
| --- | --- |
| `EVENT_GATEWAY_ADDR` | `127.0.0.1:18081` |
| `WORKFLOW_RULES` | `config/workflow-rules.yaml` |
| logging variables | See `docs/configuration.md` |

The gateway rejects unknown message fields, invalid JSON, payloads over 1 MiB, and events without ID/type. Partial executor failures return HTTP 502 with the plan retained in the response.

### Incident Routing

```bash
WORKFLOW_RULES=examples/rules/incident-routing.yaml \
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"inc-1","type":"INCIDENT","content":"checkout latency","user_id":"monitor","metadata":{"severity":"critical","service":"checkout"}}'
```

Two rules match: high severity and checkout ownership.

### Release Gate

```bash
WORKFLOW_RULES=examples/rules/release-gate.yaml \
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"release-1","type":"RELEASE_CHECK","content":"candidate 42","user_id":"ci","metadata":{"environment":"production","status":"failed"}}'
```

Change `status` to `approved` to select the approval rule.

### Support Triage

```bash
WORKFLOW_RULES=examples/rules/support-triage.yaml \
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"ticket-1","type":"SUPPORT_TICKET","content":"urgent refund request","user_id":"customer-1","metadata":{"priority":"p0"}}'
```

Both urgent and billing rules match.

### Webhook Chain

```bash
WORKFLOW_RULES=examples/rules/webhook-chain.yaml \
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"deploy-1","type":"RAW_DEPLOY","content":"release deployed","user_id":"ci"}'
```

The first rule logs and posts `DEPLOY_AUDIT` back to the gateway. The second request is processed by the audit rule. This is a compact demonstration of event chaining; production systems should add loop prevention and idempotency.

## WebSocket to Log

Terminal 1:

```bash
cargo run -p fake-server --bin fake-server
```

Terminal 2:

```bash
LOG_FORMAT=text \
cargo run -p service-platform --example websocket-to-log
```

Terminal 3:

```bash
curl -s -X POST http://127.0.0.1:8093/api/send \
  -H 'Content-Type: application/json' \
  -d '{"id":"ws-log-1","type":"AAA","content":"hello WebSocket","user_id":"local-user"}'
```

Environment:

| Variable | Default |
| --- | --- |
| `WEBSOCKET_URL` | `ws://localhost:8093/ws` |
| `WORKFLOW_RULES` | `config/workflow-rules.yaml` |

This focused example uses one connection. Automatic reconnect belongs to the reference runtime's `WebSocketService`.

## WebSocket to Mattermost-Like HTTP

Start the fake server, then:

```bash
LOG_FORMAT=text \
cargo run -p service-platform --example websocket-to-mattermost
```

Send:

```bash
curl -s -X POST http://127.0.0.1:8093/api/send \
  -H 'Content-Type: application/json' \
  -d '{"id":"notify-1","type":"AAA","content":"notify the channel","user_id":"local-user"}'
```

Inspect:

```bash
curl -s http://127.0.0.1:8091/api/v4/posts \
  -H 'Authorization: Bearer test-token-123'
```

Environment:

| Variable | Default |
| --- | --- |
| `WEBSOCKET_URL` | `ws://localhost:8093/ws` |
| `MATTERMOST_POST_URL` | `http://localhost:8091/api/v4/posts` |
| `MATTERMOST_API_TOKEN` | `test-token-123` |
| `MATTERMOST_CHANNEL` | `test-channel-1` |

The example intentionally uses the generic HTTP executor. The reference runtime demonstrates the richer service-backed Mattermost executor.

## Learning Order

1. `basic-rule-pipeline`
2. `custom-executor`
3. `http-event-gateway`
4. one scenario YAML file
5. `websocket-to-log`
6. `websocket-to-mattermost`
7. `make dev`

Use [Getting Started](../docs/getting-started.md) for a guided first pass and [Development Guidebook](../docs/development.md) for source-level debugging.
