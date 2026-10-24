# Rust Service Platform

Rust Service Platform is a configurable, rule-driven framework for event-processing services. It separates reusable matching, composition, execution, health, logging, and lifecycle APIs from application-specific transports and integrations.

The repository contains:

- `service-platform`: the reusable public SDK
- `service-runtime`: a reference application with admin and lifecycle wiring
- `fake-server`: local Confluence-like, Mattermost-like, and WebSocket APIs
- strict YAML configuration and rule files
- five runnable examples
- local and container one-command development environments

Mattermost, Confluence, and BadgeDB are demonstration adapters. They are not required by the core framework.

## Requirements

- Rust 1.86 or later
- Cargo
- `curl` for the local verification commands
- Docker with Compose v2 for the container path
- optional: CodeLLDB and rust-analyzer for VS Code debugging

Check the toolchain:

```bash
rustc --version
cargo --version
```

## Quick Start

Run the smallest in-process pipeline:

```bash
cargo run -p service-platform --example basic-rule-pipeline
```

Expected final output:

```text
matched_rules=1 executed_actions=1
```

Run the complete local demo:

```bash
make dev
```

`make dev` builds and starts `fake-server`, waits for its health endpoint, then starts `service-workflow` with the demo profile. In another terminal:

```bash
curl -s http://127.0.0.1:18080/health/readiness

curl -s -X POST http://127.0.0.1:8093/api/send \
  -H 'Content-Type: application/json' \
  -d '{"id":"quick-1","type":"AAA","content":"hello","user_id":"local-user"}'

curl -s http://127.0.0.1:8091/api/v4/posts \
  -H 'Authorization: Bearer test-token-123'
```

The event is broadcast over WebSocket, composed against YAML and demo-provider rules, logged, and posted to the fake Mattermost API.

Run the same path as an automated local smoke test:

```bash
make dev-smoke
```

Container equivalent:

```bash
docker compose up --build
```

Build, start, exercise, and clean up the complete container environment:

```bash
make compose-verify
```

Stop either path with `Ctrl+C`; use `docker compose down` after the container path.

## Processing Model

```text
input event
    |
    v
RuleProvider snapshots
    |
    v
Composer: match + policy + action merge
    |
    v
ExecutionPlan
    |
    v
registered Executors
```

The framework supports:

- condition operators: `eq`, `contains`, `regex`
- composition modes: `single`, `or`, `and`, `pipeline`
- stable priority ordering
- action deduplication by action ID or executor-plus-parameters
- strict YAML decoding with unknown-field rejection
- startup preflight and reverse-order rollback
- typed partial execution failures

## Runtime Profiles

The base profile is YAML-first:

```bash
RUNTIME_CONFIG=config/runtime.yaml \
RULE_ENGINE_CONFIG=config/rule-engine.yaml \
cargo run -p service-runtime --bin service-workflow
```

The demo profile enables the optional adapters:

```bash
RUNTIME_CONFIG=config/profiles/demo/runtime.yaml \
RULE_ENGINE_CONFIG=config/profiles/demo/rule-engine.yaml \
cargo run -p service-runtime --bin service-workflow
```

`make dev` manages the fake dependencies and uses the second command automatically.

## Configuration

Three files define the runtime:

| File | Responsibility |
| --- | --- |
| `config/runtime.yaml` | Admin listener, input services, and optional adapters |
| `config/rule-engine.yaml` | Workflow policies, providers, composition, and action merge |
| `config/workflow-rules.yaml` | Local YAML rules and actions |

Operational environment overrides include:

- `RUNTIME_CONFIG`
- `RULE_ENGINE_CONFIG`
- `WORKFLOW_RULES`
- `ADMIN_ADDR`
- `WEBSOCKET_SERVER_URL`
- `WEBSOCKET_PATH`
- `CONFLUENCE_API_ENDPOINT`
- `CONFLUENCE_SETTINGS_PAGE_ID`
- `MATTERMOST_SERVER_URL`
- `MATTERMOST_WS_URL`
- `MATTERMOST_API_TOKEN`
- `MATTERMOST_CHANNEL`
- `MATTERMOST_USERNAME`
- `MATTERMOST_PASSWORD`
- `BADGEDB_PATH`
- `LOG_LEVEL`
- `LOG_FORMAT`
- `LOG_OUTPUT`

See [Configuration](docs/configuration.md) for the complete field and override reference.

## Rule Example

```yaml
version: "1"
rules:
  - id: route_critical_incident
    workflow: WorkflowEngine
    enabled: true
    priority: 10
    conditions:
      - field: type
        op: eq
        value: INCIDENT
      - field: metadata.severity
        op: eq
        value: critical
    actions:
      - id: log_critical_incident
        executor: log
        priority: 10
        params:
          level: error
          template: "Critical incident {{.ID}}: {{.Content}}"
```

Rules are validated when the provider starts and again when a snapshot is composed. Enabled actions must reference registered executors.

## Public SDK

`crates/service-platform/src/` is the stable framework boundary:

| Module | Capability |
| --- | --- |
| `ruleengine` | Models, validation, YAML/static providers, matching, and composition |
| `pipeline` | Provider and executor lifecycle, preflight, processing, snapshots |
| `executor` | Executor trait, templates, log and HTTP executors |
| `config` | Admin listener config, duration parsing, and validation helpers |
| `runtime` | Fail-fast admin server with configured HTTP timeouts |
| `health` | Checkers, service health, caching, HTTP routes |
| `logger` | Structured tracing configuration and rotating file output |
| `errors` | Typed application errors and structured fields |

`crates/service-runtime` owns the reference application, runtime/adapter configuration, service manager, workflow engine, and demo adapters. Framework consumers normally depend only on `service-platform`.

## Admin API

The reference runtime exposes:

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Full cached health report |
| `GET /health/readiness` | Dependency-aware readiness |
| `GET /health/liveness` | Process and admin-server liveness |
| `GET /health/service?service=<name>` | One registered service |
| `GET /services` | Service wiring and metrics |
| `GET /workflows` | Registered workflows |
| `GET /rule-engine` | Composition config, providers, rules, and executors |

There is intentionally no `/metrics` contract yet.

## Examples

```bash
cargo run -p service-platform --example basic-rule-pipeline
cargo run -p service-platform --example custom-executor
cargo run -p service-platform --example http-event-gateway
cargo run -p service-platform --example websocket-to-log
cargo run -p service-platform --example websocket-to-mattermost
```

The HTTP and WebSocket examples accept environment variables documented in [Runnable Examples](examples/README.md).

## Development

Run the complete local gate:

```bash
make verify
```

Individual targets:

```bash
make fmt
make clippy
make test
make doc
make build
```

Coverage uses `cargo-llvm-cov` and enforces 90% line coverage by default:

```bash
cargo +stable install cargo-llvm-cov --version 0.8.7 --locked
make coverage
```

`cargo-llvm-cov` may require a newer Rust release than the platform MSRV. Install the coverage tool with stable Rust; normal builds remain compatible with Rust 1.86.

Rust stable does not provide a direct equivalent of `go test -race`. `make test-tsan` is an explicit optional target for nightly Rust on supported Linux hosts.

## Project Layout

```text
crates/
  service-platform/       Reusable framework SDK and runnable examples
  service-runtime/        Reference runtime, DI, managers, services, adapters
  fake-server/            Local dependency simulator
config/
  runtime.yaml            Base runtime profile
  rule-engine.yaml        Base YAML-first composition
  workflow-rules.yaml     Base local rules
  profiles/demo/          Optional full-adapter profile
examples/rules/           Scenario rule files
docs/                     User, developer, and architecture guides
.github/workflows/ci.yml  Static checks, tests, coverage, local and container smoke gates
```

## Further Reading

- [Documentation Map](docs/README.md)
- [Getting Started](docs/getting-started.md)
- [Development Guidebook](docs/development.md)
- [Configuration](docs/configuration.md)
- [Rule Engine](docs/rule-engine.md)
- [Executors](docs/executors.md)
- [Reference Adapters](docs/adapters.md)
- [Framework Cookbook](docs/cookbook.md)
- [Architecture and Design](docs/design.md)
