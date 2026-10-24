# Rust Service Platform Design

## Summary

Rust Service Platform is a configurable, rule-driven framework for event-processing services. It provides a reusable SDK, a complete reference runtime, local fake dependencies, strict configuration, runnable examples, and verified development workflows.

The product boundary is generic. WebSocket, Confluence-like, Mattermost-like, and BadgeDB components demonstrate integration patterns without becoming core requirements.

## Goals

- provide stable reusable APIs for rule matching, composition, execution, health, logging, and configuration
- keep rule behavior primarily data-driven through strict YAML
- make extension points explicit through Rust traits and constructor injection
- provide fail-fast startup, rollback, bounded shutdown, and restartable lifecycle
- keep optional business integrations outside the public SDK
- support one-command local and container development
- keep documentation, examples, and commands synchronized with code
- require English code comments and documentation

## Non-Goals

- a dynamic binary plugin ABI or marketplace
- a distributed workflow scheduler
- exactly-once event processing
- built-in durable queueing or dead-letter storage
- production authentication for the reference admin API
- production persistence in BadgeDB
- a stable `/metrics` contract
- untrusted arbitrary HTTP execution without application policy

## Workspace Layout

```text
crates/
  service-platform/
    src/
      config.rs
      errors.rs
      executor.rs
      health.rs
      logger.rs
      pipeline.rs
      ruleengine.rs
      runtime.rs
    examples/
  service-runtime/
    src/
      adapters.rs
      app.rs
      cli.rs
      config.rs
      manager.rs
      service.rs
      workflow.rs
      main.rs
    tests/
  fake-server/
    src/
      cli.rs
config/
examples/rules/
docs/
```

Dependency direction:

```text
service-platform
      ^
      |
service-runtime <---- fake-server (dev/test only)

fake-server runtime binary is otherwise independent
```

`service-platform` must not import `service-runtime` or any demo adapter.

## Core Models

### Message

The normalized event boundary:

- ID
- type
- content
- user ID
- UTC timestamp
- JSON metadata map

Transports decode into `Message` before rule processing.

### Rule

A rule has:

- stable unique ID within one provider snapshot
- exact workflow or provider default
- source provider
- enabled flag
- priority
- all-of conditions
- one or more actions

### RuleSet

A provider publishes a complete immutable snapshot with source, version, load time, rules, and optional last error.

### ExecutionPlan

A plan contains the exact workflow, selected rules, and merged actions. Empty is a valid successful plan.

## Public Extension Traits

### RuleProvider

```rust
#[async_trait]
pub trait RuleProvider: Send + Sync {
    fn name(&self) -> &str;
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    async fn snapshot(&self) -> RuleSet;
}
```

Provider names are non-empty and unique. Snapshots must be safe to clone and read concurrently.

### Executor

```rust
#[async_trait]
pub trait Executor: Send + Sync {
    fn executor_type(&self) -> &str;
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    async fn execute(&self, message: &Message, action: &Action) -> Result<()>;
}
```

Executor types are non-empty and unique.

### Health Traits

- `Checker`: produces one `CheckResult`
- `ServiceHealth`: running state, metrics, and custom checks

All trait objects are `Send + Sync`.

## Rule Matching

Supported fields:

- built-in message scalars
- nested metadata through `metadata.<path>`

Supported operators:

- equality
- substring containment
- Rust regex

All conditions in a rule must match. Enabled matched rules are stably sorted by ascending priority.

Regex values are compiled during validation and safely rechecked during matching.

## Composition

One exact workflow policy defines provider selection.

Modes:

- `single`: exactly one provider
- `or`: any provider may contribute
- `and`: every provider must contribute
- `pipeline`: every provider contributes in explicit provider order

After selection:

1. sort rules stably by priority
2. collect actions in rule order
3. optionally deduplicate by action ID or executor-plus-params
4. sort actions stably by priority

No implicit workflow-policy fallback is allowed.

## Pipeline Lifecycle

Construction:

- validate engine configuration
- reject duplicate/empty provider names
- verify configured provider names exist
- reject duplicate/empty executor types

Startup:

1. acquire exclusive process gate
2. reject pending cleanup
3. start providers in sorted name order
4. validate every snapshot
5. validate enabled executor references
6. start executors in sorted type order
7. mark started

Failure:

- stop active executors in reverse order
- stop active providers in reverse order
- retain components whose cleanup failed
- preserve both startup and cleanup diagnostics

Processing:

- acquire shared process gate
- require started state
- build one plan
- attempt every action in plan order
- collect every failure
- return typed partial failure from strict API

Shutdown:

- acquire exclusive process gate, waiting for in-flight work
- prevent new work
- stop in reverse order
- enforce a global ten-second cleanup deadline
- retain failed components for retry

Repeated successful start/stop cycles are supported.

## Errors

`AppError` carries:

- stable `ErrorType`
- human-readable message
- optional source error
- structured JSON fields

Categories include invalid input, not found, internal, unavailable, timeout, cancelled, partial failure, unauthorized, forbidden, and conflict.

Action failure uses a typed `ExecutionError` source containing the plan and action failures.

Errors should cross module boundaries; internal code must not log-and-discard failures that affect behavior.

## Configuration

### Runtime YAML

Controls:

- admin listener and HTTP timeouts
- inputs
- optional adapter services

### Engine YAML

Controls:

- workflow names
- provider sets
- composition mode
- pipeline order
- action merge

### Rule YAML

Controls:

- local provider version
- rules, conditions, and actions

Every YAML surface:

- accepts exactly one document
- rejects unknown fields
- preserves valid explicit empty collections
- distinguishes omitted configuration collections from explicit empty collections
- validates normalized values before successful startup

Environment variables are an explicit operational override layer. They do not form an undocumented generic mapping.

Secrets should be environment- or secret-manager-owned. Demo credentials are local fake values.

## Reference Runtime

`App` serializes lifecycle with one mutex.

Startup order:

1. configuration
2. service registration
3. services
4. service monitor
5. workflow engine
6. input listener wiring
7. admin server

The admin server binds before startup returns success.

Shutdown order:

1. mark health shutting down
2. stop service monitor
3. stop admin server
4. stop workflow engine/pipeline
5. stop services
6. release owned wiring

Shutdown aggregates failures.
Failed workflow or service cleanup remains owned so a later shutdown call can retry it.
Monitor, workflow, and service shutdown waits are bounded; a monitor task that exceeds its
deadline is aborted and joined.

## Dependency Wiring

`App` constructs enabled services directly and passes their `Arc` handles into `ServiceManager` and `WorkflowEngine` (`AdapterServices`). Framework consumers and reference adapters both use direct constructor injection.

## Service Management

`ServiceManager`:

- rejects empty and duplicate registration
- registers health views
- starts services concurrently
- tracks services started by the current call
- rolls back started services on any start failure
- bounds rollback operations
- stops all services concurrently
- aggregates deterministic sorted failures
- monitors/restarts down services on a delayed interval

## Admin HTTP

Routes:

- `/health`
- `/health/readiness`
- `/health/liveness`
- `/health/service`
- `/services`
- `/workflows`
- `/rule-engine`

Admin server requirements:

- fail-fast bind
- configured header/read/write/idle timeouts
- graceful connection shutdown
- bounded server join
- restart after successful stop

Liveness must not fail because of dependencies. Readiness must fail for critical down/unknown state and shutdown.

## Health

Health combines:

- built-in memory and Tokio runtime checks
- registered checkers
- service running state
- service custom checks

Checks run concurrently with panic containment and a batch timeout. Public parallel results preserve input order; the final system report sorts checks by name.

Overall status:

- critical down -> down
- warning/info down -> degraded
- any degraded -> degraded
- unknown without down/degraded -> unknown
- otherwise up

Reports are cached for the configured refresh interval and cloned before return.

## Logging

Logging uses `tracing` and `tracing-subscriber`.

Requirements:

- JSON default
- text mode for local use
- level/filter configuration
- caller fields
- optional ANSI
- stdout, stderr, or rotating file destination
- close-before-rename cross-platform rotation

The global subscriber initializes at most once.

## Demo Adapters

### WebSocket

Input with reconnect, decoded message callback, health, metrics, and bounded shutdown.

### Confluence-Like

Periodic HTTP settings loader plus last-good dynamic provider.

### Mattermost-Like

Token or fail-fast login authentication, HTTP posts, optional WebSocket connectivity, and
executor.

### BadgeDB

In-memory service-backed action example. Not durable production storage.

## Fake Server

The fake server must:

- bind all listeners before reporting successful start
- roll back partial startup
- expose health endpoints
- support custom and ephemeral ports
- retain observable events/posts
- broadcast generic and Mattermost-like WebSocket events
- stop and join all server tasks

It is used as a library by integration tests and as a standalone binary locally.

## Security Boundaries

- local admin profiles bind loopback
- container admin is published on host loopback
- the reference admin API is unauthenticated
- HTTP executor URLs are trusted configuration
- production systems must constrain SSRF, secrets, payload size, retries, and idempotency
- logs must not contain credentials
- unsafe Rust is forbidden workspace-wide

## Developer Experience

Required commands:

```bash
make dev
make dev-smoke
make compose-verify
docker compose up --build
make verify
make coverage
```

The VS Code configuration uses CodeLLDB and rust-analyzer.

Examples must compile under `cargo build --workspace --all-targets`.

## Testing Strategy

Required evidence:

- model and validation unit tests
- all composition modes
- YAML strictness
- template rendering tests
- HTTP executor method/header/body/status/timeout tests
- pipeline lifecycle, rollback, in-flight, and partial failure tests
- health status, timeout, panic, endpoint, and cache tests
- service manager start/rollback/stop/monitor tests
- adapter HTTP/WebSocket integration tests
- full demo event E2E
- app admin and restart tests
- local-process and container smoke tests
- Clippy with warnings denied
- rustdoc warnings denied
- at least 90% workspace line coverage

Network tests bind loopback ephemeral ports.

## Acceptance Criteria

- no Go source/module/build entry points remain
- `cargo fmt --all -- --check` passes
- `cargo clippy --workspace --all-targets -- -D warnings` passes
- `cargo test --workspace` passes
- `cargo build --workspace --all-targets` passes
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` passes
- coverage is at least 90%
- all five examples compile; focused examples run
- `make dev` starts fake server and reference runtime
- an AAA event reaches fake Mattermost
- `make dev-smoke` verifies the complete local process path
- all documented admin endpoints respond as specified
- `docker compose config` and image builds succeed
- `make compose-verify` verifies the complete container path
- README and guides contain only current Rust commands and paths
- demo adapters are not imported by `service-platform`
- final design-to-code audit finds no untracked discrepancy
