# Development Guidebook

This guide is for developers who want to run, debug, read, test, and extend Rust Service Platform locally. It explains both the reusable SDK and the reference runtime, then develops six realistic scenarios with the same repeatable workflow.

## What This Project Is

The project has three boundaries:

| Boundary | Location | Responsibility |
| --- | --- | --- |
| Framework SDK | `crates/service-platform` | Rules, composition, pipeline, executors, health, config, logging |
| Reference runtime | `crates/service-runtime` | Lifecycle, runtime config, services, adapters, workflow engine, admin API |
| Local dependencies | `crates/fake-server` | Confluence-like, Mattermost-like, and WebSocket APIs |

The core framework does not know where a message came from or what a selected action means. Applications connect transports, providers, and executors around the SDK.

## Mental Model

Five objects explain most source code:

1. `Message`: normalized input event.
2. `RuleProvider`: publishes a complete `RuleSet` snapshot.
3. `WorkflowPolicy`: chooses providers and composition behavior.
4. `ExecutionPlan`: matched rules plus merged actions.
5. `Executor`: performs one action.

```text
transport
  -> Message
  -> WorkflowEngine
  -> pipeline::Engine
  -> Composer
  -> ExecutionPlan
  -> Executor implementations
```

Lifecycle is a separate axis:

```text
main
  -> cli
  -> RuntimeConfig
  -> App
  -> ServiceManager
  -> WorkflowEngine
  -> AdminServer
```

## Startup Flow

The binary bootstrap is `crates/service-runtime/src/main.rs`. Testable command-line and shutdown orchestration lives in `crates/service-runtime/src/cli.rs`.

1. Load `LoggerConfig` from environment.
2. Load strict `RuntimeConfig`.
3. Construct `App`.
4. Validate runtime configuration.
5. Register enabled services in `ServiceManager`.
6. Start services concurrently with rollback.
7. Start the service monitor.
8. Load and filter `EngineConfig`.
9. Create providers and executors.
10. Start the pipeline and run provider/executor preflight.
11. Connect the WebSocket handler to `WorkflowEngine`.
12. Bind the admin listener.
13. Return successful startup only after the listener is ready.

Any critical startup error triggers cleanup. Cleanup errors are retained in the returned diagnostic.

## Message Flow

With the demo profile:

```text
POST fake-server :8093/api/send
-> fake-server broadcast
-> WebSocketService websocket_loop
-> MessageHandler
-> WorkflowEngine::process_message
-> Engine::process_outcome
-> Composer::build_execution_plan
-> Executor::execute
-> fake Mattermost / BadgeDB / log / HTTP
```

The admin API is observational; it is not in the event path.

## Prerequisites

- Rust 1.86 or later
- Cargo
- `curl`
- Git
- optional: Docker and Docker Compose v2
- optional: VS Code, rust-analyzer, and CodeLLDB
- optional: `cargo-llvm-cov`

Verify:

```bash
rustc --version
cargo --version
docker compose version
```

Install the optional coverage tool:

```bash
cargo +stable install cargo-llvm-cov --version 0.8.7 --locked
```

## First Build

```bash
cargo check --workspace --all-targets
cargo test --workspace
cargo build --workspace --all-targets
```

`--all-targets` matters because it compiles binaries, libraries, tests, and examples.

Run the repository gate:

```bash
make verify
```

## Local Run Modes

### Smallest In-Process Example

```bash
cargo run -p service-platform --example basic-rule-pipeline
```

Use this while learning rule and pipeline code. No ports or dependencies are involved.

### HTTP Gateway

```bash
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

Use this while editing rules because each request returns the plan.

### Split-Terminal Runtime

Terminal 1:

```bash
cargo run -p fake-server --bin fake-server
```

Terminal 2:

```bash
RUNTIME_CONFIG=config/profiles/demo/runtime.yaml \
RULE_ENGINE_CONFIG=config/profiles/demo/rule-engine.yaml \
LOG_FORMAT=text \
cargo run -p service-runtime --bin service-workflow
```

Terminal 3:

```bash
curl -s http://127.0.0.1:18080/health/readiness
curl -s -X POST http://127.0.0.1:8093/api/send \
  -H 'Content-Type: application/json' \
  -d '{"id":"dev-1","type":"AAA","content":"hello","user_id":"developer"}'
```

Use split terminals when one process must run under a debugger.

### One-Terminal Development

```bash
make dev
```

The Make target:

- builds the fake server
- starts it as a managed child
- waits for readiness
- prints verification commands
- starts the runtime with the demo profile
- forwards shutdown and reaps the child

Run the same local path non-interactively, including an AAA event assertion:

```bash
make dev-smoke
```

### Docker Compose

```bash
docker compose up --build
```

Verify:

```bash
curl -s http://127.0.0.1:18080/health/readiness
curl -s -X POST http://127.0.0.1:8093/api/send \
  -H 'Content-Type: application/json' \
  -d '{"id":"docker-1","type":"AAA","content":"container path","user_id":"developer"}'
```

Stop:

```bash
docker compose down
```

Build, start, exercise, and clean up the environment automatically:

```bash
make compose-verify
```

Compose binds host ports to loopback. Inside the network, the runtime uses service DNS names and binds admin to `0.0.0.0:8080`.

## Logging for Development

Readable logs:

```bash
LOG_FORMAT=text LOG_LEVEL=debug make run
```

Module filter:

```bash
LOG_FORMAT=text \
LOG_LEVEL='service_runtime=debug,service_platform=trace' \
make run
```

Backtraces:

```bash
RUST_BACKTRACE=1 make test
RUST_BACKTRACE=full make run
```

JSON is the default because it is safer for machine ingestion.

## rust-analyzer Project Checks

The checked-in VS Code settings run Clippy for every target. Equivalent terminal checks:

```bash
cargo metadata --format-version 1 --no-deps
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

If rust-analyzer shows stale errors:

1. confirm VS Code opened the repository root containing `Cargo.toml`
2. run `Rust Analyzer: Reload Workspace`
3. run `cargo check --workspace --all-targets`
4. inspect the first compiler error, not downstream cascades
5. delete only editor state if necessary; do not delete `Cargo.lock`

`cargo check` is the authoritative whole-project syntax/type check. rust-analyzer provides the same compiler-backed diagnostics incrementally.

## VS Code Debugging

Install the recommended extensions from `.vscode/extensions.json`:

- rust-analyzer
- CodeLLDB
- Even Better TOML

Checked-in launch configurations:

- `Debug service-workflow (YAML-first)`
- `Debug service-workflow (demo profile)`
- `Debug fake-server`
- `Debug HTTP event gateway`

For the demo runtime:

1. start `Debug fake-server`
2. start `Debug service-workflow (demo profile)`
3. add breakpoints
4. send an event with `curl`

Useful breakpoints:

| Question | Function |
| --- | --- |
| How is config loaded? | `service_runtime::main` |
| How is startup serialized? | `App::start` |
| How does reconnect work? | `websocket_loop` |
| Where are messages processed? | `WorkflowEngine::process_message` |
| How is the plan built? | `Composer::build_execution_plan` |
| How are actions executed? | `Engine::process_outcome` |
| How does HTTP execute? | `HttpExecutor::execute` |
| Where does admin bind? | `AdminServer::start` |

Async debugging tips:

- break after an `.await` if a local value is not yet initialized
- use the Tokio task view when available
- inspect `Arc` pointees, not only reference counts
- add a narrow tracing field when stepping changes timing
- use deterministic `Notify` barriers in tests instead of long sleeps

## Command-Line Debugging

Build with debug symbols:

```bash
cargo build -p service-runtime --bin service-workflow
```

Launch with the toolchain LLDB wrapper:

```bash
rust-lldb target/debug/service-workflow
```

Inside LLDB:

```text
breakpoint set --name service_runtime::app::App::start
run
bt
frame variable
```

For most async work, CodeLLDB plus structured logs is more productive than raw LLDB.

## Source Learning Path

### Stage 1: Models and Validation

Read:

1. `crates/service-platform/src/errors.rs`
2. `crates/service-platform/src/config.rs`
3. `crates/service-runtime/src/config.rs`
4. first half of `ruleengine.rs`

Exercise: introduce an unknown YAML field and observe the exact startup error.

### Stage 2: Matching and Composition

Read the second half of `ruleengine.rs`.

Exercise: run `incident-routing.yaml`, then step through:

1. `message_field`
2. `match_condition`
3. `match_rules`
4. `select_rules`
5. `merge_actions`

### Stage 3: Pipeline Lifecycle

Read `pipeline.rs`.

Observe:

- provider order
- preflight
- executor order
- reverse rollback
- in-flight read/write gate
- typed partial failures
- cleanup deadline

Run:

```bash
cargo test -p service-platform pipeline::tests -- --nocapture
```

### Stage 4: Runtime Services

Read:

1. `service.rs`
2. `manager.rs`
3. `workflow.rs`
4. `adapters.rs`

Exercise: disable one adapter in a temporary runtime profile and compare `/services` and `/rule-engine`.

### Stage 5: Admin and Health

Read:

1. `health.rs`
2. `runtime.rs`
3. `app.rs`

Exercise: stop `fake-server` while the runtime is active and compare liveness with readiness.

### Stage 6: Tests as Architecture

Read:

- `crates/service-runtime/tests/demo_e2e.rs`
- `crates/service-runtime/tests/service_adapters.rs`
- unit tests next to each SDK module

The tests demonstrate ownership and failure contracts more precisely than a happy-path diagram.

# Scenario Guidebooks

Every scenario uses the same structure:

1. define the event contract
2. express policy as rules
3. run a focused input
4. inspect the plan or side effect
5. debug the source path
6. test success and failure
7. identify production extensions

## Scenario 1: Incident Alert Router

### Goal

Route high-severity incidents and service-specific incidents without hard-coding routing logic in the input transport.

### Event Contract

```json
{
  "id": "inc-202",
  "type": "INCIDENT",
  "content": "checkout latency above SLO",
  "user_id": "monitor",
  "metadata": {
    "severity": "critical",
    "service": "checkout"
  }
}
```

Required metadata:

- `severity`: `low`, `medium`, `high`, or `critical`
- `service`: stable service identifier

### Rules

Use `examples/rules/incident-routing.yaml`.

It demonstrates:

- regex severity matching
- exact service matching
- multiple matching rules
- deterministic action priority
- top-level metadata templates

### Run

Terminal 1:

```bash
WORKFLOW_RULES=examples/rules/incident-routing.yaml \
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

Terminal 2:

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"inc-202","type":"INCIDENT","content":"checkout latency above SLO","user_id":"monitor","metadata":{"severity":"critical","service":"checkout"}}'
```

Expected:

- two matched rule IDs
- two planned action IDs
- warning log for severity
- informational log for checkout ownership

### Debug Path

Set breakpoints:

1. `Composer::build_execution_plan`
2. `match_condition`
3. `merge_actions`
4. `LogExecutor::execute`

Inspect the `matched` map before `select_rules`.

### Test Cases

- critical checkout: both rules
- high non-checkout: severity rule only
- low checkout: checkout rule only
- missing severity: checkout rule may still match
- invalid nested metadata type: no severity match

### Extensions

- add an on-call HTTP executor
- add deduplication/idempotency storage
- route by region and tenant
- suppress repeated alerts in an executor
- add escalation based on elapsed incident age

### Common Failures

- `severity` is nested under a different path
- rule uses uppercase value while event contract uses lowercase
- duplicate action IDs remove a desired action
- webhook endpoint times out and produces a partial failure

## Scenario 2: Release Gatekeeper

### Goal

Turn CI/CD release checks into explicit auditable policy paths.

### Event Contract

```json
{
  "id": "release-42",
  "type": "RELEASE_CHECK",
  "content": "candidate 42",
  "user_id": "ci",
  "metadata": {
    "environment": "production",
    "status": "failed"
  }
}
```

### Rules

Use `examples/rules/release-gate.yaml`.

The first rule selects failed or blocked production checks. The second selects approved production checks.

### Run

```bash
WORKFLOW_RULES=examples/rules/release-gate.yaml \
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

Failed:

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"release-42","type":"RELEASE_CHECK","content":"candidate 42","user_id":"ci","metadata":{"environment":"production","status":"failed"}}'
```

Approved:

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"release-43","type":"RELEASE_CHECK","content":"candidate 43","user_id":"ci","metadata":{"environment":"production","status":"approved"}}'
```

### Expected Observation

The two statuses select different rule/action IDs. A staging event produces an empty successful plan.

### Debug Path

1. inspect the decoded `Message.metadata`
2. stop in `match_condition`
3. compare normalized actual/expected values
4. inspect the final `ExecutionPlan`

### Test Cases

- production failed
- production blocked
- production approved
- production unknown status
- staging failed
- missing environment

### Extensions

- add a signed approval provider
- use `and` so both static and remote policy providers must approve
- post the decision to a deployment API
- persist a typed audit event
- include commit SHA and artifact digest

### Common Failures

- using `and` with a provider that has no applicable rule empties every plan
- HTTP action has no idempotency key
- a rule author assumes empty plan means approval

Production gatekeepers must make the default decision explicit outside the rule engine.

## Scenario 3: Customer Support Ticket Triage

### Goal

Classify urgent and billing-related tickets while allowing multiple queues or actions to contribute.

### Event Contract

```json
{
  "id": "ticket-88",
  "type": "SUPPORT_TICKET",
  "content": "urgent refund request",
  "user_id": "customer-1",
  "metadata": {
    "priority": "p0",
    "language": "en"
  }
}
```

### Rules

Use `examples/rules/support-triage.yaml`.

It combines regex metadata and content substring matching.

### Run

```bash
WORKFLOW_RULES=examples/rules/support-triage.yaml \
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"ticket-88","type":"SUPPORT_TICKET","content":"urgent refund request","user_id":"customer-1","metadata":{"priority":"p0","language":"en"}}'
```

### Expected Observation

Both urgent and billing actions are selected. Priority controls action order, not exclusivity.

### Debug Path

1. `match_rules` to see both rules
2. stable rule priority sort
3. action collection
4. action deduplication

### Test Cases

- p0 refund: both
- p1 non-refund: urgent only
- p3 refund: billing only
- different content capitalization: document the case-sensitive contract
- missing priority: billing may still match

### Extensions

- add language routing
- call a queue API through a typed executor
- enrich with account tier through a dynamic provider
- add a duplicate-ticket suppression executor
- generate a normalized triage event

### Common Failures

- assuming `contains` is case-insensitive
- using the same action ID for distinct queues
- putting PII into logs or URL templates

## Scenario 4: Webhook Automation Hub

### Goal

Accept one raw event, normalize it into a derived event, and process the derived event through a second rule.

### Event Contract

```json
{
  "id": "deploy-7",
  "type": "RAW_DEPLOY",
  "content": "release deployed",
  "user_id": "ci"
}
```

### Rules

Use `examples/rules/webhook-chain.yaml`.

The HTTP action posts `DEPLOY_AUDIT` back to the same gateway.

### Run

```bash
WORKFLOW_RULES=examples/rules/webhook-chain.yaml \
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"deploy-7","type":"RAW_DEPLOY","content":"release deployed","user_id":"ci"}'
```

### Expected Observation

1. first request selects log and HTTP actions
2. HTTP executor posts a second request
3. second request selects the audit rule
4. both requests produce independent plans

### Debug Path

1. `HttpExecutor::execute`
2. body template rendering
3. Axum `handle_event`
4. second `Engine::process`

Use `LOG_LEVEL=trace` to see both paths without pausing the server during its self-call.

### Test Cases

- raw deploy creates exactly one audit
- derived audit does not create another raw deploy
- gateway unavailable produces partial failure
- malformed body template fails before network I/O
- non-2xx response includes status

### Extensions

- add a hop count
- add event lineage and correlation IDs
- sign outbound webhooks
- use an allowlist to mitigate SSRF
- use a queue instead of a synchronous self-call
- add idempotency by source event ID

### Common Failures

- an accidental rule cycle creates unbounded events
- JSON template does not escape untrusted content
- self-call exhausts server capacity under load
- no idempotency causes duplicate side effects

## Scenario 5: Compliance Audit Event Processor

### Goal

Capture an immutable application-owned audit representation through a custom executor.

### Event Contract

```json
{
  "id": "audit-19",
  "type": "AUDIT",
  "content": "role assignment changed",
  "user_id": "administrator",
  "metadata": {
    "resource": "team/payments",
    "decision": "granted"
  }
}
```

### Executor

Start with:

```bash
cargo run -p service-platform --example custom-executor
```

The example's `CollectExecutor`:

- validates `params.template`
- renders a normalized record
- protects state with a mutex
- exposes a cloned inspection view

### Rule

```yaml
- id: record_audit_event
  workflow: WorkflowEngine
  enabled: true
  priority: 10
  conditions:
    - field: type
      op: eq
      value: AUDIT
  actions:
    - id: append_audit_record
      executor: collect
      priority: 10
      params:
        template: "event={{.ID}} actor={{.UserID}} content={{.Content}}"
```

### Debug Path

1. registry preflight finds `collect`
2. `Engine::process_outcome`
3. `CollectExecutor::execute`
4. lock acquisition and entry append

### Test Cases

- valid template stores one entry
- missing template returns `InvalidInput`
- multiple concurrent events do not lose entries
- duplicate action ID executes once when dedup is enabled
- executor failure is present in `ExecutionError`

### Extensions

- replace memory with an append-only database
- write a hash chain
- sign records
- separate actor from authenticated principal
- encrypt sensitive fields
- enforce retention and legal hold

### Common Failures

- treating logging as durable audit storage
- holding a synchronous lock across network `.await`
- failing open when audit persistence is required
- logging secrets in error context

## Scenario 6: Local Learning Sandbox

### Goal

Trace one event end to end through every runtime layer and intentionally break each boundary.

### Start

```bash
make dev
```

### Event

```bash
curl -s -X POST http://127.0.0.1:8093/api/send \
  -H 'Content-Type: application/json' \
  -d '{"id":"learn-001","type":"AAA","content":"trace this event end to end","user_id":"student","metadata":{"lesson":"rule-engine","step":"first-trace"}}'
```

### Inspect

```bash
curl -s http://127.0.0.1:18080/services
curl -s http://127.0.0.1:18080/rule-engine
curl -s http://127.0.0.1:18080/health
curl -s http://127.0.0.1:8093/api/messages
curl -s http://127.0.0.1:8091/api/v4/posts \
  -H 'Authorization: Bearer test-token-123'
```

### Debug Path

Step in this order:

1. fake server `send_event`
2. runtime `websocket_loop`
3. `WorkflowEngine::process_message`
4. `Composer::build_execution_plan`
5. `Engine::process_outcome`
6. `LogExecutor::execute`
7. `MattermostExecutor::execute`
8. `MattermostService::send_message_to_channel`

### Controlled Breakages

1. Set an invalid rule operator and observe startup rejection.
2. Disable Mattermost while retaining Confluence in the policy and observe dependency validation.
3. Stop fake-server and observe reconnect plus readiness.
4. Occupy port 18080 and observe fail-fast admin bind rollback.
5. Change the action executor to an unknown type and observe preflight.
6. Return HTTP 502 from a test server and inspect partial failure.

### Test Cases

- base profile contains only YAML provider
- demo profile contains YAML and Confluence
- service list matches enabled flags
- one AAA event reaches fake Mattermost
- WebSocket reconnects after forced disconnect
- app can stop and start cleanly

### Extensions

- add a new input transport
- add a remote provider
- add an executor backed by a local test API
- add request correlation to a custom HTTP input
- add a production-like profile without demo adapters

# Operational Debugging Use Cases

## Inspect Runtime Health

```bash
curl -i http://127.0.0.1:18080/health/liveness
curl -i http://127.0.0.1:18080/health/readiness
curl -i http://127.0.0.1:18080/health
curl -i 'http://127.0.0.1:18080/health/service?service=WebSocketInputService'
```

Liveness remains 200 during dependency outages. Readiness becomes 503 for critical down/unknown checks or shutdown.

## Debug a Port Conflict

Find the owner on macOS:

```bash
lsof -nP -iTCP:18080 -sTCP:LISTEN
```

Use an override:

```bash
ADMIN_ADDR=127.0.0.1:18081 make run
```

The runtime must fail startup if the admin listener cannot bind.

## Debug WebSocket Reconnects

1. start the demo
2. inspect `/services`
3. stop fake-server
4. observe connection count drop and readiness fail
5. restart fake-server
6. wait for `reconnect_interval`
7. observe connection count recover

Relevant source:

- `websocket_loop`
- `WebSocketService::custom_checks`
- `ServiceManager::monitor`

## Debug Confluence-Backed Rules

```bash
curl -s http://127.0.0.1:8090/rest/api/content/settings-page-1
curl -s http://127.0.0.1:18080/rule-engine
```

Check:

- page body contains valid JSON string
- refresh metrics have a recent timestamp
- provider snapshot has `source: confluence`
- generated actions use `mattermost`
- rule regex compiles

Relevant source:

- `fetch_confluence_settings`
- `ConfluenceSettingsService::refresh`
- `ConfluenceProvider::current_snapshot`

## Debug Mattermost Actions

```bash
curl -s http://127.0.0.1:8091/api/v4/system/ping
curl -s http://127.0.0.1:8091/api/v4/posts \
  -H 'Authorization: Bearer test-token-123'
```

Check:

- token or login credentials are present
- channel is non-empty
- URL does not duplicate `/api/v4/websocket`
- post status is 2xx
- service is running before executor lookup

## Debug HTTP Executor Failures

Inspect:

- final rendered URL
- method
- body template
- header value types
- timeout
- non-2xx response body
- `ExecutionError.failures`

Run focused tests:

```bash
cargo test -p service-platform executor::tests -- --nocapture
```

# Testing Guide

## Test Layers

| Layer | Location | Purpose |
| --- | --- | --- |
| Unit | module `#[cfg(test)]` blocks | Models, validation, lifecycle edge cases |
| Adapter integration | `service-runtime/tests/service_adapters.rs` | Real local HTTP/WebSocket behavior |
| End to end | `service-runtime/tests/demo_e2e.rs` | Fake input through demo action |
| Local smoke | `scripts/check-dev.sh` | Exact `make dev` process path and AAA delivery |
| Container smoke | `scripts/check-compose.sh` | Image build, Compose startup, and AAA delivery |
| Documentation | rustdoc build | Public comments and links compile |
| Static | rustfmt and Clippy | Style and broad correctness checks |

## Commands

```bash
make fmt
make clippy
make test
make doc
make build
make verify
make dev-smoke
make compose-verify
```

Focused:

```bash
cargo test -p service-platform ruleengine::tests
cargo test -p service-platform pipeline::tests
cargo test -p service-runtime manager::tests
cargo test -p service-runtime --test service_adapters
cargo test -p service-runtime --test demo_e2e
```

Show captured output:

```bash
cargo test -p service-runtime --test demo_e2e -- --nocapture
```

## Writing Reliable Async Tests

- bind `127.0.0.1:0`
- read the assigned port from `local_addr`
- use one-shot/watch channels for shutdown
- join every spawned task
- use `Notify` for sequencing
- wrap potential hangs in `timeout`
- avoid shared fixed ports
- verify state after shutdown
- test duplicate start/stop calls

## Coverage

```bash
make coverage
```

Default threshold: 90% workspace line coverage.

The coverage tool may require a newer compiler than the Rust 1.86 project MSRV. Install `cargo-llvm-cov` with the stable toolchain; regular project builds continue to use the documented MSRV or newer.

Override for an experiment:

```bash
COVERAGE_MIN=95 make coverage
```

Coverage is evidence, not the design target. Prioritize failure paths, lifecycle, concurrency, parsing, and network boundaries over trivial getters.

## ThreadSanitizer

Rust stable has no direct `-race` equivalent. On nightly Rust and a supported Linux host:

```bash
make test-tsan
```

Keep it separate from the portable verification gate.

# Troubleshooting

## Admin Connection Refused

Check:

```bash
curl -v http://127.0.0.1:18080/health/liveness
lsof -nP -iTCP:18080 -sTCP:LISTEN
```

Look for a startup configuration, service, pipeline preflight, or bind error. The admin server starts last.

## Readiness Is 503

Read:

```bash
curl -s http://127.0.0.1:18080/health
curl -s http://127.0.0.1:18080/services
```

A critical down service makes the system down. Warning-level failures degrade without failing readiness.

## No Expected Rule

```bash
curl -s http://127.0.0.1:18080/rule-engine
```

Confirm:

- selected config paths
- provider enabled state
- workflow name
- last provider error
- rule enabled state
- condition values

## Configuration Error

The YAML decoders reject unknown fields and extra documents. Read the first path in the error and compare it with [Configuration](configuration.md).

## Tests Cannot Bind a Port

Some restricted execution sandboxes deny loopback listeners. Run network integration tests in a normal local shell or CI. Tests use ephemeral ports, so an ordinary port conflict should be rare.

## Cargo or rust-analyzer Disagrees

Run:

```bash
cargo clean
cargo check --workspace --all-targets
```

Then reload the rust-analyzer workspace. Use `cargo check` output as the authoritative diagnostic.

## Coverage Command Missing

```bash
cargo +stable install cargo-llvm-cov --version 0.8.7 --locked
```

Ensure Cargo's bin directory is on `PATH`.

## Docker Build Fails

```bash
docker compose config
docker compose build --no-cache
```

Confirm Docker can reach the Rust image and Cargo registry. The Dockerfiles build with `Cargo.lock` through `--locked`.

## Where to Go Next

- public architecture: [Architecture and Design](design.md)
- rule details: [Rule Engine](rule-engine.md)
- action details: [Executors](executors.md)
- configuration: [Configuration](configuration.md)
- implementation recipes: [Framework Cookbook](cookbook.md)
- fake API reference: [Fake Server](../crates/fake-server/README.md)
