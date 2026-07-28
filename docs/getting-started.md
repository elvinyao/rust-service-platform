# Getting Started

This tutorial takes you from a local pipeline to the complete reference runtime. It assumes basic Rust knowledge but no previous experience with this project.

## What You Will Learn

- how messages, rules, providers, plans, and executors fit together
- how to change behavior in YAML without recompiling Rust
- how strict configuration fails early
- how to add a custom executor
- how to run and inspect the reference runtime

## Prerequisites

From the repository root:

```bash
rustc --version
cargo --version
cargo check --workspace --all-targets
```

Rust 1.86 or later is required.

## Step 1: Run the Smallest Pipeline

```bash
cargo run -p service-platform --example basic-rule-pipeline
```

The example:

1. creates a `StaticProvider`
2. configures one workflow to use that provider
3. registers the built-in `LogExecutor`
4. starts `pipeline::Engine`
5. processes one `GREETING` message
6. prints the number of matched rules and actions
7. stops the engine

Read `crates/service-platform/examples/basic-rule-pipeline.rs` next. The public types come from:

- `ruleengine::{Message, Rule, Action, StaticProvider}`
- `executor::{Executor, LogExecutor}`
- `pipeline::Engine`

## Step 2: Start an HTTP Event Gateway

Terminal 1:

```bash
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

Terminal 2:

```bash
curl -s http://127.0.0.1:18081/health

curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"start-1","type":"AAA","content":"hello gateway","user_id":"student"}'

curl -s http://127.0.0.1:18081/pipeline
```

The gateway loads `config/workflow-rules.yaml`, rejects unknown JSON fields, limits request bodies to 1 MiB, and returns the matched rule and planned action IDs.

Stop it with `Ctrl+C`.

## Step 3: Change Behavior Without Rust Code

Copy the structure of `config/workflow-rules.yaml` into a temporary file and add:

```yaml
version: "1"
rules:
  - id: learn_priority_event
    workflow: WorkflowEngine
    enabled: true
    priority: 10
    conditions:
      - field: type
        op: eq
        value: LEARNING_EVENT
      - field: metadata.priority
        op: eq
        value: high
    actions:
      - id: log_learning_event
        executor: log
        priority: 10
        params:
          level: warn
          template: 'lesson={{index .Metadata "lesson"}} content={{.Content}}'
```

Start the gateway with the new file:

```bash
WORKFLOW_RULES=/absolute/path/to/learning-rules.yaml \
LOG_FORMAT=text \
cargo run -p service-platform --example http-event-gateway
```

Send a matching event:

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"learn-1","type":"LEARNING_EVENT","content":"rules are data","user_id":"student","metadata":{"priority":"high","lesson":"composition"}}'
```

Change `priority` to `low` and send it again. The response should contain no matched rules or actions.

## Step 4: Run the Scenario Rule Sets

The repository includes four realistic rule sets:

```bash
WORKFLOW_RULES=examples/rules/incident-routing.yaml \
cargo run -p service-platform --example http-event-gateway
```

Send an incident:

```bash
curl -s -X POST http://127.0.0.1:18081/events \
  -H 'Content-Type: application/json' \
  -d '{"id":"inc-1","type":"INCIDENT","content":"checkout latency","user_id":"monitor","metadata":{"severity":"critical","service":"checkout"}}'
```

Both incident rules match, so both actions appear in deterministic priority order.

Use the same gateway command with:

- `examples/rules/release-gate.yaml`
- `examples/rules/support-triage.yaml`
- `examples/rules/webhook-chain.yaml`

The webhook chain posts a derived event back into the gateway. It demonstrates that an executor can create a second event without changing the rule engine.

## Step 5: Add an Executor

Run the complete custom executor example:

```bash
cargo run -p service-platform --example custom-executor
```

The essential extension point is:

```rust
#[async_trait::async_trait]
impl Executor for CollectExecutor {
    fn executor_type(&self) -> &'static str {
        "collect"
    }

    async fn execute(
        &self,
        message: &Message,
        action: &Action,
    ) -> service_platform::Result<()> {
        // Validate parameters and perform one bounded action.
        Ok(())
    }
}
```

Register the same `Arc<CollectExecutor>` with `Engine::new`, then reference `executor: collect` from a rule.

Important contracts:

- executor type names must be non-empty and unique
- all enabled rule actions must resolve during engine startup
- `execute` should return errors instead of swallowing them
- independent actions are all attempted
- `Engine::process` returns a typed partial-failure error when any action fails

## Step 6: Run the Reference Runtime

Start the full environment:

```bash
make dev
```

In another terminal:

```bash
curl -s http://127.0.0.1:18080/health/liveness
curl -s http://127.0.0.1:18080/health/readiness
curl -s http://127.0.0.1:18080/services
curl -s http://127.0.0.1:18080/workflows
curl -s http://127.0.0.1:18080/rule-engine
```

Send an event:

```bash
curl -s -X POST http://127.0.0.1:8093/api/send \
  -H 'Content-Type: application/json' \
  -d '{"id":"runtime-1","type":"AAA","content":"full path","user_id":"student"}'
```

Inspect the fake Mattermost posts:

```bash
curl -s http://127.0.0.1:8091/api/v4/posts \
  -H 'Authorization: Bearer test-token-123'
```

The message path is:

```text
fake WebSocket broadcast
-> WebSocketService
-> WorkflowManager
-> WorkflowEngine
-> pipeline::Engine
-> Composer
-> registered Executors
```

## Core Concepts

### Message

An event with `id`, `type`, `content`, `user_id`, `timestamp`, and arbitrary JSON `metadata`.

### Rule Provider

Publishes an immutable `RuleSet` snapshot. The SDK provides static and strict YAML providers. The demo runtime adds a Confluence-like provider.

### Workflow Policy

Selects providers and one composition mode for a workflow name.

### Execution Plan

The deterministic matched rules and merged actions for one message.

### Executor

Performs one selected action. The SDK includes `log` and `http`.

## Read the Source in This Order

Public SDK:

1. `crates/service-platform/src/ruleengine.rs`
2. `crates/service-platform/src/executor.rs`
3. `crates/service-platform/src/pipeline.rs`
4. `crates/service-platform/src/config.rs`
5. `crates/service-platform/src/health.rs`
6. `crates/service-platform/src/runtime.rs`

Reference runtime:

1. `crates/service-runtime/src/main.rs`
2. `crates/service-runtime/src/cli.rs`
3. `crates/service-runtime/src/app.rs`
4. `crates/service-runtime/src/container.rs`
5. `crates/service-runtime/src/service.rs`
6. `crates/service-runtime/src/manager.rs`
7. `crates/service-runtime/src/workflow.rs`
8. `crates/service-runtime/src/adapters.rs`

## Debugging Checkpoints

Useful CodeLLDB breakpoints:

- `service_runtime::app::App::start`
- `service_runtime::service::websocket_loop`
- `service_runtime::manager::WorkflowManager::dispatch`
- `service_runtime::workflow::WorkflowEngine::process_message`
- `service_platform::ruleengine::Composer::build_execution_plan`
- `service_platform::pipeline::Engine::process_outcome`
- `service_platform::executor::HttpExecutor::execute`

The checked-in `.vscode/launch.json` contains base-profile, demo-profile, fake-server, and HTTP-gateway configurations.

## Next Steps

- use [Development Guidebook](development.md) for source-level exercises
- use [Framework Cookbook](cookbook.md) for implementation recipes
- use [Configuration](configuration.md) before adding a deployment profile
- use [Architecture and Design](design.md) before changing public boundaries
