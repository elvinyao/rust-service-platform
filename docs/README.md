# Documentation Map

Use this page to choose the shortest useful path through the project.

## First Hour

1. Run `cargo run -p service-platform --example basic-rule-pipeline`.
2. Complete [Getting Started](getting-started.md).
3. Start `make dev` and send an event through the full reference runtime.
4. Read [Rule Engine](rule-engine.md) and [Executors](executors.md).
5. Use [Development Guidebook](development.md) while stepping through the source.

## By Role

### Rule Author

- [Getting Started](getting-started.md)
- [Configuration](configuration.md)
- [Rule Engine](rule-engine.md)
- [Framework Cookbook](cookbook.md)
- [Runnable Examples](../examples/README.md)

### Framework Integrator

- [Architecture and Design](design.md)
- [Rule Engine](rule-engine.md)
- [Executors](executors.md)
- [Bounded Concurrency](concurrency.md)
- [Reference Adapters](adapters.md)

### Runtime Maintainer

- [Development Guidebook](development.md)
- [Configuration](configuration.md)
- [Reference Adapters](adapters.md)
- [Architecture and Design](design.md)

## Document Index

| Document | Scope |
| --- | --- |
| [README](../README.md) | Product overview and verified quick start |
| [Getting Started](getting-started.md) | First pipeline, first rule change, first runtime |
| [Development Guidebook](development.md) | Local debugging, source-learning path, six complete scenarios |
| [Configuration](configuration.md) | YAML schema, profiles, environment variables, validation |
| [Rule Engine](rule-engine.md) | Models, matching, providers, composition, plans |
| [Executors](executors.md) | Executor trait, templates, built-ins, partial failures |
| [Bounded Concurrency](concurrency.md) | Worker limits, ordering, cancellation |
| [Reference Adapters](adapters.md) | WebSocket, Confluence-like, Mattermost-like, BadgeDB |
| [Framework Cookbook](cookbook.md) | Task-oriented recipes |
| [Runnable Examples](../examples/README.md) | Commands and payloads for all examples |
| [Architecture and Design](design.md) | Source-of-truth boundaries and invariants |

## Authoritative Sources

Documentation describes the following current implementation:

- public SDK: `crates/service-platform/src`
- reference runtime: `crates/service-runtime/src`
- local fake APIs: `crates/fake-server/src`
- runtime and rule configuration: `config`
- executable examples: `crates/service-platform/examples`

When behavior changes, update the relevant document in the same change.

## Verification

Run the repository gate:

```bash
make verify
```

Run the 90% line-coverage gate after installing `cargo-llvm-cov`:

```bash
cargo +stable install cargo-llvm-cov --version 0.8.7 --locked
make coverage
```

The CI workflow executes formatting, Clippy, tests, all-target builds, rustdoc, coverage, and both local and container smoke paths.
