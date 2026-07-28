# Bounded Concurrency

`service_platform::concurrency` provides a stateless worker pool for independent task batches. It limits active work, preserves successful result order, reports task failures in input order, and supports cooperative cancellation.

## Basic Usage

```rust
use service_platform::concurrency::WorkerPool;

let tasks = (0..20)
    .map(|index| {
        move || async move {
            perform_work(index).await?;
            Ok(index)
        }
    })
    .collect();

let results = WorkerPool::new(4)?.run(tasks).await?;
```

`WorkerPool::new(0)` returns `InvalidInput`.

## Cancellation

```rust
use service_platform::concurrency::{CancellationToken, WorkerPool};

let token = CancellationToken::new();
let worker_token = token.clone();

let task = tokio::spawn(async move {
    WorkerPool::new(4)?
        .run_cancellable(&worker_token, tasks)
        .await
});

token.cancel();
let result = task.await?;
```

Cancellation contract:

- a pre-cancelled batch starts no tasks and returns `ErrorType::Cancelled`
- cancellation stops pending tasks from starting
- already-active tasks are allowed to finish
- cancellation after work started returns `PartialFailure`
- cancelling a token is idempotent
- clones observe the same state

Task closures do not receive the token automatically. Pass a clone into a task when the operation itself should stop cooperatively.

## Ordering and Errors

- every started task runs at most once
- active task count never exceeds the configured worker count
- successful values are returned in input order, not completion order
- task error messages are ordered by input index
- the pool waits for all active tasks before returning
- any task failure returns `ErrorType::PartialFailure`
- a task panic or join failure is reported as an infrastructure failure

The current API returns either all successful values or one aggregate error. It does not return a partial vector of values.

## Appropriate Uses

Good fits:

- bounded independent API calls
- batch validation
- parallel health probes
- file or record transformations
- fan-out work where all active operations must be joined

Poor fits:

- an unbounded stream
- tasks that depend on previous task output
- work that needs per-key ordering
- operations requiring transactional all-or-nothing behavior
- fire-and-forget background tasks

Use a channel-based long-lived worker service for streams. Use the rule pipeline for provider/action orchestration.

## Testing Concurrent Code

Prefer observable synchronization over sleeps:

- `tokio::sync::Notify` to block/release tasks
- atomics to record active and maximum counts
- paused Tokio time for timer-driven logic
- bounded `tokio::time::timeout` around tests that could hang

Run:

```bash
cargo test -p service-platform concurrency::tests
```

Rust stable has no direct race-detector command. On supported nightly Linux environments, `make test-tsan` runs ThreadSanitizer. The normal CI gate still relies on deterministic concurrency tests, Clippy, and integration tests.
