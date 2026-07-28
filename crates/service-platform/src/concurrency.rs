//! Bounded concurrent execution helpers with cooperative cancellation.

use std::{collections::VecDeque, future::Future};

use tokio::{sync::watch, task::JoinSet};

use crate::errors::{AppError, ErrorType, Result};

/// Cloneable cooperative cancellation signal for one or more task batches.
#[derive(Debug, Clone)]
pub struct CancellationToken {
    state: watch::Sender<bool>,
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationToken {
    pub fn new() -> Self {
        let (state, _) = watch::channel(false);
        Self { state }
    }

    pub fn cancel(&self) {
        self.state.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.state.borrow()
    }

    pub async fn cancelled(&self) {
        let mut state = self.state.subscribe();
        if *state.borrow_and_update() {
            return;
        }
        while state.changed().await.is_ok() {
            if *state.borrow_and_update() {
                return;
            }
        }
    }
}

/// Stateless concurrency limiter reusable across independent batches.
#[derive(Debug, Clone)]
pub struct WorkerPool {
    workers: usize,
}

impl WorkerPool {
    pub fn new(workers: usize) -> Result<Self> {
        if workers == 0 {
            return Err(AppError::new(
                ErrorType::InvalidInput,
                "worker count must be greater than zero",
            ));
        }
        Ok(Self { workers })
    }

    /// Runs every task at most once and returns results in input order.
    pub async fn run<F, Fut, T>(&self, tasks: Vec<F>) -> Result<Vec<T>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        self.run_cancellable(&CancellationToken::new(), tasks).await
    }

    /// Runs tasks in input order while cancellation prevents pending tasks from starting.
    pub async fn run_cancellable<F, Fut, T>(
        &self,
        cancellation: &CancellationToken,
        tasks: Vec<F>,
    ) -> Result<Vec<T>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        if cancellation.is_cancelled() {
            return Err(AppError::new(
                ErrorType::Cancelled,
                "worker pool was cancelled",
            ));
        }

        let count = tasks.len();
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut pending: VecDeque<_> = tasks.into_iter().enumerate().collect();
        let mut set = JoinSet::new();
        while set.len() < self.workers {
            let Some((index, task)) = pending.pop_front() else {
                break;
            };
            set.spawn(async move { (index, task().await) });
        }

        let mut results: Vec<Option<T>> = (0..count).map(|_| None).collect();
        let mut failures: Vec<Option<String>> = (0..count).map(|_| None).collect();
        let mut infrastructure_failures = Vec::new();
        let mut cancelled = false;
        while !set.is_empty() {
            let joined = if cancelled {
                set.join_next().await
            } else {
                tokio::select! {
                    () = cancellation.cancelled() => {
                        cancelled = true;
                        pending.clear();
                        continue;
                    }
                    joined = set.join_next() => joined,
                }
            };
            let Some(joined) = joined else {
                break;
            };
            match joined {
                Ok((index, Ok(value))) => results[index] = Some(value),
                Ok((index, Err(error))) => {
                    failures[index] = Some(format!("task {index}: {error}"));
                }
                Err(error) => {
                    infrastructure_failures.push(format!("worker task failed: {error}"));
                }
            }

            if cancellation.is_cancelled() {
                cancelled = true;
                pending.clear();
            }
            while !cancelled && set.len() < self.workers {
                let Some((index, task)) = pending.pop_front() else {
                    break;
                };
                set.spawn(async move { (index, task().await) });
            }
        }

        let mut messages: Vec<_> = failures.into_iter().flatten().collect();
        messages.extend(infrastructure_failures);
        if cancelled {
            messages.push("worker pool was cancelled".to_owned());
        }
        if !messages.is_empty() {
            return Err(AppError::new(
                if cancelled && results.iter().all(Option::is_none) && messages.len() == 1 {
                    ErrorType::Cancelled
                } else {
                    ErrorType::PartialFailure
                },
                messages.join("; "),
            ));
        }
        Ok(results.into_iter().flatten().collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::{
        sync::Notify,
        time::{Duration, sleep},
    };

    use super::*;

    #[tokio::test]
    async fn pool_limits_concurrency_and_preserves_result_order() {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let tasks = (0..8)
            .map(|index| {
                let active = active.clone();
                let maximum = maximum.clone();
                move || async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(current, Ordering::SeqCst);
                    sleep(Duration::from_millis(5)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(index)
                }
            })
            .collect();
        let values = WorkerPool::new(2).unwrap().run(tasks).await.unwrap();
        assert_eq!(values, (0..8).collect::<Vec<_>>());
        assert!(maximum.load(Ordering::SeqCst) <= 2);
    }

    #[tokio::test]
    async fn pool_reports_failures_in_input_order() {
        let tasks = (0..4)
            .map(|index| {
                move || async move {
                    sleep(Duration::from_millis(u64::try_from(4 - index).unwrap())).await;
                    if index % 2 == 0 {
                        Err(AppError::new(
                            ErrorType::Internal,
                            format!("failure-{index}"),
                        ))
                    } else {
                        Ok(index)
                    }
                }
            })
            .collect();
        let error = WorkerPool::new(4).unwrap().run(tasks).await.unwrap_err();
        let message = error.to_string();
        assert!(message.find("task 0").unwrap() < message.find("task 2").unwrap());
    }

    #[tokio::test]
    async fn cancellation_stops_pending_tasks_without_interrupting_active_work() {
        let token = CancellationToken::new();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let tasks = (0..5)
            .map(|_| {
                let started = started.clone();
                let release = release.clone();
                let calls = calls.clone();
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    release.notified().await;
                    Ok(())
                }
            })
            .collect();
        let pool = WorkerPool::new(1).unwrap();
        let run_token = token.clone();
        let run = tokio::spawn(async move { pool.run_cancellable(&run_token, tasks).await });

        started.notified().await;
        token.cancel();
        release.notify_one();

        let error = run.await.unwrap().unwrap_err();
        assert_eq!(error.error_type, ErrorType::PartialFailure);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pre_cancelled_batch_does_not_start_tasks() {
        let token = CancellationToken::new();
        token.cancel();
        let calls = Arc::new(AtomicUsize::new(0));
        let task_calls = calls.clone();
        let error = WorkerPool::new(1)
            .unwrap()
            .run_cancellable(
                &token,
                vec![move || async move {
                    task_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }],
            )
            .await
            .unwrap_err();
        assert_eq!(error.error_type, ErrorType::Cancelled);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn token_defaults_and_empty_batches_complete_immediately() {
        let token = CancellationToken::default();
        assert!(!token.is_cancelled());
        token.cancel();
        token.cancelled().await;

        assert_eq!(
            WorkerPool::new(0).unwrap_err().error_type,
            ErrorType::InvalidInput
        );
        let tasks: Vec<fn() -> std::future::Ready<Result<usize>>> = Vec::new();
        assert!(
            WorkerPool::new(2)
                .unwrap()
                .run(tasks)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn panicking_worker_is_reported_as_infrastructure_failure() {
        let tasks = vec![|| async {
            panic!("worker panic");
            #[allow(unreachable_code)]
            Ok::<(), AppError>(())
        }];
        let error = WorkerPool::new(1).unwrap().run(tasks).await.unwrap_err();
        assert_eq!(error.error_type, ErrorType::PartialFailure);
        assert!(error.to_string().contains("worker task failed"));
    }
}
