//! Command-line entrypoint orchestration for the local fake APIs.

use std::future::Future;

use anyhow::{Context, Result, bail};
use tracing_subscriber::EnvFilter;

use crate::{FakeApiManager, Ports, healthcheck};

/// Runs the fake API command with process arguments.
pub async fn run(arguments: impl IntoIterator<Item = String>) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    run_with_shutdown(arguments, shutdown_signal()).await
}

async fn run_with_shutdown(
    arguments: impl IntoIterator<Item = String>,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let (ports, health_only) = parse_args(arguments)?;
    if health_only {
        return healthcheck(ports.websocket).await;
    }

    let manager = FakeApiManager::new(ports);
    let bound = manager.start().await.context("start fake API servers")?;
    tracing::info!(
        confluence = bound.confluence,
        mattermost_http = bound.mattermost_http,
        mattermost_ws = bound.mattermost_ws,
        websocket = bound.websocket,
        "All fake API servers started"
    );
    shutdown.await;
    manager.stop().await.context("stop fake API servers")
}

fn parse_args(arguments: impl IntoIterator<Item = String>) -> Result<(Ports, bool)> {
    let mut ports = Ports::default();
    let mut healthcheck = false;
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        let target = match argument.as_str() {
            "-confluence-port" | "--confluence-port" => Some(&mut ports.confluence),
            "-mm-http-port" | "--mm-http-port" => Some(&mut ports.mattermost_http),
            "-mm-ws-port" | "--mm-ws-port" => Some(&mut ports.mattermost_ws),
            "-ws-port" | "--ws-port" => Some(&mut ports.websocket),
            "-healthcheck" | "--healthcheck" => {
                healthcheck = true;
                None
            }
            _ => bail!("unknown argument: {argument}"),
        };
        if let Some(target) = target {
            let value = arguments
                .next()
                .with_context(|| format!("missing value for {argument}"))?;
            *target = value
                .parse()
                .with_context(|| format!("invalid port for {argument}"))?;
        }
    }
    Ok((ports, healthcheck))
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                let _ = result;
            }
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use std::future;

    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(ToString::to_string).collect()
    }

    fn ephemeral_arguments() -> Vec<String> {
        strings(&[
            "--confluence-port",
            "0",
            "--mm-http-port",
            "0",
            "--mm-ws-port",
            "0",
            "--ws-port",
            "0",
        ])
    }

    #[test]
    fn parser_supports_defaults_aliases_and_healthcheck() {
        let (defaults, health) = parse_args(Vec::new()).unwrap();
        assert_eq!(defaults.confluence, crate::DEFAULT_CONFLUENCE_PORT);
        assert_eq!(
            defaults.mattermost_http,
            crate::DEFAULT_MATTERMOST_HTTP_PORT
        );
        assert_eq!(defaults.mattermost_ws, crate::DEFAULT_MATTERMOST_WS_PORT);
        assert_eq!(defaults.websocket, crate::DEFAULT_WEBSOCKET_PORT);
        assert!(!health);

        let (ports, health) = parse_args(strings(&[
            "-confluence-port",
            "1",
            "--mm-http-port",
            "2",
            "-mm-ws-port",
            "3",
            "--ws-port",
            "4",
            "-healthcheck",
        ]))
        .unwrap();
        assert_eq!(ports.confluence, 1);
        assert_eq!(ports.mattermost_http, 2);
        assert_eq!(ports.mattermost_ws, 3);
        assert_eq!(ports.websocket, 4);
        assert!(health);
        assert!(parse_args(strings(&["--healthcheck"])).unwrap().1);
    }

    #[test]
    fn parser_reports_unknown_missing_and_invalid_values() {
        assert!(
            parse_args(strings(&["--unknown"]))
                .unwrap_err()
                .to_string()
                .contains("unknown argument")
        );
        assert!(
            parse_args(strings(&["--ws-port"]))
                .unwrap_err()
                .to_string()
                .contains("missing value")
        );
        assert!(
            parse_args(strings(&["--ws-port", "invalid"]))
                .unwrap_err()
                .to_string()
                .contains("invalid port")
        );
    }

    #[tokio::test]
    async fn runner_starts_and_stops_ephemeral_servers() {
        run_with_shutdown(ephemeral_arguments(), async {})
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn runner_executes_healthcheck_mode() {
        let manager = FakeApiManager::new(Ports {
            confluence: 0,
            mattermost_http: 0,
            mattermost_ws: 0,
            websocket: 0,
        });
        let ports = manager.start().await.unwrap();
        run_with_shutdown(
            strings(&["--ws-port", &ports.websocket.to_string(), "--healthcheck"]),
            future::pending(),
        )
        .await
        .unwrap();
        manager.stop().await.unwrap();
    }
}
